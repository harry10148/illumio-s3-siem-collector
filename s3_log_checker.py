import boto3
import argparse
import sys
from botocore.exceptions import ClientError, NoCredentialsError

def get_aws_session(aws_profile=None, access_key=None, secret_key=None, region=None):
    """初始化並回傳 AWS Boto3 Session"""
    try:
        if access_key and secret_key:
            print("使用手動提供的 Access Key 與 Secret Key 進行驗證...")
            return boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region
            )
        else:
            profile_msg = aws_profile if aws_profile else "預設 (default)"
            print(f"使用 AWS Profile [{profile_msg}] 進行驗證...")
            return boto3.Session(profile_name=aws_profile, region_name=region)
    except Exception as e:
        print(f"【錯誤】AWS Session 初始化失敗: {e}")
        sys.exit(1)

def test_s3_log_paths(session, bucket_name, fqdn, org_id):
    """測試 S3 Bucket 中的 Illumio 日誌路徑"""
    s3_client = session.client('s3')
    
    paths_to_check = [
        {"prefix": f"{fqdn}/org_id={org_id}/auditable/", "description": "稽核日誌 (Auditable)"},
        {"prefix": f"{fqdn}/org_id={org_id}/summaries/pd=0/", "description": "流量摘要 (pd=0 - Allowed)"},
        {"prefix": f"{fqdn}/org_id={org_id}/summaries/pd=1/", "description": "流量摘要 (pd=1 - Potentially blocked)"},
        {"prefix": f"{fqdn}/org_id={org_id}/summaries/pd=2/", "description": "流量摘要 (pd=2 - Blocked)"},
        {"prefix": f"{fqdn}/org_id={org_id}/summaries/pd=3/", "description": "流量摘要 (pd=3 - Unknown)"}
    ]

    print(f"\n[模式: S3] 開始測試 Bucket: {bucket_name}")
    print("=" * 60)

    for item in paths_to_check:
        prefix = item["prefix"]
        desc = item["description"]
        print(f"檢查項目: {desc}")
        print(f"S3 路徑: s3://{bucket_name}/{prefix}")
        
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=3)
            if 'Contents' in response:
                file_count = len(response['Contents'])
                print(f"狀態: 【成功】已成功存取，並找到物件。 (顯示前 {file_count} 筆)")
                for obj in response['Contents']:
                    print(f"  -> 檔案: {obj['Key']} ({obj['Size'] / 1024:.2f} KB)")
            else:
                print("狀態: 【警告】路徑存取成功，但該目錄下目前沒有任何檔案。")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'AccessDenied':
                print("狀態: 【失敗】存取被拒絕 (Access Denied)。請檢查金鑰權限或 Bucket Policy。")
            elif error_code == 'NoSuchBucket':
                print(f"狀態: 【失敗】找不到 Bucket ({bucket_name})。")
                break
            else:
                print(f"狀態: 【失敗】發生 API 錯誤: {e}")
        print("-" * 60)

def test_sqs_access(session, sqs_url):
    """測試 SQS 佇列是否可存取且是否有訊息 (安全讀取，不刪除)"""
    sqs_client = session.client('sqs')
    
    print(f"\n[模式: SQS] 開始測試佇列: {sqs_url}")
    print("=" * 60)
    
    try:
        # 僅讀取最多 3 筆訊息，等待時間設為 3 秒
        response = sqs_client.receive_message(
            QueueUrl=sqs_url,
            MaxNumberOfMessages=3,
            WaitTimeSeconds=3
        )
        
        messages = response.get('Messages', [])
        if messages:
            print(f"狀態: 【成功】已成功連線，目前拉取到 {len(messages)} 則訊息 (未刪除)。")
            for i, msg in enumerate(messages, 1):
                # 擷取訊息內容前 100 個字元作為預覽
                body_snippet = msg['Body'][:100].replace('\n', ' ')
                print(f"  -> 訊息 {i} ID: {msg['MessageId']}")
                print(f"     內容預覽: {body_snippet}...")
        else:
            print("狀態: 【警告】已成功連線至 SQS，但佇列目前為空 (無待處理的日誌)。")
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['AccessDenied', 'AWS.SimpleQueueService.NonExistentQueue']:
            print(f"狀態: 【失敗】存取被拒絕或佇列不存在: {error_code}。請檢查金鑰權限或 SQS URL 是否正確。")
        else:
            print(f"狀態: 【失敗】發生 API 錯誤: {e}")
    print("-" * 60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AWS 日誌儲存與佇列測試工具 (支援 S3 / SQS 擇一測試)")
    
    # 建立互斥群組，強制使用者只能在 S3 或 SQS 中選擇一種測試目標
    target_group = parser.add_mutually_exclusive_group(required=True)
    target_group.add_argument("--bucket", help="[S3 模式] 目標 S3 Bucket 名稱")
    target_group.add_argument("--sqs-url", help="[SQS 模式] 目標 SQS Queue URL")
    
    # S3 模式專用的參數
    parser.add_argument("--fqdn", help="[S3 模式必填] PCE FQDN (例如: scp3.illum.io)")
    parser.add_argument("--org-id", help="[S3 模式必填] 組織 ID (例如: 123456)")
    
    # AWS 驗證相關參數
    parser.add_argument("--profile", required=False, help="AWS CLI Profile 名稱")
    parser.add_argument("--access-key", required=False, help="AWS Access Key ID")
    parser.add_argument("--secret-key", required=False, help="AWS Secret Access Key")
    parser.add_argument("--region", required=False, help="AWS 區域 (例如: ap-northeast-1，使用 SQS 時建議提供)")
    
    args = parser.parse_args()
    
    # 驗證 Access Key 與 Secret Key 是否成對出現
    if bool(args.access_key) != bool(args.secret_key):
        print("【錯誤】--access-key 與 --secret-key 必須同時提供。")
        sys.exit(1)
        
    session = get_aws_session(args.profile, args.access_key, args.secret_key, args.region)
    
    # 根據使用者提供的目標決定執行哪一種測試
    if args.bucket:
        if not args.fqdn or not args.org_id:
            print("【錯誤】選擇 S3 測試模式時，必須同時提供 --fqdn 與 --org-id。")
            sys.exit(1)
        test_s3_log_paths(session, args.bucket, args.fqdn, args.org_id)
        
    elif args.sqs_url:
        test_sqs_access(session, args.sqs_url)