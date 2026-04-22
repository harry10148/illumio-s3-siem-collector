"""S3 log checker — 連線測試 / 瀏覽 / 下載工具

操作摘要：
  1. 預設不加 `--list` / `--download` 時，會做「連線測試」。
  2. 加 `--list` 時，會列出 S3 物件。
  3. 加 `--download` 時，會下載 S3 物件。
  4. 參數來源優先順序：CLI 參數 > `--config` > boto3 / AWS 預設環境。

必要參數規則：
  - S3 模式必須提供 `--bucket`，或用 `--config` 載入 `source.bucket`。
  - SQS 模式改用 `--sqs-url`，不需要 `--bucket`。
  - 若指定 `--access-key`，則 `--secret-key` 必須一起提供。
  - `--prefix` 的優先順序高於 `--fqdn` + `--org-id` + `--log-type`。

常見操作：
  # 1) 從 config.yaml 載入 bucket / fqdn / org_id / AWS 認證，做預設連線測試
  python s3_log_checker.py --config config.yaml

  # 2) 直接帶參數做預設連線測試
  python s3_log_checker.py --bucket <B> --fqdn <F> --org-id <ID> \\
      --access-key <AK> --secret-key <SK>

  # 3) 列出特定 log type 的檔案
  python s3_log_checker.py --config config.yaml --list --log-type auditable --max-keys 50

  # 4) 列出任意 prefix 下的檔案
  python s3_log_checker.py --bucket <B> --access-key <AK> --secret-key <SK> \\
      --list --prefix "ap-scp45.illum.io/org_id=123456/auditable/"

  # 5) 下載單一檔案
  python s3_log_checker.py --bucket <B> --access-key <AK> --secret-key <SK> \\
      --download --key "ap-scp45.illum.io/org_id=123456/auditable/2026/04/20/file.jsonl.gz" \\
      --out ./downloads/

  # 6) 下載某個 log type 底下的全部檔案
  python s3_log_checker.py --config config.yaml --download --log-type pd2 --out ./downloads/

  # 7) 下載任意 prefix 下的全部檔案
  python s3_log_checker.py --bucket <B> --access-key <AK> --secret-key <SK> \\
      --download --prefix "ap-scp45.illum.io/org_id=123456/auditable/2026/04/20/" \\
      --out ./downloads/

補充說明：
  - `--log-type` 可用值：`auditable` / `pd0` / `pd1` / `pd2` / `pd3`
  - `--download --prefix` 會保留 prefix 以下的相對目錄，避免同名檔案互相覆蓋。
  - `--download --key` 下載單檔時，目的地檔名使用該 key 的 basename。
  - `--list` 只顯示物件，不會下載檔案。
"""
import argparse
import os
import socket
import sys
import urllib.error
import urllib.request
from pathlib import Path, PurePosixPath

import boto3
from botocore.exceptions import ClientError
import yaml

# log_type → S3 路徑片段
_LOG_TYPE_SUBPATH = {
    "auditable": "auditable/",
    "pd0":       "summaries/pd=0/",
    "pd1":       "summaries/pd=1/",
    "pd2":       "summaries/pd=2/",
    "pd3":       "summaries/pd=3/",
}


def get_aws_session(aws_profile=None, access_key=None, secret_key=None, region=None):
    try:
        if access_key and secret_key:
            print("使用手動提供的 Access Key 與 Secret Key 進行驗證...")
            return boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region,
            )
        else:
            profile_msg = aws_profile if aws_profile else "預設 (default)"
            print(f"使用 AWS Profile [{profile_msg}] 進行驗證...")
            return boto3.Session(profile_name=aws_profile, region_name=region)
    except Exception as e:
        print(f"【錯誤】AWS Session 初始化失敗: {e}")
        sys.exit(1)


def _build_prefix(fqdn, org_id, log_type):
    """組合標準 Illumio S3 路徑前綴。"""
    base = f"{fqdn}/org_id={org_id}/"
    if log_type:
        return base + _LOG_TYPE_SUBPATH[log_type]
    return base


def _load_yaml_config(path):
    config_path = Path(path)
    if not config_path.is_file():
        raise ValueError(f"找不到 config 檔案: {config_path}")
    try:
        data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as e:
        raise ValueError(f"config YAML 解析失敗: {e}") from e
    if not isinstance(data, dict):
        raise ValueError("config 根節點必須是 mapping/object")
    return data


def _config_section(config, section_name):
    section = config.get(section_name) or {}
    if not isinstance(section, dict):
        raise ValueError(f"config.{section_name} 必須是 mapping/object")
    return section


def _resolve_aws_auth(args, config):
    aws_cfg = _config_section(config, "aws")
    region = args.region if args.region is not None else aws_cfg.get("region")

    if args.access_key is not None or args.secret_key is not None:
        return args.profile, args.access_key, args.secret_key, region

    if args.profile is not None:
        return args.profile, None, None, region

    return aws_cfg.get("profile"), aws_cfg.get("access_key"), aws_cfg.get("secret_key"), region


def _resolve_s3_source(args, config):
    source_cfg = _config_section(config, "source")
    bucket = args.bucket if args.bucket is not None else source_cfg.get("bucket")
    fqdn = args.fqdn if args.fqdn is not None else source_cfg.get("fqdn")
    org_id = args.org_id if args.org_id is not None else source_cfg.get("org_id")
    return bucket, fqdn, org_id


def _iter_s3_objects(session, bucket, prefix, max_keys=None):
    s3 = session.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    paginate_kwargs = {"Bucket": bucket, "Prefix": prefix}
    if max_keys is not None:
        paginate_kwargs["PaginationConfig"] = {"PageSize": min(max_keys, 1000)}

    yielded = 0
    for page in paginator.paginate(**paginate_kwargs):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("/") and obj["Size"] == 0:
                continue
            yield obj
            yielded += 1
            if max_keys is not None and yielded >= max_keys:
                return


def _download_destination(out_dir, key, prefix=None, preserve_hierarchy=False):
    if preserve_hierarchy and prefix:
        normalized_prefix = prefix.rstrip("/")
        if key.startswith(normalized_prefix + "/"):
            relative = key[len(normalized_prefix) + 1:]
        else:
            relative = key
    else:
        relative = PurePosixPath(key).name

    parts = [part for part in PurePosixPath(relative).parts if part not in ("", ".", "..")]
    if not parts:
        parts = [PurePosixPath(key).name or "downloaded-object"]

    return os.path.join(out_dir, *parts)


# ── Endpoint 探測(純網路,無需 AWS 認證)─────────────────────────────────────

def _probe_bucket_region(bucket, timeout=10):
    """匿名 HEAD 到 <bucket>.s3.amazonaws.com，從 `x-amz-bucket-region` 標頭取得 region。

    回傳 (region, http_status, error_msg)；403/404 屬正常(代表網路可達 AWS)。
    """
    url = f"https://{bucket}.s3.amazonaws.com/"
    req = urllib.request.Request(url, method="HEAD")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.headers.get("x-amz-bucket-region"), resp.status, None
    except urllib.error.HTTPError as e:
        region = e.headers.get("x-amz-bucket-region") if e.headers else None
        return region, e.code, None
    except urllib.error.URLError as e:
        reason = getattr(e, "reason", e)
        return None, None, f"網路錯誤: {reason}(防火牆可能擋 443 或 DNS 無法解析)"
    except socket.timeout:
        return None, None, "連線逾時(防火牆可能擋 443)"
    except Exception as e:
        return None, None, f"探測失敗: {e}"


def _fqdn_candidates(bucket, region):
    """列出 boto3 所有可能用到的 S3 endpoint 變體。

    回傳 [(fqdn, 說明, is_primary)]，is_primary=True 代表 boto3 預設會走的。
    """
    return [
        (f"{bucket}.s3.{region}.amazonaws.com",            "Virtual-hosted 區域(主要)", True),
        (f"s3.{region}.amazonaws.com",                     "Path-style 區域",            True),
        (f"{bucket}.s3.amazonaws.com",                     "初始探測 / us-east-1",       False),
        (f"s3.amazonaws.com",                              "全域 endpoint",              False),
        (f"{bucket}.s3-{region}.amazonaws.com",            "Legacy 連字號格式",          False),
        (f"s3-{region}.amazonaws.com",                     "Legacy 連字號格式",          False),
        (f"{bucket}.s3.dualstack.{region}.amazonaws.com",  "IPv6 dualstack",             False),
        (f"s3.dualstack.{region}.amazonaws.com",           "IPv6 dualstack path-style",  False),
    ]


def _resolve_dns(fqdn, timeout=3):
    """回傳 FQDN 的 A 記錄清單;失敗回 None。"""
    old_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(timeout)
    try:
        _, _, ips = socket.gethostbyname_ex(fqdn)
        return ips
    except Exception:
        return None
    finally:
        socket.setdefaulttimeout(old_timeout)


def probe_and_report_fqdns(bucket):
    """純探測:不需 AWS key,印出 region、所有候選 FQDN 與 DNS 解析結果。

    回傳偵測到的 region(字串)或 None(探測失敗)。
    """
    print(f"\n[探測] Endpoint & FQDN 清單 (無需 AWS 認證)")
    print("=" * 70)
    print(f"Bucket: {bucket}")

    region, status, err = _probe_bucket_region(bucket)
    if err:
        print(f"狀態: 【失敗】{err}")
        print("      → 下方 FQDN 以 <REGION> 佔位,請先排除網路/DNS 問題")
        display_region = "<REGION>"
    else:
        note = "(403/404 屬正常,代表網路可達 AWS)" if status in (403, 404) else ""
        print(f"HTTP 狀態: {status} {note}")
        print(f"偵測到 Region: {region}")
        display_region = region

    print("-" * 70)
    print("防火牆白名單(所有可能 FQDN,★ = boto3 實際會用):")
    print()
    header = f"{'':<3}{'FQDN':<66} {'說明':<28} DNS (A 記錄)"
    print(header)
    print("-" * len(header) + "-" * 20)

    for fqdn, desc, is_primary in _fqdn_candidates(bucket, display_region):
        marker = " ★ " if is_primary else "   "
        if display_region == "<REGION>":
            ips_str = "(region 未知)"
        else:
            ips = _resolve_dns(fqdn)
            ips_str = ", ".join(ips) if ips else "(DNS 解析失敗)"
        print(f"{marker}{fqdn:<66} {desc:<28} {ips_str}")

    print("-" * 70)
    return region if not err else None


# ── 測試模式 ──────────────────────────────────────────────────────────────────

def test_s3_log_paths(session, bucket_name, fqdn, org_id):
    s3 = session.client("s3")
    paths = [
        (f"{fqdn}/org_id={org_id}/auditable/",        "稽核日誌 (Auditable)"),
        (f"{fqdn}/org_id={org_id}/summaries/pd=0/",   "流量摘要 (pd=0 - Allowed)"),
        (f"{fqdn}/org_id={org_id}/summaries/pd=1/",   "流量摘要 (pd=1 - Potentially blocked)"),
        (f"{fqdn}/org_id={org_id}/summaries/pd=2/",   "流量摘要 (pd=2 - Blocked)"),
        (f"{fqdn}/org_id={org_id}/summaries/pd=3/",   "流量摘要 (pd=3 - Unknown)"),
    ]

    print(f"\n[模式: 測試] Bucket: {bucket_name}")
    print("=" * 70)
    for prefix, desc in paths:
        print(f"檢查: {desc}")
        print(f"路徑: s3://{bucket_name}/{prefix}")
        try:
            resp = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=3)
            if "Contents" in resp:
                print(f"狀態: 【成功】找到 {len(resp['Contents'])} 個物件（顯示前 3 筆）")
                for obj in resp["Contents"]:
                    print(f"  -> {obj['Key']}  ({obj['Size']/1024:.2f} KB)")
            else:
                print("狀態: 【警告】路徑可存取，但目前無檔案。")
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "AccessDenied":
                print("狀態: 【失敗】存取被拒絕，請確認金鑰權限。")
            elif code == "NoSuchBucket":
                print(f"狀態: 【失敗】找不到 Bucket: {bucket_name}")
                break
            else:
                print(f"狀態: 【失敗】API 錯誤: {e}")
        print("-" * 70)


def test_sqs_access(session, sqs_url):
    sqs = session.client("sqs")
    print(f"\n[模式: SQS] 佇列: {sqs_url}")
    print("=" * 70)
    try:
        resp = sqs.receive_message(QueueUrl=sqs_url, MaxNumberOfMessages=3, WaitTimeSeconds=3)
        msgs = resp.get("Messages", [])
        if msgs:
            print(f"狀態: 【成功】拉取到 {len(msgs)} 則訊息（未刪除）")
            for i, m in enumerate(msgs, 1):
                print(f"  -> 訊息 {i} ID: {m['MessageId']}")
                print(f"     預覽: {m['Body'][:100].replace(chr(10), ' ')}...")
        else:
            print("狀態: 【警告】連線成功，佇列目前為空。")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("AccessDenied", "AWS.SimpleQueueService.NonExistentQueue"):
            print(f"狀態: 【失敗】{code}，請確認金鑰權限或 SQS URL。")
        else:
            print(f"狀態: 【失敗】API 錯誤: {e}")
    print("-" * 70)


# ── 列出模式 ──────────────────────────────────────────────────────────────────

def list_s3_files(session, bucket, prefix, max_keys=100):
    print(f"\n[模式: 列出] s3://{bucket}/{prefix}")
    print(f"最多顯示 {max_keys} 筆")
    print("=" * 70)

    total = 0
    try:
        for obj in _iter_s3_objects(session, bucket, prefix, max_keys=max_keys):
            last_mod = obj["LastModified"].strftime("%Y-%m-%d %H:%M:%S UTC")
            size_kb = obj["Size"] / 1024
            print(f"  {obj['Key']}")
            print(f"    大小: {size_kb:>8.1f} KB   修改時間: {last_mod}")
            total += 1
    except ClientError as e:
        print(f"【錯誤】{e}")
        sys.exit(1)

    print("-" * 70)
    print(f"共顯示 {total} 個檔案（限制 {max_keys} 筆）")
    if total == max_keys:
        print("  （可能還有更多，用 --max-keys 增加上限）")


# ── 下載模式 ──────────────────────────────────────────────────────────────────

def download_s3_files(session, bucket, prefix=None, key=None, out_dir="."):
    s3 = session.client("s3")
    out_path = os.path.abspath(out_dir)
    os.makedirs(out_path, exist_ok=True)

    print(f"\n[模式: 下載] 目的地: {out_path}")
    print("=" * 70)

    ok = fail = 0
    found_any = False

    try:
        if key:
            keys = [key]
            preserve_hierarchy = False
        else:
            print(f"列出 s3://{bucket}/{prefix} ...")
            keys = (obj["Key"] for obj in _iter_s3_objects(session, bucket, prefix))
            preserve_hierarchy = True

        for k in keys:
            found_any = True
            dest = _download_destination(out_path, k, prefix=prefix, preserve_hierarchy=preserve_hierarchy)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            print(f"  {k}")
            try:
                s3.download_file(bucket, k, dest)
                size = os.path.getsize(dest)
                print(f"    -> 完成: {dest}  ({size/1024:.1f} KB)")
                ok += 1
            except ClientError as e:
                print(f"    -> 【失敗】{e}")
                fail += 1
    except ClientError as e:
        print(f"【錯誤】列出失敗: {e}")
        sys.exit(1)

    if not found_any:
        print("【警告】找不到任何檔案。")
        return

    print("-" * 70)
    print(f"下載完成：成功 {ok} / 失敗 {fail}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def build_parser():
    parser = argparse.ArgumentParser(
        description="Illumio S3 log 工具：連線測試 / 瀏覽 / 下載",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # `--config` 用來提供預設值；若同一欄位 CLI 有另外指定，CLI 會覆蓋 config。
    parser.add_argument("--config", help="讀取 config.yaml 的 aws/source 設定")

    # 目標：S3 bucket 或 SQS URL（二選一）
    target = parser.add_mutually_exclusive_group()
    target.add_argument("--bucket",  help="S3 Bucket 名稱")
    target.add_argument("--sqs-url", help="SQS Queue URL")

    # S3 路徑參數
    parser.add_argument("--fqdn",   help="PCE FQDN（例如 ap-scp45.illum.io）")
    parser.add_argument("--org-id", help="PCE Org ID（純數字）")
    parser.add_argument("--log-type", choices=list(_LOG_TYPE_SUBPATH), metavar="TYPE",
                        help="log 類型：" + " / ".join(_LOG_TYPE_SUBPATH))
    parser.add_argument("--prefix", help="自訂 S3 路徑前綴（優先於 --fqdn/--org-id/--log-type）")

    # 操作模式
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--list",     action="store_true", help="列出 S3 檔案")
    mode.add_argument("--download", action="store_true", help="下載 S3 檔案")

    # 各模式的選用參數
    parser.add_argument("--key",      help="[--download] 指定單一 S3 key")
    parser.add_argument("--max-keys", type=int, default=100,
                        help="[--list] 最多顯示幾筆（預設 100）")
    parser.add_argument("--out",      default=".",
                        help="[--download] 下載目的目錄（預設：當前目錄）")

    # AWS 認證
    parser.add_argument("--profile",    help="AWS CLI Profile 名稱")
    parser.add_argument("--access-key", help="AWS Access Key ID")
    parser.add_argument("--secret-key", help="AWS Secret Access Key")
    parser.add_argument("--region",     help="AWS 區域（通常不需要填）")
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

    config = {}
    if args.config:
        try:
            config = _load_yaml_config(args.config)
        except ValueError as e:
            print(f"【錯誤】{e}")
            return 1

    try:
        # 載入後的有效值規則：CLI > config.yaml。
        bucket, fqdn, org_id = _resolve_s3_source(args, config)
        profile, access_key, secret_key, region = _resolve_aws_auth(args, config)
    except ValueError as e:
        print(f"【錯誤】{e}")
        return 1

    if not bucket and not args.sqs_url:
        print("【錯誤】需要提供 --bucket、--sqs-url，或用 --config 載入 source.bucket。")
        return 1

    if bool(access_key) != bool(secret_key):
        print("【錯誤】--access-key 與 --secret-key 必須同時提供。")
        return 1

    # 預設測試模式:先做純網路探測(不需 AWS key),方便盤點防火牆白名單
    is_default_test_mode = not (args.list or args.download or args.sqs_url)
    if is_default_test_mode and bucket:
        probe_and_report_fqdns(bucket)

    session = get_aws_session(profile, access_key, secret_key, region)

    # ── SQS ──
    if args.sqs_url:
        test_sqs_access(session, args.sqs_url)
        return 0

    # ── S3 ──
    if args.list:
        # 決定 prefix：--prefix > fqdn+org-id+log-type
        if args.prefix:
            prefix = args.prefix
        else:
            if not fqdn or not org_id:
                print("【錯誤】--list 需要 --prefix，或同時提供 --fqdn 與 --org-id。")
                return 1
            prefix = _build_prefix(fqdn, org_id, args.log_type)
        list_s3_files(session, bucket, prefix, max_keys=args.max_keys)

    elif args.download:
        if args.key:
            # 單一檔案
            download_s3_files(session, bucket, key=args.key, out_dir=args.out)
        elif args.prefix:
            download_s3_files(session, bucket, prefix=args.prefix, out_dir=args.out)
        else:
            if not fqdn or not org_id:
                print("【錯誤】--download 需要 --key、--prefix，或同時提供 --fqdn 與 --org-id。")
                return 1
            prefix = _build_prefix(fqdn, org_id, args.log_type)
            download_s3_files(session, bucket, prefix=prefix, out_dir=args.out)

    else:
        # 預設：連線測試
        if not fqdn or not org_id:
            print("【錯誤】測試模式需要同時提供 --fqdn 與 --org-id。")
            return 1
        test_s3_log_paths(session, bucket, fqdn, org_id)

    return 0


if __name__ == "__main__":
    sys.exit(main())
