# Task

## Active

*(none)*

---

## Done

### 核心實作（v1.0）
- [x] S3 source（分頁 list + gzip 讀取 + checkpoint）
- [x] Mapper（syslog_json / CEF / JSON，展平，filter）
- [x] Sink（UDP / TCP / TLS / HTTPS，retry + backoff）
- [x] Scheduler（APScheduler，multi-pipeline）
- [x] Config schema（Pydantic v2）
- [x] CLI（`--config`, `--dry-run`, `--once`）
- [x] SIEM parser XML（Auditable + Summaries）
- [x] 測試（unit tests + real bucket smoke test）
- [x] 離線 bundle（Linux + Windows）

### Post-v1.0 polish（本次）
- [x] 去品牌：FortiSIEM → SIEM，fortisiem_parser → siem_parser
- [x] README 整合 OPERATIONS.md，補 git-clone 安裝步驟
- [x] 設定文件：四種 sink type YAML 範例，mapper 全參數
- [x] Bundle：補 s3_log_checker.py，移除無用 systemd/
- [x] PowerShell 5.x 相容性修正（?.Source）
- [x] Preflight：-Config 選填，自動偵測 config.example.yaml
- [x] s3_log_checker.py：新增 --list、--download 模式
- [x] Windows 安裝目錄：→ Program Files\illumio-collector
- [x] install.ps1 編碼修正：Get-Content -Encoding UTF8
- [x] NSSM arguments 引號修正（路徑含空格）
- [x] README 安裝後資源清單（目錄/服務/帳號）
- [x] **Bug fix**：config.yaml 權限 600→640，service user 可讀

---

## 已知限制 / 未來考慮

- UDP `max_bytes` 超出仍只截斷警告，不會自動降級到 TCP
- SQS source 保留抽象但未實作
- CEF mapping 檔案需手動維護
- Windows 無 NSSM 時（nssm.cc 不穩），stdout/stderr 不自動捕捉到檔案（Python logging 仍會寫 collector.log）

---

## 環境備忘

- 真實 bucket：`illumio-flow-XXXXXXXX-your-bucket`（git-ignored）
- AWS region：`ap-northeast-1`（Tokyo）
- AWS 認證：`MSGI-S3-Access-Key.txt`（repo root，**勿 commit**）
- 本地測試 config：`config.sandbox.yaml`（git-ignored，含真實認證）
