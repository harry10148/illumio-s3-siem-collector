# Task

## Active

- [ ] 可選：調查 pytest 預設 capture 模式在此環境的 I/O 例外，避免必須使用 `-s`

### 2026-04-22 本地留存功能
- [x] `sinks/file_sink.py`：rolling append + gzip rotation + retention cleanup
- [x] `sinks/multi_sink.py`：fan-out，全部失敗才回傳 False
- [x] `core/config.py`：`NetworkSinkConfig` / `FileSinkConfig` / `MultiSinkConfig` discriminated union
- [x] `core/pipeline.py`：提取 `_build_sink` helper，加 file / multi 分支
- [x] `tests/test_file_sink.py`：15 個 FileSink + MultiSink 測試
- [x] `config.example.yaml`：file sink 參數完整說明 + audit-local / traffic-local / audit-dual / blocked-dual 範例
- [x] 全 89 tests passed

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

### 2026-04-22 s3_log_checker 防火牆盤點輔助
- [x] 新增匿名 endpoint 探測(urllib HEAD 讀 `x-amz-bucket-region`)
- [x] 列出所有 S3 FQDN 變體(virtual-hosted / path-style / legacy hyphen / dualstack)
- [x] 每個 FQDN 附上 DNS A 記錄(利於嚴格防火牆白名單鎖 IP)
- [x] 預設測試模式自動先探測(不需 AWS key),--list/--download/--sqs-url 不觸發

### 2026-04-21 s3_log_checker 維護
- [x] 修正 `--list` 上限控制與 0-byte directory marker 顯示問題
- [x] 修正 `--download --prefix` 同名檔案互相覆蓋問題
- [x] 新增 `--config`，可讀取 `config.yaml` 的 `aws` / `source`
- [x] 新增回歸測試（list / download / config）
- [x] 補充 `s3_log_checker.py` 腳本內建操作說明與參數優先順序註解
- [x] 統一 `config.example.yaml` / `config.yaml` 的 `mapper` YAML 寫法

### 2026-04-21 code review（穩定性 / 可靠性 / 資安）
- [x] 審查 `core/pipeline.py`、`sinks/https_sink.py`、`sources/s3_source.py` 的失敗處理與資料一致性風險
- [x] 審查 `scripts/install.ps1` 預設服務帳號權限模型
- [x] 審查離線 bundle 建置流程（runtime / NSSM / wheels 供應鏈完整性）
- [x] 驗證測試可執行性（`.venv/bin/pytest -q -s tests`）

### 2026-04-21 review findings 修復
- [x] 修正 HTTPS sink flush 失敗後重複 append / buffer 持續成長風險
- [x] 修正 daemon 缺少 batch flush / shutdown close 風險
- [x] 修正 S3 candidate 全量累積造成記憶體與延遲風險
- [x] 修正 Windows install 預設 LocalSystem 權限過高
- [x] 修正離線 bundle 缺少上游下載完整性驗證（SHA256）
- [x] 新增/更新回歸測試並完成驗證（74 passed）
- [x] 整理 README / `s3_log_checker` / review fixes 變更，完成提交前收尾

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
