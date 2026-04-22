# Status

## Current phase

**本地留存功能完成（`FileSink` + `MultiSink`）。89 tests passed。**

## 2026-04-21 Code Review Snapshot（穩定性 / 可靠性 / 資安）

- 已完成一次以生產風險為中心的審查（pipeline/sink/source/install/bundle）。
- 最高風險集中在 HTTPS batching 失敗重試語意與 daemon flush 行為。
- 次高風險在 S3 掃描記憶體上限、Windows 預設服務帳號權限、離線 bundle 供應鏈驗證。
- 測試現況：`.venv/bin/pytest -q -s tests` 可通過（74 passed），但預設 capture 模式在此環境會觸發 pytest I/O 例外。

## 2026-04-21 Fix Pass（review findings 已修復）

- HTTPS sink：加入 flush failure gate，避免失敗後重複 append 導致 buffer 無上限成長。
- Pipeline：每檔案處理完成後 `sink.flush()`；flush 失敗時 checkpoint 不前進。
- Scheduler：daemon 結束時統一關閉 sinks，避免 HTTPS batch 尾端事件遺失。
- S3 source：改為 bounded candidate selection，不再先累積全量 candidates。
- Windows install：預設 service account 改為 `NT AUTHORITY\NetworkService`。
- 離線 bundle：Python runtime/NSSM 下載加入 SHA256 驗證。
- 測試現況更新：`.venv/bin/pytest -q -s tests` → **74 passed**。

## Summary

Illumio S3 → SIEM 收集器，從 S3 bucket 拉 PCE log 並轉發到 SIEM。
支援 UDP / TCP / TLS / HTTPS、syslog_json / CEF / JSON 格式、Linux + Windows。

---

## Post-v1.0 本次完成項目

| 類型 | 內容 |
|---|---|
| **去品牌** | `fortisiem_parser/` → `siem_parser/`；所有 FortiSIEM → SIEM |
| **README 整合** | 合併 OPERATIONS.md；補 git-clone 安裝步驟（pip install）|
| **設定文件** | 四種 sink type 完整 YAML 範例；mapper 全參數說明 |
| **bundle 修正** | 補 `s3_log_checker.py`；移除 bundle 內無用的 `systemd/` |
| **PS5 相容性** | `?.Source` → PowerShell 5.x 相容寫法 |
| **preflight 改善** | `-Config` / `--config` 選填；自動偵測 `app/config.example.yaml` |
| **s3_log_checker 功能** | 新增 `--list`（分頁瀏覽）和 `--download`（單檔/批次下載）|
| **s3_log_checker 文件** | 腳本頂部補齊操作手冊、模式說明與 CLI > config 優先順序 |
| **YAML 一致性** | `config.example.yaml` / `config.yaml` 的 `mapper` 統一改為展開式區塊寫法 |
| **Windows 安裝目錄** | 預設 `C:\illumio-collector` → `C:\Program Files\illumio-collector` |
| **編碼修正** | install.ps1 的 `Get-Content` 加 `-Encoding UTF8`（中文不亂碼） |
| **NSSM 引號修正** | Arguments 字串內路徑加引號（含空格的 Program Files 路徑）|
| **安裝後文件** | README 新增完整目錄/檔案/服務清單（Linux + Windows、兩種安裝模式） |
| **service user 文件** | illumio-collector 系統帳號安全模型說明 |
| **⚠️ 嚴重 bug 修正** | `config.yaml` 從 `root:root 600` → `root:<service_user> 640`；service user 原本根本讀不到 config |

---

## 關鍵設計決策

| 面向 | 決策 |
|---|---|
| Pipeline 拓撲 | Multi-pipeline，各自獨立 checkpoint |
| 預設格式 | `syslog_json`（RFC5424 header + 展平 JSON）|
| 傳輸 | UDP / TCP / TLS / HTTPS；建議 TLS/6514 |
| 本地留存 | `file` sink：rolling append + gzip rotation；`multi` sink：SIEM + file 同時 |
| Multi sink 失敗語意 | 全部子 sink 失敗才停止 checkpoint；任一成功即繼續 |
| Source | S3 only；SQS 抽象保留待未來 |
| 排程 | APScheduler BlockingScheduler，每 pipeline 獨立 interval |
| Checkpoint | JSON 檔，`last_modified` + `last_key`，atomic write |
| 失敗處理 | retry + backoff；sink 失敗時 checkpoint 不前進 |
| 過濾 | simpleeval 表達式 |
| 展平 | 巢狀 JSON → `_` 分隔，陣列預設 stringify |
| 平台 | Linux（systemd）+ Windows（NSSM/New-Service）|
| 離線部署 | bundle 含 python-build-standalone；目標主機不需 Python |
| SIEM 解析 | Custom Parser XML 隨工具一起提供 |
| service user | `illumio-collector`（system account, nologin）；config 640 group-readable |

---

## 下一步（客戶部署）

1. 選擇部署方式：
   - 離線 bundle：`bash scripts/build_offline_bundle.sh` → 傳 tar.gz → `sudo ./install.sh`
   - git clone：`sudo bash scripts/install.sh`
2. 填入認證：`sudo vi /etc/illumio-collector/config.yaml`
3. 驗證：`--dry-run` + `--once audit`
4. 啟動：`sudo systemctl start illumio-collector`
5. 匯入 SIEM parser XML（`siem_parser/` 目錄）

---

## 重要路徑提醒

| 路徑 | 說明 |
|---|---|
| `/etc/illumio-collector/config.yaml` | 主設定（root:illumio-collector 640） |
| `/var/lib/illumio-collector/state/` | Checkpoint 檔 |
| `/var/log/illumio-collector/` | Log 檔 |
| `/opt/illumio-collector/uninstall.sh` | 解除安裝 |
| `C:\Program Files\illumio-collector\config.yaml` | Windows 主設定 |
