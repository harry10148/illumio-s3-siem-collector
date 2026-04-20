# Illumio S3 → SIEM Collector

Pull Illumio PCE logs (auditable events, traffic summaries) from an AWS S3
bucket, convert them to Syslog-JSON / CEF / JSON, and forward them to a SIEM
over UDP / TCP / TLS / HTTPS.

- **Multi-pipeline** — each log type can go to a different destination with its own poll interval, format, and filter.
- **Built-in scheduler** (APScheduler) — no external cron / Task Scheduler.
- **Checkpoint** via atomic JSON file; resumes after restart with at-least-once semantics.
- **Offline-installable** bundles for Linux and Windows: target needs no Python, no pip, no internet.

---

## 目錄

1. [選擇安裝方式](#選擇安裝方式)
2. [方式一：git clone（目標主機有網路）](#方式一git-clone目標主機有網路)
3. [方式二：離線 bundle（目標主機無網路）](#方式二離線-bundle目標主機無網路)
4. [設定說明](#設定說明)
5. [完整參數說明](#完整參數說明)
6. [日常操作](#日常操作)
7. [更新](#更新)
8. [解除安裝](#解除安裝)
9. [排錯](#排錯)
10. [SIEM 設定](#siem-設定)

---

## 選擇安裝方式

| 方式 | 目標主機需要 | 適用場景 |
|---|---|---|
| **git clone** | Python 3.x、pip、網路 | 開發測試、目標主機可聯網 |
| **離線 bundle** | 無（Python runtime 已內含） | 客戶環境、無法連網的生產主機 |

---

## 方式一：git clone（目標主機有網路）

前提：目標主機有 `python3`（3.9+）、`pip`、`git`。

### Linux

```bash
# 1. Clone 專案
git clone <repo_url>
cd illumio_s3_collector

# 2. 建立 virtualenv 並安裝相依套件
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 3. 準備設定
cp config.example.yaml config.yaml
vi config.yaml          # 填入 AWS 認證 + SIEM IP/port（見下方設定說明）

# 4. Preflight 測試（不需要 sudo）
bash scripts/preflight.sh --config config.yaml --test-s3
# → 結尾出現 PASS 再繼續

# 5. 正式安裝成 systemd 服務（建立 venv + service）
sudo bash scripts/install.sh

# 6. 填好 config 後啟動
sudo vi /etc/illumio-collector/config.yaml
sudo systemctl start illumio-collector
sudo journalctl -u illumio-collector -f
```

> **不想裝成服務，只想直接跑：**
> ```bash
> source venv/bin/activate
> python collector.py --config config.yaml
> ```

### Windows

前提：`python`（3.9+）已安裝且在 PATH。

```powershell
# 1. Clone 專案
git clone <repo_url>
cd illumio_s3_collector

# 2. 建立 virtualenv 並安裝相依套件
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

# 3. 準備設定
Copy-Item config.example.yaml config.yaml
notepad config.yaml      # 填入 AWS 認證 + SIEM IP/port

# 4. Preflight 測試（不需要 Administrator）
.\scripts\preflight.ps1 -Config config.yaml -TestS3
# → 結尾出現 PASS 再繼續

# 5. 正式安裝成 Windows 服務 — 需要 Administrator PowerShell
.\scripts\install.ps1

# 6. 填好 config 後啟動
notepad "C:\Program Files\illumio-collector\config.yaml"
Start-Service IllumioCollector
Get-Content "C:\Program Files\illumio-collector\logs\collector.log" -Wait
```

> **不想裝成服務，只想直接跑：**
> ```powershell
> venv\Scripts\activate
> python collector.py --config config.yaml
> ```

---

## 方式二：離線 bundle（目標主機無網路）

Bundle 內含 Python 3.11 runtime + 所有 wheels，目標主機完全不需要 Python 或網路。

### Linux

**Step 1 — 在有網路的 build host 建 bundle**

```bash
git clone <repo_url>
cd illumio_s3_collector
bash scripts/build_offline_bundle.sh
# → dist/illumio-collector-linux-x86_64-v1.0.tar.gz  (~47 MB)
```

**Step 2 — 把 `.tar.gz` 複製到目標主機**（USB、SCP、跳板機等）

**Step 3 — 在目標主機安裝**

```bash
tar xzf illumio-collector-linux-x86_64-v1.0.tar.gz
cd bundle

# （選用）先跑 preflight 確認環境沒問題
cp app/config.example.yaml /tmp/config.yaml && vi /tmp/config.yaml
bash preflight.sh --config /tmp/config.yaml --test-s3

# 正式安裝
sudo ./install.sh
sudo vi /etc/illumio-collector/config.yaml
sudo systemctl start illumio-collector
sudo journalctl -u illumio-collector -f
```

### Windows

**Step 1 — 在有網路的 build host 建 bundle**

```powershell
git clone <repo_url>
cd illumio_s3_collector
.\scripts\build_offline_bundle.ps1
# → dist\illumio-collector-windows-x86_64-v1.0.zip
```

**Step 2 — 把 `.zip` 複製到目標主機**

**Step 3 — 以 Administrator PowerShell 安裝**

```powershell
Expand-Archive illumio-collector-windows-x86_64-v1.0.zip C:\illumio-bundle
cd C:\illumio-bundle

# （選用）preflight
Copy-Item app\config.example.yaml C:\temp\config.yaml
notepad C:\temp\config.yaml
.\preflight.ps1 -Config C:\temp\config.yaml -TestS3

# 正式安裝
.\install.ps1
notepad "C:\Program Files\illumio-collector\config.yaml"
Start-Service IllumioCollector
Get-Content "C:\Program Files\illumio-collector\logs\collector.log" -Wait
```

### 安裝後建立的資源

#### Linux

**目錄與檔案**

| 路徑 | bundle | git clone | 說明 |
|---|:---:|:---:|---|
| `/opt/illumio-collector/app/` | ✅ | ✅ | 程式碼（collector.py、core/、sinks/ 等） |
| `/opt/illumio-collector/python/` | ✅ | — | Python 3.11 standalone runtime |
| `/opt/illumio-collector/venv/` | — | ✅ | Python virtualenv |
| `/opt/illumio-collector/wheels/` | ✅ | — | pip wheel 快取（安裝完可刪） |
| `/opt/illumio-collector/uninstall.sh` | ✅ | ✅ | 解除安裝腳本 |
| `/opt/illumio-collector/INSTALL_META` | ✅ | ✅ | 安裝時間、模式、服務帳號紀錄 |
| `/opt/illumio-collector/VERSION` | ✅ | — | bundle 版本資訊 |
| `/etc/illumio-collector/config.yaml` | ✅ | ✅ | **主設定檔（需填入認證）** 權限 600 |
| `/var/lib/illumio-collector/state/` | ✅ | ✅ | Checkpoint 檔（每個 pipeline 一個 JSON） |
| `/var/log/illumio-collector/` | ✅ | ✅ | Log 檔（含 rotate） |
| `/etc/systemd/system/illumio-collector.service` | ✅ | ✅ | systemd unit 檔 |

**系統服務與使用者**

```
服務名稱：illumio-collector
服務狀態：enabled（開機自動啟動，未啟動服務本身）
服務帳號：illumio-collector（系統帳號，no shell, no home）
          或 --user 指定的現有帳號
```

```bash
# 確認服務狀態
systemctl status illumio-collector

# 確認系統帳號
id illumio-collector

# 查看 systemd unit 內容
cat /etc/systemd/system/illumio-collector.service
```

---

#### Windows

**目錄與檔案**

預設安裝路徑：`C:\Program Files\illumio-collector\`

| 路徑 | bundle | git clone | 說明 |
|---|:---:|:---:|---|
| `...\app\` | ✅ | ✅ | 程式碼 |
| `...\python\` | ✅ | — | Python 3.11 standalone runtime |
| `...\venv\` | — | ✅ | Python virtualenv |
| `...\wheels\` | ✅ | — | pip wheel 快取 |
| `...\nssm\nssm-2.24\win64\nssm.exe` | ✅* | — | NSSM（若下載成功） |
| `...\uninstall.ps1` | ✅ | ✅ | 解除安裝腳本 |
| `...\INSTALL_META` | ✅ | ✅ | 安裝時間、模式、服務帳號紀錄 |
| `...\VERSION` | ✅ | — | bundle 版本資訊 |
| `...\config.yaml` | ✅ | ✅ | **主設定檔（需填入認證）** |
| `...\state\` | ✅ | ✅ | Checkpoint 檔 |
| `...\logs\` | ✅ | ✅ | Log 檔 |
| `...\logs\collector.log` | ✅ | ✅ | Python logging 輸出 |
| `...\logs\nssm-stdout.log` | ✅* | — | stdout（NSSM 模式） |
| `...\logs\nssm-stderr.log` | ✅* | — | stderr（NSSM 模式） |

\* NSSM 相關項目只在 NSSM 下載成功時存在；否則使用 New-Service fallback。

**Windows 服務**

```
服務名稱（Name）：IllumioCollector
顯示名稱：Illumio S3 to SIEM Collector
啟動類型：Automatic（開機自動啟動，未啟動服務本身）
服務帳號：LocalSystem（預設）或 -ServiceAccount 指定的帳號
```

```powershell
# 確認服務已建立
Get-Service IllumioCollector

# 查看詳細資訊
Get-Service IllumioCollector | Select-Object *
```

---

## 設定說明

### Step 1 — 決定要收哪些 log

| `log_type` | 中文說明 | 建議 |
|---|---|---|
| `auditable` | **稽核事件** — 管理操作、VEN 狀態、登入、policy 部署 | ✅ 必收 |
| `pd2` | **封鎖流量** — 被 policy 實際封鎖的連線 | ✅ 必收 |
| `pd1` | **潛在封鎖** — Enforce 模式下會被封鎖的連線 | ✅ 建議收 |
| `pd0` | **允許流量** — 所有允許連線（量大，約佔 95%+） | ⚠️ 選收 |
| `pd3` | **未知流量** — policy 無法判定的連線 | ⚪ 視需要 |

> **最小配置建議：** 只開 `auditable` + `pd2`（稽核 + 封鎖），涵蓋資安告警核心需求。

### Step 2 — 最小可用 config

複製 `config.example.yaml` → `config.yaml`，修改以下必填欄位：

```yaml
aws:
  access_key: "AKIA..."           # AWS Access Key ID（原廠文件提供）
  secret_key: "..."               # AWS Secret Access Key（原廠文件提供）

source:
  bucket: "illumio-flow-..."      # S3 bucket 名稱
  fqdn: "your-pce.illum.io"       # PCE FQDN（在 bucket 路徑裡）
  org_id: "123456"                # PCE Org ID（在 bucket 路徑裡）

pipelines:
  - name: "audit"
    log_type: auditable
    poll_interval_sec: 60
    mapper: { format: syslog_json }
    sink:
      type: tls
      host: "siem.example.com"
      port: 6514

  - name: "blocked"
    log_type: pd2
    poll_interval_sec: 60
    mapper: { format: syslog_json }
    sink:
      type: tls
      host: "siem.example.com"
      port: 6514
```

### Step 3 — 驗證與測試

```bash
# 驗證 config 語法（不連 S3 / SIEM）
python collector.py --config config.yaml --dry-run

# 跑一次 pipeline 確認有資料送出
python collector.py --config config.yaml --once audit

# 確認 SIEM 收到後再啟動排程
python collector.py --config config.yaml
```

---

## 完整參數說明

### `aws` 區塊

| 參數 | 說明 | 範例 |
|---|---|---|
| `access_key` | AWS Access Key ID | `"AKIA..."` |
| `secret_key` | AWS Secret Access Key | `"abc123..."` |
| `region` | S3 bucket region（**選填**，可填 `null`） | `"ap-northeast-1"` |
| `profile` | 使用 AWS CLI profile（與 key/secret 二選一） | `"my-profile"` |

> `region` 可以不填。boto3 會自動偵測，只有在出現 `AuthorizationHeaderMalformed` 錯誤時才需要填。

### `source` 區塊

| 參數 | 說明 | 如何取得 |
|---|---|---|
| `bucket` | S3 bucket 名稱 | 原廠 Access Key 文件提供 |
| `fqdn` | PCE FQDN | 原廠文件，格式 `xx-scpYY.illum.io` |
| `org_id` | PCE Org ID | 原廠文件，純數字 |

> `fqdn` 和 `org_id` 是 S3 路徑的一部分：`{fqdn}/org_id={org_id}/auditable/...`，填錯會找不到檔案。

### `checkpoint` 區塊

| 參數 | 預設 | 說明 |
|---|---|---|
| `dir` | `./state` | checkpoint 存放目錄 |
| `initial_lookback_hours` | `0` | 第一次啟動往回拉幾小時（0 = 只拉新資料） |
| `atomic_write` | `true` | 先寫暫存再 rename，防止 crash 時損毀 |

### `pipelines` 區塊

| 欄位 | 必填 | 預設 | 說明 |
|---|---|---|---|
| `name` | ✅ | — | 唯一名稱（英數+連字號），用於 checkpoint 檔名 |
| `log_type` | ✅ | — | `auditable` / `pd0` / `pd1` / `pd2` / `pd3` |
| `enabled` | | `true` | `false` 可暫時停用 |
| `poll_interval_sec` | | `60` | 最小 10 秒 |
| `max_files_per_tick` | | `1000` | 每次最多處理幾個 S3 檔案 |
| `filter.expression` | | — | 事件過濾條件 |
| `mapper.format` | | `syslog_json` | 輸出格式 |
| `mapper.flatten` | | `true` | 展平巢狀 JSON（SIEM 需要 true） |
| `sink.type` | ✅ | — | `tls` / `tcp` / `udp` / `https` |

#### `sink.type` — 傳輸方式

四種 type 的選擇指引：

| type | 何時用 | SIEM 預設 port |
|---|---|---|
| `tls` | **推薦**。加密傳輸，防竊聽，適合跨網段 | 6514 |
| `tcp` | 內部網路，不需加密，SIEM 不支援 TLS 時 | 1470 |
| `udp` | 同網段、對效能要求高、可接受掉包 | 514 |
| `https` | SIEM 有 HTTP rawupload API（如 Splunk HEC、Elastic） | 443 |

---

**`type: tls`** — TCP + TLS 加密（推薦）

```yaml
sink:
  type: tls
  host: "siem.example.com"   # 必填：SIEM IP 或 hostname
  port: 6514                 # 必填：SIEM 的 TLS syslog port
  tls:
    verify: true             # 預設 true；自簽憑證時改 false
    ca_file: null            # 自訂 CA 路徑（null = 使用系統 CA）
  timeout_sec: 10            # 連線 / 傳送逾時（秒）
  max_retries: 3             # 失敗最多重試幾次
  retry_backoff_sec:         # 每次重試等待秒數
    - 1
    - 2
    - 4
```

---

**`type: tcp`** — 純 TCP 明文

```yaml
sink:
  type: tcp
  host: "192.168.1.50"
  port: 1470
  timeout_sec: 10
  max_retries: 3
  retry_backoff_sec: [1, 2, 4]
```

---

**`type: udp`** — UDP（fire-and-forget）

```yaml
sink:
  type: udp
  host: "192.168.1.50"
  port: 514
  max_bytes: 8192            # 單則最大 bytes；超過就截斷並警告
                             # 0 = 不限制（不建議）
                             # 1472 = Ethernet 不分片上限
```

> UDP 沒有重試和逾時設定（無連線）。如果 log 中出現 `truncating` 警告，建議改用 `tcp` 或 `tls`。

---

**`type: https`** — HTTPS batch POST（NDJSON）

```yaml
sink:
  type: https
  url: "https://siem.example.com/rawupload?vendor=Illumio&model=PCE"  # 必填
  batch_size: 100            # 累積幾筆再一次送出（預設 100）
  tls:
    verify: true             # false = 停用憑證驗證（自簽憑證用）
  timeout_sec: 10
  max_retries: 3
  retry_backoff_sec: [1, 2, 4]
```

> HTTPS sink 用 NDJSON 格式（每行一筆 JSON）batch POST，適合 Splunk HEC 或 Elastic Bulk API。

#### `mapper` — 輸出格式設定

```yaml
mapper:
  format: syslog_json        # 必填：syslog_json / cef / json
  flatten: true              # 展平巢狀 JSON（SIEM 需要 true）
  flatten_separator: "_"     # 巢狀路徑的分隔符（預設底線）
  flatten_max_depth: 10      # 最大展平層數
  array_strategy: stringify  # 陣列處理：stringify / first / skip
  mapping_file: null         # CEF 格式必填；其餘可省略
```

| `format` | 輸出樣式 | 適用場景 |
|---|---|---|
| `syslog_json` | RFC5424 header + 展平 JSON body | SIEM（**推薦**） |
| `cef` | CEF 格式，需搭配 `mapping_file` | 支援 CEF 的 SIEM（ArcSight 等） |
| `json` | 純 JSON（無 syslog header） | Splunk HEC / Elastic（配合 `https` sink） |

| `array_strategy` | 說明 |
|---|---|
| `stringify` | 陣列轉字串 `"[a,b,c]"`（預設） |
| `first` | 只取第一個元素 |
| `skip` | 完全略過含陣列的欄位 |

#### `filter.expression` — 事件過濾

```yaml
# 只收高風險 port 的允許流量
filter:
  expression: "ev.dst_port in (22, 445, 3389)"

# 只收 failure 等級的稽核事件
filter:
  expression: "ev.severity == 'err'"

# 組合條件
filter:
  expression: "ev.pd == 2 and ev.dst_port in (22, 445, 3389)"
```

> 欄位名稱參考：稽核事件 → `docs/Flow Logs and Auditable Event Logs for Illumio SaaS Core PCE.md`；流量事件 → `docs/Illumio PCE Traffic Summaries Log Format.md`

---

## 日常操作

### 查看服務狀態與 log

```bash
# Linux
sudo systemctl status illumio-collector
sudo journalctl -u illumio-collector -f

# 或直接看 log 檔
tail -f /var/log/illumio-collector/collector.log
```

```powershell
# Windows
Get-Service IllumioCollector
Get-Content "C:\Program Files\illumio-collector\logs\collector.log" -Wait
```

正常運作時每個 pipeline 每次 tick 會輸出一行：
```
tick: files=12 read=847 sent=847 filtered=0 failed=0 checkpoint=...20260420_abc.jsonl.gz duration=2.31s
```

| 欄位 | 說明 |
|---|---|
| `files` | 本次從 S3 取了幾個檔案 |
| `read` | 讀了幾行 JSON |
| `sent` | 成功送出幾則事件 |
| `filtered` | 被 filter 排除幾則 |
| `failed` | sink 送出失敗幾則 |

### 啟動 / 停止 / 重啟

```bash
# Linux
sudo systemctl start   illumio-collector
sudo systemctl stop    illumio-collector
sudo systemctl restart illumio-collector
```

```powershell
# Windows
Start-Service IllumioCollector
Stop-Service  IllumioCollector
Restart-Service IllumioCollector
```

### 修改設定

修改設定後需重啟服務才會生效：

```bash
# Linux
sudo vi /etc/illumio-collector/config.yaml

# 先驗證語法
sudo /opt/illumio-collector/python/bin/python3 \
  /opt/illumio-collector/app/collector.py \
  --config /etc/illumio-collector/config.yaml --dry-run

sudo systemctl restart illumio-collector
```

```powershell
# Windows
notepad "C:\Program Files\illumio-collector\config.yaml"
& "C:\Program Files\illumio-collector\python\python.exe" `
  "C:\Program Files\illumio-collector\app\collector.py" `
  --config "C:\Program Files\illumio-collector\config.yaml" --dry-run
Restart-Service IllumioCollector
```

### 重播歷史資料

刪除 checkpoint 後重啟，該 pipeline 從 `initial_lookback_hours` 指定的時間點重新拉取：

```bash
# Linux — 重播 audit pipeline
sudo systemctl stop illumio-collector
sudo rm /var/lib/illumio-collector/state/audit.json
sudo systemctl start illumio-collector

# 重播所有 pipeline（慎用，SIEM 會收到重複事件）
sudo rm /var/lib/illumio-collector/state/*.json
```

```powershell
# Windows
Stop-Service IllumioCollector
Remove-Item "C:\Program Files\illumio-collector\state\audit.json"
Start-Service IllumioCollector
```

---

## 更新

> **原則：** 更新只換程式碼與套件，**設定檔和 checkpoint 全部保留。**

### 離線 bundle 更新（Linux）

```bash
# 1. Build host：拉最新程式碼並重建 bundle
git pull && bash scripts/build_offline_bundle.sh

# 2. 複製新 bundle 到目標主機

# 3. 目標主機：停止服務、安裝、啟動
sudo systemctl stop illumio-collector
tar xzf illumio-collector-linux-x86_64-vX.X.tar.gz && cd bundle
sudo ./install.sh
sudo systemctl start illumio-collector
sudo journalctl -u illumio-collector -f
```

`install.sh` 更新時的行為：

| 路徑 | 行為 |
|---|---|
| `/opt/illumio-collector/app/` | **覆蓋**（新程式碼） |
| `/opt/illumio-collector/wheels/` | **覆蓋**（新套件） |
| `/opt/illumio-collector/python/` | **保留**（已存在就不動） |
| `/etc/illumio-collector/config.yaml` | **保留** |
| `/var/lib/illumio-collector/state/` | **保留** |

### git clone 更新（Linux）

```bash
git pull
sudo systemctl stop illumio-collector
sudo bash scripts/install.sh
sudo systemctl start illumio-collector
```

---

## 解除安裝

預設**保留** config 和 checkpoint，加 `--purge` / `-Purge` 才會一併刪除。

### Linux

```bash
# 保留 config + state（預設）
sudo /opt/illumio-collector/uninstall.sh

# 完全移除
sudo /opt/illumio-collector/uninstall.sh --purge
```

### Windows

```powershell
# 保留 config + state（預設）
& 'C:\Program Files\illumio-collector\uninstall.ps1'

# 完全移除
& 'C:\Program Files\illumio-collector\uninstall.ps1' -Purge
```

---

## 排錯

### 服務啟動失敗

```bash
sudo journalctl -u illumio-collector --no-pager | tail -30
```

常見原因：

| 症狀 | 處理方式 |
|---|---|
| `config.yaml` 語法錯誤 | `python collector.py --config config.yaml --dry-run` |
| SIEM host/port 無法連線 | `nc -zv <host> <port>`（TCP）；`nc -u -l <port>` 測試 UDP |
| S3 認證失敗 | 用 `s3_log_checker.py` 驗證（見下方） |
| `Read-only file system` | config 裡的路徑是相對路徑，重新執行 `install.sh` 讓它覆寫為絕對路徑 |

### S3 連線測試

```bash
# git clone 模式
python s3_log_checker.py --bucket <B> --fqdn <F> --org-id <ID> \
    --access-key <AK> --secret-key <SK>

# bundle 模式（已安裝）
sudo /opt/illumio-collector/python/bin/python3 \
  /opt/illumio-collector/app/s3_log_checker.py \
  --bucket <B> --fqdn <F> --org-id <ID> --access-key <AK> --secret-key <SK>
```

### 手動跑一次 pipeline（不啟動排程）

```bash
# git clone 模式
python collector.py --config config.yaml --once <pipeline-name>

# bundle 模式（已安裝）
sudo /opt/illumio-collector/python/bin/python3 \
  /opt/illumio-collector/app/collector.py \
  --config /etc/illumio-collector/config.yaml --once <pipeline-name>
```

### SIEM 沒收到事件

1. 確認 SIEM syslog receiver 已啟用（Admin → Device Support → Syslog）
2. 確認 port 和協定正確（TLS=6514, TCP=1470, UDP=514）
3. UDP 測試一定要加 `-u`：`nc -u -l 5514`
4. TLS 憑證錯誤時暫時改 `tls.verify: false` 確認連通性
5. 確認 SIEM custom parser 已匯入啟用（見 `siem_parser/README.md`）

### 事件量為 0（`sent=0`）

| 現象 | 原因 |
|---|---|
| `files=0` | S3 沒有新檔案（正常，PCE 無新事件） |
| `files>0` 但 `sent=0` | sink 連線問題，查 log 中的 `SinkSendError` |
| `filtered=read` | filter 條件過濾掉所有事件，檢查 `filter.expression` |

---

## SIEM 設定

Import parsers from `siem_parser/`:

1. SIEM GUI → Admin → Device Support → Parsers → **New** → Upload XML
2. 上傳 `siem_parser/IllumioPCE_Auditable.xml` 和 `IllumioPCE_Summaries.xml`
3. Set **Enabled = Yes** → **Apply**

詳細說明見 `siem_parser/README.md`。

---

## Architecture

```
  S3 Source ──> Mapper (flatten + format) ──> Sink (UDP/TCP/TLS/HTTPS)
                      │                              │
                 filter (opt)                  retry + backoff
                      │
                 checkpoint (atomic JSON)
```

Full design spec: `docs/superpowers/specs/2026-04-20-illumio-s3-siem-collector-design.md`

---

## License

TBD
