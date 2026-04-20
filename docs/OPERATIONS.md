# Illumio S3 → SIEM Collector — 操作手冊

## 目錄

1. [首次安裝](#首次安裝)
2. [日常操作](#日常操作)
3. [離線更新](#離線更新)
4. [設定變更](#設定變更)
5. [重播歷史資料](#重播歷史資料)
6. [解除安裝](#解除安裝)
7. [排錯](#排錯)

---

## 首次安裝

兩種安裝方式，結果相同（都會安裝成 systemd / Windows 服務）：

| 方式 | 適用場景 | 需要網路 |
|---|---|---|
| **離線 bundle** | 客戶端無法連網、生產環境 | 只有 build host 需要 |
| **git clone** | 開發 / 測試環境，或目標主機可聯網 | 目標主機需要 pip |

---

### 方式一：離線 bundle（推薦生產環境）

#### Linux

**Step 1 — 在有網路的 build host 準備 bundle**

```bash
git clone <repo_url>
cd illumio_s3_collector
bash scripts/build_offline_bundle.sh
# → dist/illumio-collector-linux-x86_64-v1.0.tar.gz
```

**Step 2 — 把 `.tar.gz` 複製到目標主機**（USB、SCP、跳板機等）

**Step 3 — 在目標主機安裝**

```bash
tar xzf illumio-collector-linux-x86_64-v1.0.tar.gz
cd bundle
sudo ./install.sh
```

#### Windows

**Step 1 — 在有網路的 build host 準備 bundle**

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
.\install.ps1
```

---

### 方式二：git clone（有網路的目標主機）

#### Linux

前提：目標主機需要 `python3`、`python3-venv`、`git`。

```bash
# 1. Clone 專案
git clone <repo_url>
cd illumio_s3_collector

# 2. 準備 config
cp config.example.yaml config.yaml
vi config.yaml    # 填入 AWS 認證 + FortiSIEM IP/port

# 3. Preflight 測試（不需要 sudo）
bash scripts/preflight.sh --config config.yaml --test-s3
# → 看到 PASS 再繼續

# 4. 正式安裝（建立 venv + systemd service）
sudo bash scripts/install.sh

# 5. 啟動
sudo systemctl start illumio-collector
sudo journalctl -u illumio-collector -f
```

#### Windows

前提：需要 Python 3.x 已安裝且在 PATH 中（`python --version` 可執行）、`git`。

```powershell
# 1. Clone 專案
git clone <repo_url>
cd illumio_s3_collector

# 2. 準備 config
Copy-Item config.example.yaml config.yaml
notepad config.yaml    # 填入 AWS 認證 + FortiSIEM IP/port

# 3. Preflight 測試（不需要 Administrator）
.\scripts\preflight.ps1 -Config config.yaml -TestS3
# → 看到 PASS 再繼續

# 4. 正式安裝（建立 venv + Windows service）— 需要 Administrator PowerShell
.\scripts\install.ps1

# 5. 啟動
Start-Service IllumioCollector
Get-Content C:\illumio-collector\logs\collector.log -Wait
```

> **更新（git clone 模式）：** `git pull` 後重新執行 `install.sh` / `install.ps1` 即可覆蓋新程式碼，config 和 checkpoint 不受影響。

---

### 安裝前測試（preflight check）

建議在執行 `install.sh` / `install.ps1` 前先跑一次 preflight，確認 Python、依賴套件、config 語法、S3 連線都沒問題。
全程不需要 sudo / Administrator，不寫入任何系統路徑，完成後自動清掉 temp。

兩種模式都支援（自動偵測）：
- **bundle 模式**：從 bundle 目錄執行，使用 bundle 內附的 Python + wheels
- **git clone 模式**：從 repo 的 `scripts/` 目錄執行，使用系統 Python + pip

#### Linux

```bash
# ---- bundle 模式 ----
tar xzf illumio-collector-linux-x86_64-v1.0.tar.gz
cd bundle
cp app/config.example.yaml /tmp/config.yaml && vi /tmp/config.yaml
bash preflight.sh --config /tmp/config.yaml --test-s3

# ---- git clone 模式 ----
cd illumio_s3_collector
bash scripts/preflight.sh --config config.yaml --test-s3
```

#### Windows

```powershell
# ---- bundle 模式 ----
Expand-Archive illumio-collector-windows-x86_64-v1.0.zip C:\illumio-bundle
cd C:\illumio-bundle
Copy-Item app\config.example.yaml C:\temp\config.yaml; notepad C:\temp\config.yaml
.\preflight.ps1 -Config C:\temp\config.yaml -TestS3

# ---- git clone 模式 ----
cd illumio_s3_collector
.\scripts\preflight.ps1 -Config config.yaml -TestS3
```

輸出結尾看到 `PASS` 就可以安心執行 `sudo ./install.sh` / `.\install.ps1`。

---

### 安裝後（兩種方式相同）

#### Linux

```bash
# 填入 AWS 認證 + FortiSIEM IP/port
sudo vi /etc/illumio-collector/config.yaml

# 驗證設定
sudo /opt/illumio-collector/python/bin/python3 \
  /opt/illumio-collector/app/collector.py \
  --config /etc/illumio-collector/config.yaml --dry-run

# 啟動服務
sudo systemctl start illumio-collector
sudo systemctl status illumio-collector
```

#### Windows

```powershell
notepad C:\illumio-collector\config.yaml

# 驗證設定
C:\illumio-collector\python\python.exe `
  C:\illumio-collector\app\collector.py `
  --config C:\illumio-collector\config.yaml --dry-run

# 啟動服務
Start-Service IllumioCollector
Get-Service IllumioCollector
```

### 安裝後的目錄結構

| 路徑 | 內容 |
|---|---|
| `/opt/illumio-collector/app/` | 程式碼 |
| `/opt/illumio-collector/python/` | Python 3.11 runtime（bundle 內建） |
| `/opt/illumio-collector/wheels/` | pip wheels（offline 安裝用） |
| `/etc/illumio-collector/config.yaml` | 設定檔（**需手動填入認證**） |
| `/var/lib/illumio-collector/state/` | Checkpoint 檔（每個 pipeline 一個） |
| `/var/log/illumio-collector/` | Log 檔 |

---

## 日常操作

### 查看服務狀態

```bash
sudo systemctl status illumio-collector
```

### 即時看 log

```bash
sudo journalctl -u illumio-collector -f
```

### 看 log 檔（有 rotate 的完整紀錄）

```bash
tail -f /var/log/illumio-collector/collector.log
```

正常運作時每個 pipeline 每次 tick 會輸出一行：

```
tick: files=12 read=847 sent=847 filtered=0 failed=0 \
      checkpoint=...20260420_abc.jsonl.gz duration=2.31s
```

| 欄位 | 說明 |
|---|---|
| `files` | 本次從 S3 拿了幾個檔案 |
| `read` | 讀了幾行 JSON |
| `sent` | 成功送出幾則事件 |
| `filtered` | 被 filter 條件排除幾則 |
| `failed` | sink 送出失敗幾則 |
| `checkpoint` | 目前指到哪個 S3 檔案 |

### 啟動 / 停止 / 重啟

```bash
sudo systemctl start   illumio-collector
sudo systemctl stop    illumio-collector
sudo systemctl restart illumio-collector
```

---

## 離線更新

> **原則：** 更新只更換程式碼和套件，**設定檔和 checkpoint 全部保留不動。**

### 步驟

**Step 1：在有網路的 build host 上拉最新程式碼並重新 build**

```bash
cd illumio_s3_collector
git pull
bash scripts/build_offline_bundle.sh
# → dist/illumio-collector-linux-x86_64-v1.0.tar.gz（或新版本號）
```

**Step 2：把新 bundle 複製到目標主機**

**Step 3：停止服務**

```bash
sudo systemctl stop illumio-collector
```

> ⚠️ 必須先停止服務，否則正在使用的 `.py` 檔案被覆蓋可能導致不可預期的行為。

**Step 4：解壓並執行 install.sh**

```bash
tar xzf illumio-collector-linux-x86_64-vX.X.tar.gz
cd bundle
sudo ./install.sh
```

`install.sh` 在更新時的行為：

| 項目 | 行為 |
|---|---|
| `/opt/illumio-collector/app/` | **覆蓋**（新程式碼） |
| `/opt/illumio-collector/wheels/` | **覆蓋**（新套件） |
| `/opt/illumio-collector/python/` | **保留**（已存在就不動） |
| `/etc/illumio-collector/config.yaml` | **保留**（已存在就不動） |
| `/var/lib/illumio-collector/state/` | **保留**（checkpoint 不動） |
| `/var/log/illumio-collector/` | **保留**（log 不動） |

**Step 5：啟動服務**

```bash
sudo systemctl start illumio-collector
sudo journalctl -u illumio-collector -f   # 確認正常運作
```

### 更新 Python runtime（通常不需要）

Python runtime 只有在新 bundle 包含不同 Python 版本時才需要更新。若有此需要：

```bash
sudo systemctl stop illumio-collector
sudo rm -rf /opt/illumio-collector/python   # 刪除舊 runtime
# 重新執行 install.sh，它會重新解壓新的 python-runtime.tar.gz
sudo ./install.sh
sudo systemctl start illumio-collector
```

---

## 設定變更

修改設定後需要重啟服務才會生效：

```bash
sudo vi /etc/illumio-collector/config.yaml

# 先驗證語法
sudo /opt/illumio-collector/python/bin/python3 \
  /opt/illumio-collector/app/collector.py \
  --config /etc/illumio-collector/config.yaml --dry-run

# 無誤後重啟
sudo systemctl restart illumio-collector
```

---

## 重播歷史資料

每個 pipeline 的 checkpoint 存在 `/var/lib/illumio-collector/state/<pipeline-name>.json`。

刪除 checkpoint 後重啟，該 pipeline 會從頭重新拉資料（從 `initial_lookback_hours` 設定的時間點起算）。

```bash
sudo systemctl stop illumio-collector

# 重播 audit pipeline
sudo rm /var/lib/illumio-collector/state/audit.json

# 重播所有 pipeline（慎用，可能產生大量重複事件）
sudo rm /var/lib/illumio-collector/state/*.json

sudo systemctl start illumio-collector
```

> ⚠️ FortiSIEM 會收到重複事件，請確認 SIEM 端的 dedup 規則已啟用。

---

## 解除安裝

解除安裝腳本預設**保留** config 和 checkpoint（state），方便之後重新安裝時不用重頭設定。
加上 `--purge` / `-Purge` 才會一併刪除。

### Linux

```bash
# 保留 config + state（預設）
sudo bash scripts/uninstall.sh

# 完全移除（含 config 和 checkpoint）
sudo bash scripts/uninstall.sh --purge
```

保留的路徑：

| 路徑 | 內容 |
|---|---|
| `/etc/illumio-collector/config.yaml` | 設定檔（含 AWS 認證） |
| `/var/lib/illumio-collector/state/` | Checkpoint 檔 |
| `/var/log/illumio-collector/` | Log 檔 |

### Windows

```powershell
# 保留 config + state（預設）
.\scripts\uninstall.ps1

# 完全移除
.\scripts\uninstall.ps1 -Purge
```

保留的路徑：

| 路徑 | 內容 |
|---|---|
| `C:\illumio-collector\config.yaml` | 設定檔 |
| `C:\illumio-collector\state\` | Checkpoint 檔 |

---

## 排錯

### 服務啟動失敗

```bash
sudo journalctl -u illumio-collector --no-pager | tail -30
```

常見原因：
- `config.yaml` 語法錯誤 → 用 `--dry-run` 驗證
- FortiSIEM host/port 無法連線 → 用 `nc -zv <host> <port>` 測試
- S3 認證失敗 → 用 `s3_log_checker.py` 驗證（見下方）

### S3 連線測試

```bash
sudo /opt/illumio-collector/python/bin/python3 \
  /opt/illumio-collector/app/s3_log_checker.py \
  --bucket <bucket> \
  --fqdn <fqdn> \
  --org-id <org_id> \
  --access-key <AK> \
  --secret-key <SK>
```

### 手動跑一次 pipeline（不啟動排程）

```bash
sudo /opt/illumio-collector/python/bin/python3 \
  /opt/illumio-collector/app/collector.py \
  --config /etc/illumio-collector/config.yaml \
  --once <pipeline-name>
```

### 送出的事件 FortiSIEM 沒收到

1. 確認 FortiSIEM 的 syslog receiver 已啟用（Admin → Device Support → Syslog）
2. 確認 port 和協定正確（TLS=6514, TCP=1470, UDP=514）
3. TLS sink 出現憑證錯誤時，暫時改 `tls.verify: false` 確認連通性
4. 確認 FortiSIEM custom parser 已匯入並啟用（見 `fortisiem_parser/README.md`）

### 事件量為 0（sent=0）

- `files=0`：S3 沒有新檔案 → 正常（若 PCE 沒有新 event）
- `files>0` 但 `sent=0`：sink 連線問題，查 log 中的 `SinkSendError`
- `filtered=read`：filter 條件過濾掉所有事件 → 檢查 `filter.expression`
