# Illumio S3 → SIEM Collector

Pull Illumio PCE logs (auditable events, traffic summaries) from an AWS S3
bucket, convert them to Syslog-JSON / CEF / JSON, and forward them to a SIEM
(designed for FortiSIEM) over UDP / TCP / TLS / HTTPS.

- Multi-pipeline: each log type can go to a different destination with its
  own poll interval, format, and filter.
- Built-in scheduler (APScheduler) — no external cron / Task Scheduler.
- Checkpoint via atomic JSON file; resumes after restart with at-least-once
  semantics (SIEM must tolerate duplicates; FortiSIEM rule-based dedup works).
- Offline-installable bundles for Linux and Windows: **target needs no
  Python, no pip, no internet**.

---

## Step 1 — 決定要收哪些 log

這是最重要的設定。每個 pipeline 對應 S3 bucket 裡的一個資料夾：

| `log_type` | 中文說明 | S3 路徑 | 建議收集 |
|---|---|---|---|
| `auditable` | **稽核事件** — PCE 管理操作、VEN 狀態變更、使用者登入、政策部署 | `{fqdn}/org_id={id}/auditable/` | ✅ 必收 |
| `pd2` | **封鎖流量** — 被 policy 實際封鎖的連線 | `{fqdn}/org_id={id}/pd=2/` | ✅ 必收 |
| `pd1` | **潛在封鎖** — 目前允許但在 Enforce 模式下會被封鎖的連線 | `{fqdn}/org_id={id}/pd=1/` | ✅ 建議收 |
| `pd0` | **允許流量** — 所有被允許的連線（量大，約佔 95%+） | `{fqdn}/org_id={id}/pd=0/` | ⚠️ 選收（量大） |
| `pd3` | **未知流量** — policy 無法判定的連線 | `{fqdn}/org_id={id}/pd=3/` | ⚪ 視需要 |

> **最小配置建議：** 只開 `auditable` + `pd2`（稽核 + 封鎖），這兩個涵蓋資安告警最核心的需求。

---

## Step 2 — 最小可用 config

複製 `config.example.yaml` → `config.yaml`，修改以下必填欄位：

```yaml
aws:
  access_key: "AKIA..."           # AWS Access Key ID
  secret_key: "..."               # AWS Secret Access Key
  region: "ap-northeast-1"        # S3 bucket 所在 region

source:
  bucket: "illumio-flow-..."      # S3 bucket 名稱
  fqdn: "ap-scp45.illum.io"       # PCE FQDN（在 bucket 路徑裡）
  org_id: "4456569"               # PCE Org ID（在 bucket 路徑裡）

pipelines:
  - name: "audit"
    log_type: auditable            # 稽核事件
    poll_interval_sec: 60
    mapper: { format: syslog_json }
    sink:
      type: tls
      host: "fortisiem.example.com"
      port: 6514

  - name: "blocked"
    log_type: pd2                  # 封鎖流量
    poll_interval_sec: 60
    mapper: { format: syslog_json }
    sink:
      type: tls
      host: "fortisiem.example.com"
      port: 6514
```

驗證設定：
```bash
python collector.py --config config.yaml --dry-run
```

---

## Step 3 — 測試單一 pipeline

```bash
python collector.py --config config.yaml --once audit
```

確認 FortiSIEM 收到訊息後再啟動正式排程：

```bash
python collector.py --config config.yaml
```

---

## 完整參數說明

### `aws` 區塊

| 參數 | 說明 | 範例 |
|---|---|---|
| `access_key` | AWS Access Key ID | `"AKIA..."` |
| `secret_key` | AWS Secret Access Key | `"abc123..."` |
| `region` | S3 bucket region | `"ap-northeast-1"` |
| `profile` | 使用 AWS CLI profile（與 key/secret 二選一） | `"my-profile"` |

> **如何找 region？** bucket 名稱通常含地區提示（`ap-` = Asia Pacific）。若不確定，用 `python s3_log_checker.py` 驗證。

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
| `dir` | `./state` | checkpoint 存放目錄；每個 pipeline 一個 JSON 檔 |
| `initial_lookback_hours` | `0` | 第一次啟動往回拉幾小時的資料（0 = 只拉新資料） |
| `atomic_write` | `true` | 寫入前先寫暫存再 rename，避免寫到一半損毀 |

> 刪除 `state/<pipeline-name>.json` 可以重播指定 pipeline 的歷史資料。

### `pipelines` 區塊

每個 pipeline 獨立，以下是完整欄位：

| 欄位 | 必填 | 預設 | 說明 |
|---|---|---|---|
| `name` | ✅ | — | 唯一名稱（英數+連字號），用於 checkpoint 檔名 |
| `log_type` | ✅ | — | 見下方 log_type 說明 |
| `enabled` | | `true` | `false` 可暫時停用不刪設定 |
| `poll_interval_sec` | | `60` | 多久拉一次，最小 10 秒 |
| `max_files_per_tick` | | `1000` | 每次最多處理幾個 S3 檔案（防止單次 tick 太久） |
| `filter.expression` | | — | 事件過濾條件（見下方） |
| `mapper.format` | | `syslog_json` | 輸出格式（見下方） |
| `mapper.flatten` | | `true` | 是否展平巢狀 JSON（FortiSIEM 需要 true） |
| `sink.type` | ✅ | — | 傳輸方式（見下方） |

#### `log_type` — 對應 S3 路徑與資料內容

| 值 | Illumio 術語 | 內容 | 典型量 |
|---|---|---|---|
| `auditable` | Auditable Events | 管理操作、VEN 上下線、policy 部署、登入登出 | 低 |
| `pd0` | Allowed Flows | policy decision = 0，允許的流量 | 極高 |
| `pd1` | Potentially Blocked | policy decision = 1，測試模式下潛在封鎖的流量 | 中 |
| `pd2` | Blocked Flows | policy decision = 2，實際封鎖的流量 | 低～中 |
| `pd3` | Unknown Flows | policy decision = 3，無法判定的流量 | 低 |

#### `sink.type` — 傳輸方式

| 值 | 說明 | 必填參數 | 限制 |
|---|---|---|---|
| `tls` | TCP + TLS（**推薦**） | `host`, `port` | 每則 ≤ 8192 bytes |
| `tcp` | 純 TCP（明文） | `host`, `port` | 每則 ≤ 8192 bytes |
| `udp` | UDP（無連線確認） | `host`, `port` | 每則 ≤ 1024 bytes |
| `https` | HTTPS batch POST | `url` | NDJSON，批次送出 |

FortiSIEM 預設監聽 port：TLS = **6514**，TCP = **1470**，UDP = **514**。

#### `mapper.format` — 輸出格式

| 值 | 說明 | 適用場景 |
|---|---|---|
| `syslog_json` | RFC5424 header + 展平 JSON body | FortiSIEM（**推薦**） |
| `cef` | CEF 格式，需 `mapping_file` | 其他支援 CEF 的 SIEM |
| `json` | 純 JSON，適合 HTTPS sink | Splunk / Elastic HTTP receiver |

#### `filter.expression` — 事件過濾

使用 `ev.欄位名` 存取事件欄位（支援巢狀路徑）：

```yaml
# 只收封鎖流量（pd2 已是單一 log_type，通常不需再 filter）
filter:
  expression: "ev.pd == 2"

# 只收特定 port 的流量
filter:
  expression: "ev.dst_port in (22, 445, 3389)"

# 排除 healthcheck agent
filter:
  expression: "ev.created_by.agent.hostname != 'healthcheck'"

# 只收 failure 等級的稽核事件
filter:
  expression: "ev.severity == 'err'"

# 組合條件
filter:
  expression: "ev.pd == 2 and ev.dst_port in (22, 445, 3389)"
```

> 欄位名稱參考：`auditable` 事件看 `docs/Flow Logs and Auditable Event Logs for Illumio SaaS Core PCE.md`；流量事件看 `docs/Illumio PCE Traffic Summaries Log Format.md`。

---

## Offline install

### Linux

```bash
./scripts/build_offline_bundle.sh
# -> dist/illumio-collector-linux-x86_64-v1.0.tar.gz
```

On the target:
```bash
tar xzf illumio-collector-linux-x86_64-v1.0.tar.gz
cd bundle
sudo ./install.sh
sudo vi /etc/illumio-collector/config.yaml
sudo systemctl start illumio-collector
sudo journalctl -u illumio-collector -f
```

### Windows

PowerShell on build host:
```powershell
.\scripts\build_offline_bundle.ps1
# -> dist\illumio-collector-windows-x86_64-v1.0.zip
```

Administrator PowerShell on target:
```powershell
Expand-Archive illumio-collector-windows-x86_64-v1.0.zip C:\illumio-bundle
cd C:\illumio-bundle
.\install.ps1
notepad C:\illumio-collector\config.yaml
& "C:\illumio-collector\nssm\nssm-2.24\win64\nssm.exe" start IllumioCollector
Get-Content C:\illumio-collector\logs\collector.log -Wait
```

---

## Operations

### 查看運作狀況

```bash
# Linux
sudo journalctl -u illumio-collector -f

# 或直接看 log 檔
tail -f /var/log/illumio-collector/collector.log
```

Log 每行會顯示：
```
tick: files=12 read=847 sent=847 filtered=0 failed=0 checkpoint=...20260420_abc.jsonl.gz duration=2.31s
```

### 重播歷史資料

刪除 checkpoint 檔後重啟即可重播：
```bash
# 重播 audit pipeline 的全部資料
sudo rm /var/lib/illumio-collector/state/audit.json
sudo systemctl restart illumio-collector
```

或透過 `initial_lookback_hours` 控制首次啟動往回拉多少小時（設定在 `checkpoint.initial_lookback_hours`）。

### Troubleshooting

```bash
# 1. 測試 S3 連線
python s3_log_checker.py --bucket <B> --fqdn <F> --org-id <ID> \
    --access-key <AK> --secret-key <SK>

# 2. 驗證 config 語法
python collector.py --config config.yaml --dry-run

# 3. 跑一次 pipeline 看輸出（不啟動排程）
python collector.py --config config.yaml --once <pipeline-name>
```

### Upgrading

1. Stop the service
2. Re-run `install.sh` / `install.ps1` from the new bundle
3. Config and state are preserved
4. Start the service

---

## FortiSIEM 設定

Import parsers from `fortisiem_parser/`. See `fortisiem_parser/README.md`.

---

## Architecture

Full design: `docs/superpowers/specs/2026-04-20-illumio-s3-siem-collector-design.md`.

```
  Source (S3) -> Mapper (flatten + format) -> Sink (UDP/TCP/TLS/HTTPS)
                        |                          |
                    filter (opt)               retry + backoff
```

## License

TBD
