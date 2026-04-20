# Illumio S3 → SIEM Collector 設計規格

**日期**：2026-04-20
**狀態**：Draft (待 user review)
**作者**：Harry + Claude (brainstorm)

---

## 1. 目的與背景

Illumio SaaS PCE 目前透過 AWS S3 bucket 拋送兩類日誌：

- **Auditable events**：管理/稽核事件（登入、策略修改、VEN 生命週期…）
- **Traffic summaries**：流量摘要，按 policy decision 分 4 個 prefix
  - `pd=0` Allowed、`pd=1` Potentially blocked、`pd=2` Blocked、`pd=3` Unknown

客戶的 SIEM（FortiSIEM）不支援 S3 pull，只支援 Syslog/CEF/JSON over UDP/TCP/HTTPS。本工具做為 **S3 → SIEM collector**：定期從 S3 拉取新檔案、轉換格式、推送到 SIEM。

現有 `s3_log_checker.py` 為一次性連線檢查工具，將保留做為 smoke test。新 collector 為獨立程式。

---

## 2. Goals / Non-Goals

### Goals

- 從 S3 定期 pull Illumio 日誌，每類日誌獨立 pipeline
- 支援 3 種輸出格式：`syslog_json`（預設）、`cef`、`json`
- 支援 4 種傳輸：UDP、TCP、TLS/TCP、HTTPS
- 可設定哪些日誌類型要轉拋、poll 間隔、失敗重試
- Python 內建排程（APScheduler），不依賴外部 cron/Task Scheduler
- Checkpoint 機制確保不遺漏、不重送（容忍 at-least-once）
- 二級 filter：pipeline 內可依 event 欄位過濾
- **Linux + Windows 雙平台**，不使用 Docker
- 附 FortiSIEM Custom Parser XML 範本

### Non-Goals

- 不做 SQS 事件驅動模式（架構預留 `sources/base.py` 抽象，未來可加）
- 不做多 tenant（單一 bucket / 單一 PCE）
- 不做 disk spool（SIEM 長停機 → 依賴 checkpoint 不前進後重啟補送）
- 不做 log 內容修改 / 欄位黑名單（超出 collector 範疇）
- 不內建 Windows Service 封裝（用 NSSM 外掛；Linux 用 systemd unit 範例）

---

## 3. 資料事實（已實測驗證）

針對 `illumio-flow-XXXXXXXX-your-bucket` bucket，2026-04-20 全量 list 結果：

| Prefix | 檔案數 | 總大小 | 日期範圍 | 備註 |
|---|---|---|---|---|
| `auditable/` | 7,679 | 4.0 MB | 2026-02-26 ~ 04-20 | 每日約 142 檔 |
| `summaries/pd=0/` | 25,353 | 14.4 MB | 2026-03-02 ~ 04-20 | 每日約 507 檔 |
| `summaries/pd=1/` | 43,792 | 107.5 MB | 2026-03-02 ~ 04-20 | 每日約 876 檔（量最大） |
| `summaries/pd=2/` | 0 | 0 | — | 環境未 enforce，無 blocked |
| `summaries/pd=3/` | 7,984 | 4.5 MB | 2026-03-02 ~ 04-20 | 每日約 190 檔 |

**關鍵觀察**：

- 檔名 100% 符合 `{YYYYMMDD}_{uuid}.jsonl.gz`，無例外、無子目錄
- 檔名日期 vs S3 LastModified 日期 **0/10 mismatch**（僅檢查末 10 筆）
- 單檔含 1~100 行 JSON Lines，gzip 壓縮
- **同一天內 UUID 是亂數，字典序 ≠ 時序** → checkpoint 必須用 LastModified，不能只靠 key

---

## 4. 架構總覽

```
┌─────────────────────── collector.py (main) ───────────────────────┐
│                                                                    │
│  Config (YAML) → Pipeline Orchestrator → APScheduler              │
│                                                                    │
│  每條 pipeline 一個 job，獨立間隔、獨立格式、獨立目的                │
└──────────────────────────────┬─────────────────────────────────────┘
                               │
          ┌────────────────────┼────────────────────┐
          ▼                    ▼                    ▼
     ┌─────────┐          ┌─────────┐          ┌─────────┐
     │ Source  │   →      │ Mapper  │    →     │  Sink   │
     │ (S3)    │          │ (+filter│          │(TLS/TCP │
     │         │          │ +flatten│          │ UDP/HTTP)│
     │         │          │ +format)│          │         │
     └─────────┘          └─────────┘          └─────────┘
          │                    │                    │
     ┌─────────┐          ┌─────────┐          ┌─────────┐
     │Checkpoint│          │ (in-mem)│          │ Retry   │
     │(JSON file)         │          │          │ logic   │
     └─────────┘          └─────────┘          └─────────┘
```

### 三層抽象介面

- **Source**：提供 `iter_new_files(checkpoint) → Iterator[RawFile]`
- **Mapper**：提供 `format(event_dict) → bytes`
- **Sink**：提供 `send(wire_bytes) → bool` + `close()`

未來擴充（例如加 SQS source、Kafka sink）只需實作對應介面。

---

## 5. 目錄結構

```
illumio_s3_collector/
├── collector.py                  # 入口
├── core/
│   ├── __init__.py
│   ├── config.py                 # pydantic schema + YAML loader
│   ├── scheduler.py              # APScheduler wrapper
│   ├── pipeline.py               # 一條 pipeline 的 tick orchestrator
│   ├── checkpoint.py             # atomic JSON read/write
│   ├── expression_filter.py      # simpleeval-based expression matcher
│   └── logging_setup.py          # 應用 log 設定
├── sources/
│   ├── __init__.py
│   ├── base.py                   # Source 抽象介面
│   └── s3_source.py              # list_objects_v2 + LastModified filter
├── mappers/
│   ├── __init__.py
│   ├── base.py                   # Mapper 抽象介面
│   ├── _flatten.py               # nested JSON flattener
│   ├── passthrough.py            # 原樣 JSON
│   ├── syslog_json.py            # RFC5424 header + flattened JSON
│   └── cef.py                    # CEF + YAML mapping loader
├── mappings/
│   ├── auditable.yaml            # CEF 欄位對應（選配）
│   └── summaries.yaml            # CEF 欄位對應（選配）
├── sinks/
│   ├── __init__.py
│   ├── base.py                   # Sink 抽象介面
│   ├── udp_sink.py               # UDP socket（1024 byte 截斷）
│   ├── tcp_sink.py               # TCP 長連線 + reconnect
│   ├── tls_sink.py               # ssl-wrapped TCP（port 6514 預設）
│   └── https_sink.py             # requests.post + batch
├── fortisiem_parser/
│   ├── IllumioPCE_Auditable.xml  # FortiSIEM Custom Parser
│   ├── IllumioPCE_Summaries.xml
│   └── README.md
├── tests/
│   ├── test_flatten.py
│   ├── test_filter.py
│   ├── test_mappers.py
│   ├── test_checkpoint.py
│   └── fixtures/                 # 真實 Illumio JSON 樣本（去識別化）
├── config.example.yaml
├── config.yaml                   # runtime 設定（git-ignored）
├── state/                        # checkpoint 存放（runtime 建立，git-ignored）
├── logs/                         # 應用日誌（runtime 建立，git-ignored）
├── requirements.txt
├── README.md                     # 安裝、設定、部署 (NSSM/systemd)
├── s3_log_checker.py             # 保留：連線 smoke test
└── doc/                          # 既有 Illumio 文件
```

---

## 6. Config Schema（完整範例）

```yaml
# ===== 全域設定 =====
aws:
  # 三選一：profile / 直接金鑰 / 留空走環境變數 / IAM role
  profile: null
  access_key: "AKIA..."
  secret_key: "..."
  region: "ap-northeast-1"

source:
  type: s3                         # 未來擴充：sqs
  bucket: "illumio-flow-XXXXXXXX-your-bucket"
  fqdn: "your-pce.illum.io"
  org_id: "123456"

checkpoint:
  dir: "./state"
  initial_lookback_hours: 24       # 首次啟動回補時數
  atomic_write: true               # write-temp-then-rename

logging:
  level: INFO                      # DEBUG/INFO/WARN/ERROR
  dir: "./logs"
  file: "collector.log"
  rotate_mb: 50
  keep_files: 7
  console: true                    # 是否同步輸出到 stdout

# ===== Pipeline 列表 =====
pipelines:
  - name: "audit-to-fortisiem"
    enabled: true
    log_type: auditable            # auditable | pd0 | pd1 | pd2 | pd3
    poll_interval_sec: 60

    filter:                        # 選配；省略代表全收
      expression: "ev.pce_fqdn == 'your-pce.illum.io'"

    mapper:
      format: syslog_json          # syslog_json | cef | json
      flatten: true
      flatten_separator: "_"
      flatten_max_depth: 10
      array_strategy: stringify    # stringify | first | skip
      # 若 format=cef 才用：
      mapping_file: null           # "mappings/auditable.yaml"

    sink:
      type: tls                    # udp | tcp | tls | https
      host: "fortisiem.example.com"
      port: 6514
      tls:
        verify: true
        ca_file: null              # null 走系統 CA
      timeout_sec: 10
      max_retries: 3
      retry_backoff_sec: [1, 2, 4]
      # 若 type=https 才用：
      url: null                    # "https://fsm/rawupload?vendor=Illumio&model=PCE..."
      batch_size: 100

  - name: "deny-traffic-to-fortisiem"
    enabled: true
    log_type: pd2
    poll_interval_sec: 30
    mapper: { format: syslog_json, flatten: true }
    sink:
      type: tls
      host: "fortisiem.example.com"
      port: 6514

  - name: "pd1-filtered-smb-rdp"
    enabled: false
    log_type: pd1
    poll_interval_sec: 300
    filter:
      expression: "ev.dst_port in (445, 3389)"
    mapper: { format: syslog_json, flatten: true }
    sink:
      type: tls
      host: "fortisiem.example.com"
      port: 6514
```

---

## 7. Core 元件規格

### 7.1 Config (`core/config.py`)

- 用 `pydantic` v2 定義 Schema，啟動時驗證
- 驗證規則：
  - `log_type` ∈ {auditable, pd0, pd1, pd2, pd3}
  - `mapper.format` ∈ {syslog_json, cef, json}
  - `sink.type` ∈ {udp, tcp, tls, https}
  - `https` sink 必填 `url`
  - `cef` format 必填 `mapping_file`
  - `poll_interval_sec` ≥ 10（防呆，避免過密）
  - `pipeline.name` 全域唯一
- 錯誤時輸出**所有**驗證錯誤（不要只報第一個）後 sys.exit(1)

### 7.2 Source (`sources/s3_source.py`)

核心方法：

```python
def iter_new_files(
    self,
    log_type: str,
    checkpoint: Checkpoint,
    max_files_per_tick: int = 1000,
) -> Iterator[tuple[str, datetime, bytes]]:
    """Yield (s3_key, last_modified, decompressed_body) for new files."""
```

演算法：

```
1. base_prefix = f"{fqdn}/org_id={org_id}/{log_type_path}/"
   # log_type_path: auditable | summaries/pd=0 | summaries/pd=1 | ...

2. 計算要掃的日期 prefixes:
   if checkpoint is None or checkpoint.last_modified < now - 48h:
       # 首次啟動或大斷線：從 lookback 日期掃到今日
       start_date = now - initial_lookback_hours
       scan_prefixes = [f"{base_prefix}{d}_" for d in date_range(start_date, today)]
   else:
       # 穩態：只掃 yesterday + today（UTC）
       scan_prefixes = [f"{base_prefix}{yesterday}_", f"{base_prefix}{today}_"]

3. 收集候選物件:
   candidates = []
   for prefix in scan_prefixes:
       for obj in s3.list_objects_v2_paginator(Prefix=prefix):
           if obj.LastModified > checkpoint.last_modified:
               candidates.append(obj)
           elif (obj.LastModified == checkpoint.last_modified
                 and obj.Key > checkpoint.last_key):
               candidates.append(obj)

4. 排序: (LastModified, Key) 升冪
5. 截斷 max_files_per_tick
6. 逐檔 get_object + gunzip，yield
```

**設計理由**：

- 用 `LastModified` 做主排序鍵，解決同日 UUID 非時序問題
- 用 date-prefix 限定 list 範圍，避免每 tick 掃 43k+ 物件
- `max_files_per_tick` 防止首次啟動一次灌入過多把 SIEM 淹掉（預設 1000，可 config）

### 7.3 Flatten (`mappers/_flatten.py`)

```python
def flatten(
    obj: dict,
    separator: str = "_",
    max_depth: int = 10,
    array_strategy: str = "stringify",
) -> dict:
    """
    Recursively flatten nested dict. Keys joined by separator.
    Arrays handled per array_strategy:
      - "stringify": 整個 array 轉 JSON 字串
      - "first": 取第一個元素遞迴 flatten
      - "skip": 丟棄整個欄位
    """
```

單元測試必須涵蓋：

- 純 flat dict（輸入 = 輸出）
- 1 層 nested
- 3 層 nested
- dict 裡含 array
- array 裡含 dict
- 空 dict / 空 array
- None 值保留（不變成 "None" 字串）
- max_depth 到達時 → 剩下的 subtree 視為 array_strategy 處理

### 7.4 Filter (`core/expression_filter.py`)

使用 `simpleeval` 函式庫（這是**安全的運算器**，不是 Python 內建 `eval`；禁用 import、exec、attribute access on built-ins）。預先用 `DotDict` 包裝 event：

```python
from simpleeval import SimpleEval, DEFAULT_FUNCTIONS

class DotDict:
    """Proxy dict that supports ev.a.b.c path access; returns None on miss."""
    def __init__(self, d): self._d = d or {}
    def __getattr__(self, key):
        v = self._d.get(key)
        return DotDict(v) if isinstance(v, dict) else v
    def __bool__(self): return bool(self._d)
    def __eq__(self, other): return self._d == other
    def __contains__(self, key): return key in self._d

def compile_expression(expression: str) -> Callable[[dict], bool]:
    evaluator = SimpleEval(functions={"str": str, "len": len, **DEFAULT_FUNCTIONS})
    def match(event: dict) -> bool:
        evaluator.names = {"ev": DotDict(event)}
        try:
            return bool(evaluator.evaluate(expression))  # simpleeval 安全運算
        except Exception:
            return False
    return match
```

允許運算子：`== != < <= > >= and or not in not in`。

Filter 錯誤（語法錯或欄位不存在）**首次記一次 warning 後就快取沉默**（避免 log 爆量），整個 pipeline 照常跑（event 視為 not match，全部丟棄）。這點在 README 寫清楚：若 filter 表達式錯，pipeline 會丟全部 event，請看 log。

### 7.5 Mappers

#### `mappers/syslog_json.py` (預設)

產出 bytes：

```
<PRI>1 TIMESTAMP HOSTNAME APPNAME PROCID MSGID - STRUCTURED-DATA MSG
```

- `PRI`：facility=16 (local0) × 8 + severity=6 (info) = 134
- `TIMESTAMP`：從 event 取 `timestamp` 欄位（ISO8601），若無則用 now()
- `HOSTNAME`：`pce_fqdn` 欄位
- `APPNAME`：`illumio-pce`
- `PROCID`：`audit` 或 `summary`（依 log_type）
- `MSGID`：log_type（`auditable`、`pd0`、`pd1`、`pd2`、`pd3`）
- `STRUCTURED-DATA`：`-`（不用）
- `MSG`：flatten 後的 JSON `json.dumps(flat_event, separators=(',', ':'))`

範例（單行）：

```
<134>1 2026-04-20T07:00:17.395Z your-pce.illum.io illumio-pce audit auditable - {"href":"/orgs/123456/events/xxx","timestamp":"2026-04-20T07:00:17.395Z","created_by_agent_hostname":"webserver01",...}
```

#### `mappers/cef.py`

載入 `mappings/*.yaml`，格式：

```yaml
# mappings/summaries.yaml
cef_header:
  vendor: "Illumio"
  product: "PCE"
  version: "1.0"
  signature_id_field: "pd"          # 從 event 哪個欄位取 signature ID
  name_template: "Traffic pd={pd} {dir}"
  severity_map:
    "0": 3    # allowed
    "1": 6    # potentially blocked
    "2": 9    # blocked
    "3": 4    # unknown

extensions:
  # CEF key → event 欄位（支援 nested 用 dot）
  src: "src_ip"
  dst: "dst_ip"
  spt: null                         # 無對應
  dpt: "dst_port"
  proto: "proto"
  shost: "src_hostname"
  dhost: "dst_hostname"
  cs1: "pd"
  cs1Label: "PolicyDecision"
  cs2: "pd_qualifier"
  cs2Label: "PDQualifier"
  cs3: "dir"
  cs3Label: "Direction"
  cs4: "pce_fqdn"
  cs4Label: "PCE"
  suser: "un"                       # user
  sproc: "pn"                       # process name
```

CEF 輸出：

```
<134>1 2026-04-20T07:00:17Z your-pce.illum.io illumio-pce summary pd1 - CEF:0|Illumio|PCE|1.0|1|Traffic pd=1 O|6|src=10.1.13.30 dst=10.1.13.34 dpt=17472 proto=6 cs1=1 cs1Label=PolicyDecision ...
```

（CEF 內部 `=` 與 `\` 需 escape）

#### `mappers/passthrough.py`

純 JSON bytes（配合 `sink.type=https`），**不加 syslog header**。

### 7.6 Sinks

#### `sinks/udp_sink.py`

- 每 event 一封包 `sendto()`
- 超過 1024 bytes **截斷並記 WARN**（FortiSIEM 限制）
- 不需連線、不需重試（UDP 無 ACK）

#### `sinks/tcp_sink.py` 與 `sinks/tls_sink.py`

- 維持長連線
- 每 event 一幀，newline-terminated（RFC 6587 non-transparent framing）
- send 失敗 → 關 socket、按 `retry_backoff_sec` 重連重送，最多 `max_retries` 次
- 仍失敗 → 回傳 False，pipeline 層攔截 → checkpoint 不前進
- TLS：用 `ssl.create_default_context()`，可選 `ca_file` 指定自簽 CA
- 單筆超過 8192 bytes 截斷並記 WARN

#### `sinks/https_sink.py`

- 用 `requests.Session` 維持連線
- 累積到 `batch_size` 筆或單次 tick 結束時 POST
- HTTP status 2xx = 成功；4xx/5xx → 按 backoff 重試
- Endpoint 範例：`https://<fsm>/rawupload?vendor=Illumio&model=PCE&reptIp=<ip>&reptName=<host>`
- Body：`NDJSON`（newline-delimited JSON）或 JSON array，由 `mapper.format=json` 決定
- `verify=true` 預設開（自簽憑證 → `ca_file`）

### 7.7 Checkpoint (`core/checkpoint.py`)

檔案路徑：`{checkpoint.dir}/{pipeline.name}.json`

內容：

```json
{
  "pipeline": "audit-to-fortisiem",
  "last_modified": "2026-04-20T07:08:27+00:00",
  "last_key": "your-pce.illum.io/org_id=123456/auditable/20260420_a1ef5ccb-....jsonl.gz",
  "updated_at": "2026-04-20T07:09:15+00:00",
  "processed_files_cumulative": 12345,
  "processed_events_cumulative": 987654
}
```

**Atomic write**：先寫 `{name}.json.tmp`，`os.replace()` 覆蓋。Windows + Linux 都支援 atomic rename。

**讀取時相容**：檔不存在 → 新 pipeline，按 `initial_lookback_hours` 算起點。

### 7.8 Scheduler (`core/scheduler.py`)

- `apscheduler.schedulers.blocking.BlockingScheduler`
- 每條 pipeline 一個 `IntervalTrigger(seconds=poll_interval_sec)`
- Job 選項：
  - `coalesce=True`：若前次還沒跑完就跳過累積的 tick
  - `max_instances=1`：同一 pipeline 不並發
  - `next_run_time=now()`：啟動立刻跑第一次
- 多 pipeline 之間用 `ThreadPoolExecutor(max_workers=N)` 平行（N = pipeline 數）

### 7.9 Pipeline (`core/pipeline.py`)

一次 tick：

```python
def tick(self):
    t0 = time.monotonic()
    cp = self.checkpoint.load()

    stats = {"files": 0, "events_read": 0, "events_filtered": 0,
             "events_sent": 0, "events_failed": 0, "mapper_errors": 0}

    try:
        for key, last_modified, body in self.source.iter_new_files(
                self.log_type, cp, max_files_per_tick=1000):

            stats["files"] += 1
            file_events_all_sent = True

            for line in gunzip_lines(body):
                stats["events_read"] += 1
                try:
                    ev = json.loads(line)
                except Exception:
                    stats["mapper_errors"] += 1
                    self.log.warning(f"bad JSON line in {key}, skipping")
                    continue

                if self.filter and not self.filter(ev):
                    stats["events_filtered"] += 1
                    continue

                try:
                    wire = self.mapper.format(ev)
                except Exception as e:
                    stats["mapper_errors"] += 1
                    self.log.error(f"mapper error on {key}: {e}")
                    continue

                ok = self.sink.send(wire)
                if ok:
                    stats["events_sent"] += 1
                else:
                    stats["events_failed"] += 1
                    file_events_all_sent = False
                    break  # 不繼續此檔，下次重拉整檔

            if not file_events_all_sent:
                self.log.error(f"sink failed on {key}, checkpoint not advancing")
                break  # 不繼續後面的檔

            # 此檔全數成功 → 推 checkpoint
            cp = cp.update(last_modified=last_modified, last_key=key,
                           files_inc=1, events_inc=stats["events_sent"])
            self.checkpoint.save(cp)

    except Exception as e:
        self.log.exception(f"tick aborted: {e}")
    finally:
        self.log.info(
            f"tick: files={stats['files']} "
            f"read={stats['events_read']} sent={stats['events_sent']} "
            f"filtered={stats['events_filtered']} failed={stats['events_failed']} "
            f"mapper_err={stats['mapper_errors']} "
            f"checkpoint={cp.last_key[-40:] if cp.last_key else 'none'} "
            f"duration={time.monotonic()-t0:.2f}s"
        )
```

---

## 8. 錯誤處理與 Retry 語義

| 場景 | 行為 | Checkpoint |
|---|---|---|
| S3 list 失敗（429/timeout） | 整個 tick 中止，下次重試 | 不變 |
| S3 get_object 失敗 | 整個 tick 中止，下次重拉 | 不變 |
| gunzip 失敗 | 記 ERROR，跳該檔繼續下一檔 | **不前進過該檔**（下次重拉） |
| 單行 JSON parse 失敗 | 記 WARN，跳該行繼續 | 正常前進 |
| Mapper 拋例外 | 記 ERROR，跳該行繼續 | 正常前進 |
| Filter 表達式錯 | 首次記 WARN 後沉默，全部丟 | 正常前進 |
| Sink send 失敗（重試後仍失敗） | **停止該檔後續 event 與後續檔案**，tick 結束。該檔先前已 send 成功的 event 會在下次 tick 重送（SIEM 端去重） | **不前進過該檔** |
| Sink 失敗又逢檔案累積 | 下 tick 重拉同批（含先前已送成功的 event），SIEM 端去重 | 不變直到成功 |
| 程式 crash | Checkpoint 最後成功的檔案之前都已持久化 | 重啟自動從 checkpoint 繼續 |

---

## 9. 觀測性與應用日誌

### 9.1 應用 Log 格式

```
YYYY-MM-DD HH:MM:SS LEVEL [pipeline-name] message key=value ...
```

- Logger name = pipeline name（每條獨立）
- 輸出到檔案（rotate）+ stdout（可 config 關閉）

### 9.2 每 tick 必印的 INFO 行

如 §7.9 pipeline.tick 的 finally block。包含：

- `files`：本 tick 處理的檔案數
- `read`：讀到的 event 行數
- `sent`：成功送出的 event 數
- `filtered`：被 filter 濾掉的數
- `failed`：送出失敗的數
- `mapper_err`：mapper 例外數
- `checkpoint`：checkpoint 推到的最後 key（尾 40 字元，避免太長）
- `duration`：tick 執行耗時

### 9.3 啟動 banner

```
[2026-04-20 15:00:00] Illumio S3 → SIEM Collector v1.0
  config: config.yaml (6 pipelines, 4 enabled)
  pipelines enabled:
    - audit-to-fortisiem     → tls fortisiem.example.com:6514 every 60s
    - deny-traffic           → tls fortisiem.example.com:6514 every 30s
    - pd1-filtered           → tls fortisiem.example.com:6514 every 300s
    - audit-json-backup      → https backup.example.com every 300s
  state: ./state (3 existing checkpoints loaded)
  log: ./logs/collector.log (level=INFO, rotate=50MB, keep=7)
```

---

## 10. 部署

### 10.1 相依套件（requirements.txt）

```
boto3>=1.34
botocore>=1.34
pydantic>=2.6
pyyaml>=6.0
apscheduler>=3.10
requests>=2.31
simpleeval>=0.9.13
```

### 10.2 Linux（systemd）

提供範例 `docs/systemd/illumio-collector.service`：

```ini
[Unit]
Description=Illumio S3 to SIEM Collector
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=illumio-collector
WorkingDirectory=/opt/illumio-collector
ExecStart=/opt/illumio-collector/venv/bin/python collector.py --config /etc/illumio-collector/config.yaml
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

### 10.3 Windows（NSSM）

README 附步驟（不內建 pywin32 service，避免 32-bit/venv 雷）：

```
1. 下載 NSSM：https://nssm.cc/download
2. nssm install IllumioCollector
3. Application path: C:\Python311\python.exe
   Arguments:        C:\illumio_s3_collector\collector.py --config C:\illumio_s3_collector\config.yaml
   Startup dir:      C:\illumio_s3_collector
4. Details → Display name: Illumio S3 to SIEM Collector
5. I/O → Output (stdout): C:\illumio_s3_collector\logs\nssm-stdout.log
6. nssm start IllumioCollector
```

### 10.4 手動前景執行（開發 / debug）

```bash
python collector.py --config config.yaml
python collector.py --config config.yaml --dry-run       # 只解析 config 並印出不執行
python collector.py --config config.yaml --once pipe-name # 只跑一條 pipeline 一次
```

### 10.5 離線部署打包（air-gapped 環境，無 Python / 無 pip 的目的主機）

目標：Linux 與 Windows 目的主機**完全沒有 Python、沒有 pip、沒有對外網路**。策略是把**可攜式 Python runtime** 連同所有 wheel 與程式碼打包成一個 tarball / zip，目的主機**解壓即用**，不需要 admin 安裝 Python、不需要改 PATH。

#### 核心技術選擇：python-build-standalone

採用 **[python-build-standalone](https://github.com/astral-sh/python-build-standalone)**（Astral 維護、`uv` 與 `rye` 都用的方案），特性：

- 完全 **relocatable**：解壓到任何路徑、可以隨意移動，Python 自己算 sys.path
- **不需安裝、不需 admin**：Linux 就是個 tarball 解壓；Windows 是 tar.gz（Windows 10+ 內建 `tar.exe`）
- 內建 `pip`、`venv`、完整標準庫
- 官方維護，每月更新，提供 Linux x86_64、Windows x86_64、macOS 多個變體
- 同一份 Python 可在 RHEL 7、Ubuntu 18+、Alpine（需 `x86_64-unknown-linux-musl` 變體）跑

放棄 python.org 官方 installer 的理由：Windows 需要 admin，Linux 需要 distro package。離線加無 admin = 只能靠 portable runtime。

#### 前置需求

| 項目 | 要求 |
|---|---|
| **Build host（有網路）** | Linux bundle 在 Linux build、Windows bundle 在 Windows build（含平台特定 binary wheel） |
| **Build host Python** | 任一 Python 3.11 + pip（僅用來下載 wheel，跟 bundle 裡的 runtime 無關） |
| **Target（無網路）** | **無任何 Python / pip 需求**。需要 x86_64 CPU、glibc ≥ 2.17（Linux）或 Windows 10+ |

#### 打包架構

```
illumio-collector-<platform>-v1.0/
├── app/                          # 程式碼
│   ├── collector.py
│   ├── core/ sources/ mappers/ sinks/ mappings/
│   ├── fortisiem_parser/
│   ├── tests/ doc/
│   ├── requirements.txt
│   ├── config.example.yaml
│   └── README.md
├── python-runtime.tar.gz         # ★ python-build-standalone (~30 MB)
│                                 #    解壓後成為 python/ 目錄
│                                 #    含 python / pip / 完整 stdlib
├── wheels/                       # 所有 .whl 檔（離線 pip 來源）
│   ├── boto3-*.whl
│   ├── pydantic-*.whl
│   ├── pydantic_core-*-manylinux_2_17_x86_64.whl   # Linux
│   │                      或 *-win_amd64.whl        # Windows
│   ├── PyYAML-*.whl APScheduler-*.whl
│   ├── requests-*.whl simpleeval-*.whl
│   └── ... (全部遞迴相依)
├── install.sh / install.ps1       # 目的主機一鍵安裝腳本
├── nssm-2.24.zip                  # 僅 Windows bundle
├── systemd/illumio-collector.service  # 僅 Linux bundle
└── VERSION                        # 版本號 + build 時間戳
```

**Bundle 大小預估**：
- Linux bundle：~85 MB（Python 30 + wheels 50 + 程式碼 + NSSM/systemd 檔）
- Windows bundle：~80 MB（Python 25 + wheels 50 + NSSM 0.4 MB + 程式碼）

#### Build 步驟 - Linux

在**有網路的 Linux x86_64 主機**執行（任何裝 Python 3.11 + pip 的即可，不限 distro）：

```bash
set -euo pipefail

VERSION="1.0"
PBS_TAG="20240415"                    # python-build-standalone 發布日，依可用最新版
PY_VER="3.11.9"

cd /tmp
cp -r /path/to/illumio_s3_collector bundle-src
cd bundle-src
mkdir -p ../bundle/app ../bundle/wheels ../bundle/systemd

# 1. 下載可攜式 Python runtime
curl -L -o ../bundle/python-runtime.tar.gz \
  "https://github.com/astral-sh/python-build-standalone/releases/download/${PBS_TAG}/cpython-${PY_VER}+${PBS_TAG}-x86_64-unknown-linux-gnu-install_only.tar.gz"

# 2. 下載所有 wheel（指定 Linux x86_64 + Python 3.11）
python3.11 -m pip download \
  --only-binary=:all: \
  --platform manylinux2014_x86_64 \
  --python-version 3.11 --implementation cp --abi cp311 \
  -d ../bundle/wheels \
  -r requirements.txt

# 3. 複製程式碼
cp -r collector.py core sources mappers sinks mappings fortisiem_parser \
      tests doc requirements.txt config.example.yaml README.md \
      ../bundle/app/

# 4. 複製 systemd unit 與 install 腳本
cp docs/systemd/illumio-collector.service ../bundle/systemd/
cp scripts/install.sh ../bundle/
chmod +x ../bundle/install.sh

# 5. 標記版本
cat > ../bundle/VERSION <<EOF
illumio-s3-siem-collector v${VERSION}
built: $(date -u +%Y-%m-%dT%H:%M:%SZ)
host:  $(uname -a)
python: cpython-${PY_VER}+${PBS_TAG} (x86_64 linux gnu)
EOF

# 6. 打包
cd ..
tar czf illumio-collector-linux-x86_64-v${VERSION}.tar.gz bundle/
sha256sum illumio-collector-linux-x86_64-v${VERSION}.tar.gz > SHA256SUMS
echo "Bundle ready: illumio-collector-linux-x86_64-v${VERSION}.tar.gz"
```

#### Install 步驟 - Linux（目的主機，無網路，無 Python）

```bash
set -euo pipefail

# 1. 用 root 安裝到 /opt（或任何自定路徑）
sudo mkdir -p /opt
sudo tar xzf illumio-collector-linux-x86_64-v1.0.tar.gz -C /opt/
sudo mv /opt/bundle /opt/illumio-collector
cd /opt/illumio-collector

# 2. 解壓 Python runtime（解壓後得到 python/ 目錄，內含 bin/python3、bin/pip）
sudo tar xzf python-runtime.tar.gz
# 驗證：
./python/bin/python3 --version    # => Python 3.11.9

# 3. 用 bundled Python 安裝 wheel（全離線）
sudo ./python/bin/python3 -m pip install \
    --no-index \
    --find-links=/opt/illumio-collector/wheels \
    -r /opt/illumio-collector/app/requirements.txt

# 4. 準備 config
sudo mkdir -p /etc/illumio-collector
sudo cp /opt/illumio-collector/app/config.example.yaml /etc/illumio-collector/config.yaml
sudo chmod 600 /etc/illumio-collector/config.yaml
sudo vi /etc/illumio-collector/config.yaml   # 填 AWS key / FortiSIEM 位址

# 5. 建立服務帳號與目錄
sudo useradd -r -s /sbin/nologin illumio-collector || true
sudo mkdir -p /var/lib/illumio-collector/state /var/log/illumio-collector
sudo chown -R illumio-collector:illumio-collector \
    /var/lib/illumio-collector /var/log/illumio-collector \
    /opt/illumio-collector

# 6. 安裝 systemd unit（ExecStart 已指到 /opt/illumio-collector/python/bin/python3）
sudo cp /opt/illumio-collector/systemd/illumio-collector.service \
    /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now illumio-collector

# 7. 驗證
sudo systemctl status illumio-collector
sudo journalctl -u illumio-collector -f
```

更新 systemd unit：

```ini
[Unit]
Description=Illumio S3 to SIEM Collector
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=illumio-collector
WorkingDirectory=/opt/illumio-collector/app
ExecStart=/opt/illumio-collector/python/bin/python3 /opt/illumio-collector/app/collector.py --config /etc/illumio-collector/config.yaml
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

#### Build 步驟 - Windows

在**有網路的 Windows 10/11 x86_64 主機**，已裝 Python 3.11 的 PowerShell：

```powershell
$ErrorActionPreference = "Stop"
$VERSION = "1.0"
$PBS_TAG = "20240415"
$PY_VER  = "3.11.9"

Set-Location C:\build
Copy-Item -Recurse C:\path\to\illumio_s3_collector bundle-src
Set-Location bundle-src
New-Item -ItemType Directory -Force -Path ..\bundle\app, ..\bundle\wheels | Out-Null

# 1. 下載可攜式 Python runtime（Windows 版）
Invoke-WebRequest `
  -Uri "https://github.com/astral-sh/python-build-standalone/releases/download/${PBS_TAG}/cpython-${PY_VER}+${PBS_TAG}-x86_64-pc-windows-msvc-install_only.tar.gz" `
  -OutFile ..\bundle\python-runtime.tar.gz

# 2. 下載所有 Windows wheel
python -m pip download `
  --only-binary=:all: `
  --platform win_amd64 `
  --python-version 3.11 --implementation cp --abi cp311 `
  -d ..\bundle\wheels `
  -r requirements.txt

# 3. 複製程式碼
Copy-Item collector.py, requirements.txt, config.example.yaml, README.md ..\bundle\app\
Copy-Item -Recurse core, sources, mappers, sinks, mappings, fortisiem_parser, tests, doc ..\bundle\app\

# 4. 下載 NSSM（Service 包裝器）
Invoke-WebRequest `
  -Uri "https://nssm.cc/release/nssm-2.24.zip" `
  -OutFile ..\bundle\nssm-2.24.zip

# 5. 複製 install 腳本
Copy-Item scripts\install.ps1 ..\bundle\

# 6. 標記版本
@"
illumio-s3-siem-collector v$VERSION
built: $(Get-Date -Format 'yyyy-MM-ddTHH:mm:ssZ')
host:  $(hostname)
python: cpython-$PY_VER+$PBS_TAG (x86_64 windows msvc)
"@ | Out-File ..\bundle\VERSION -Encoding UTF8

# 7. 打包
Set-Location ..
Compress-Archive -Path bundle\* -DestinationPath illumio-collector-windows-x86_64-v${VERSION}.zip -Force
Get-FileHash illumio-collector-windows-x86_64-v${VERSION}.zip -Algorithm SHA256 | Out-File SHA256SUMS.txt
Write-Host "Bundle ready: illumio-collector-windows-x86_64-v${VERSION}.zip"
```

#### Install 步驟 - Windows（目的主機，無網路，無 Python）

以**系統管理員**開 PowerShell（NSSM 註冊 service 需要 admin，**不需要**安裝 Python）：

```powershell
$ErrorActionPreference = "Stop"

# 1. 解壓 bundle 到 C:\illumio-collector
Expand-Archive illumio-collector-windows-x86_64-v1.0.zip -DestinationPath C:\ -Force
Rename-Item C:\bundle C:\illumio-collector
Set-Location C:\illumio-collector

# 2. 解壓可攜式 Python runtime（Windows 10+ 內建 tar.exe）
tar -xzf python-runtime.tar.gz
# 解壓後路徑：C:\illumio-collector\python\python.exe
C:\illumio-collector\python\python.exe --version   # => Python 3.11.9

# 3. 用 bundled Python 安裝 wheel（全離線）
C:\illumio-collector\python\python.exe -m pip install `
    --no-index `
    --find-links=C:\illumio-collector\wheels `
    -r C:\illumio-collector\app\requirements.txt

# 4. 準備 config
Copy-Item C:\illumio-collector\app\config.example.yaml C:\illumio-collector\config.yaml
notepad C:\illumio-collector\config.yaml    # 填 AWS key / FortiSIEM 位址

# 5. 建 state / logs 目錄
New-Item -ItemType Directory -Force `
    C:\illumio-collector\state, C:\illumio-collector\logs | Out-Null

# 6. 解壓 NSSM
Expand-Archive C:\illumio-collector\nssm-2.24.zip -DestinationPath C:\illumio-collector\nssm -Force

# 7. 註冊 Windows Service（指向 bundled Python，不是系統 Python）
$nssm = "C:\illumio-collector\nssm\nssm-2.24\win64\nssm.exe"
& $nssm install IllumioCollector `
    "C:\illumio-collector\python\python.exe" `
    "C:\illumio-collector\app\collector.py --config C:\illumio-collector\config.yaml"
& $nssm set IllumioCollector AppDirectory "C:\illumio-collector\app"
& $nssm set IllumioCollector DisplayName "Illumio S3 to SIEM Collector"
& $nssm set IllumioCollector Description "Pull Illumio PCE logs from S3 and forward to FortiSIEM"
& $nssm set IllumioCollector AppStdout "C:\illumio-collector\logs\nssm-stdout.log"
& $nssm set IllumioCollector AppStderr "C:\illumio-collector\logs\nssm-stderr.log"
& $nssm set IllumioCollector AppRotateFiles 1
& $nssm set IllumioCollector AppRotateBytes 52428800
& $nssm set IllumioCollector Start SERVICE_AUTO_START

# 8. 啟動
& $nssm start IllumioCollector

# 9. 驗證
& $nssm status IllumioCollector
Get-Content C:\illumio-collector\logs\collector.log -Tail 50 -Wait
```

#### 離線部署驗收清單

| 檢查項目 | 驗證方式 |
|---|---|
| 目的主機**無 Python 預裝**也能 install 完成 | `which python3` / `python --version` → 失敗；install 腳本仍成功 |
| 目的主機**無對外網路**也能 install 完成 | 執行前拔網路線 / block outbound |
| Bundled Python 正確 | `/opt/illumio-collector/python/bin/python3 --version` → 3.11.9 |
| 所有套件來自 local wheel | 執行時 `pip install -v` log 顯示 `Looking in links: ./wheels` |
| 服務重開機自動啟動 | Linux: `systemctl is-enabled illumio-collector`；Windows: `sc qc IllumioCollector` 顯示 `AUTO_START` |
| Collector 功能正常 | `collector.log` 出現 `events sent > 0` |
| 解除安裝乾淨 | 一行 `rm -rf /opt/illumio-collector` 或 `Remove-Item C:\illumio-collector`；不污染系統 Python 或註冊表（除 NSSM service） |

#### 升級流程（換版本）

```bash
# Linux
sudo systemctl stop illumio-collector
sudo tar xzf illumio-collector-linux-x86_64-v1.1.tar.gz -C /opt/
sudo mv /opt/illumio-collector /opt/illumio-collector.backup
sudo mv /opt/bundle /opt/illumio-collector
cd /opt/illumio-collector
sudo tar xzf python-runtime.tar.gz
sudo ./python/bin/python3 -m pip install --no-index --find-links=./wheels -r app/requirements.txt
# config 保留在 /etc/illumio-collector/，state 保留在 /var/lib/illumio-collector/
sudo systemctl start illumio-collector
# 驗證 OK 後再 rm -rf /opt/illumio-collector.backup
```

Windows 同理：停 service → 覆蓋解壓 → 重啟 service。

#### 不打算處理的情境

- **非 x86_64 CPU（ARM）**：python-build-standalone 有 aarch64 變體，有需求再加
- **Alpine Linux (musl libc)**：需要換 `x86_64-unknown-linux-musl` 變體的 PBS + musl wheel；v1.0 不做
- **Windows 7 / 8 / Server 2012**：缺內建 `tar`，需額外提供 7-Zip 解壓；v1.0 不做
- **FIPS 模式 Python**：未驗證

---

## 11. 測試策略

### 11.1 單元測試（pytest）

必備：

- `test_flatten.py`：flatten 函式 8+ 個 case（上述 §7.3 列表）
- `test_filter.py`：simpleeval 表達式 + DotDict 存取
- `test_mappers.py`：
  - syslog_json 輸出含正確 RFC5424 header
  - CEF 輸出含正確格式與 escape
  - passthrough 是純 JSON
- `test_checkpoint.py`：atomic write、讀不存在檔、併發安全
- `test_config.py`：pydantic validation 錯誤訊息正確

### 11.2 整合測試

- `test_s3_source_integration.py`：用 `moto` 模擬 S3（不打真正 bucket）
- 用 fixtures 提供真實 Illumio JSON 樣本（去識別化後的 2~3 份 .jsonl.gz）

### 11.3 Smoke test

- 保留 `s3_log_checker.py`：驗 AWS 憑證 / bucket 權限
- `collector.py --dry-run`：驗 config 正確性
- `collector.py --once {pipeline}`：單次跑一條 pipeline，觀察 log

### 11.4 SIEM 接收端驗證

- UDP：用 `nc -u -l 514` 驗內容格式
- TCP/TLS：用 `openssl s_server -accept 6514 -cert test.crt -key test.key`
- HTTPS：用 Flask mock server 印出 POST body
- FortiSIEM：上 Custom Parser XML，查 `Reporting Device` 下 `Illumio PCE` 是否出現事件

---

## 12. 安全考量

- **憑證管理**：`config.yaml` 可能含 AWS access_key + secret_key。建議：
  - 檔案權限設 600（Linux）/ 限制 NTFS 存取（Windows）
  - 或用 AWS CLI profile（`aws configure`）讓金鑰不進 config
  - 或跑在有 IAM role 的 EC2（`profile: null, access_key: null`，boto3 自動 fallback）
- **TLS 憑證驗證**：`sink.tls.verify` 預設 true，**不建議關閉**
- **Filter 表達式沙箱**：`simpleeval` 是設計用來安全運算使用者輸入的表達式，不是 Python 內建 `eval`。禁用 import、exec、攻擊性 attribute access
- **Checkpoint 檔**：只存 S3 key 與 timestamp，不含敏感資料
- **應用 log**：不寫入 event payload（避免 PII leak），只印統計數字

---

## 13. Out of Scope / Future Work

- SQS 事件驅動模式（抽象已預留 `sources/base.py`）
- 多 tenant（多 bucket 來源）
- Disk spool（SIEM 長停機 buffering）
- Prometheus metrics endpoint
- Event-level deduplication（目前依賴 SIEM 端）
- YAML config reload without restart
- Kafka / Splunk HEC / Elasticsearch bulk API sink

---

## 14. 開放問題（待寫 plan 時釐清）

- `max_files_per_tick` 預設值要多少？提案：1000。首次啟動 1000 檔 × 平均 50 events = 50k events，TLS 連線一秒應可打完
- CEF mapping YAML 要不要 v1 就寫？或 v1 只做 syslog_json，CEF mapping 放 v1.1？提案：**v1 只做 syslog_json**，把工作量省下來做 FortiSIEM parser XML
- 首次啟動 lookback 預設 24h 還是 0（從現在開始）？提案：**0**（從現在開始），使用者要 backfill 自己改

---

## 15. 驗收標準

設計完成後寫出的實作，必須通過：

1. `pytest` 全綠（單元 + 整合 + mocked）
2. `python collector.py --config config.example.yaml --dry-run` 不報錯
3. 真實環境對 `illumio-flow-XXXXXXXX-your-bucket` 連線，`audit-to-fortisiem` pipeline 跑 5 分鐘，log 顯示 events sent > 0
4. FortiSIEM 端用 Custom Parser XML 收到 event 後，Analytics 查詢 `Reporting Device = Illumio PCE` 有結果
5. 殺掉 collector process 再啟動，checkpoint 正確讀回不重送、不遺漏
6. Windows + Linux 各至少一台跑過 30 分鐘無 crash
