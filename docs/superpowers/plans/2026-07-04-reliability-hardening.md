# 可靠性強化（Reliability Hardening）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修復 code review 找到的全部 20 項可靠性問題，讓 collector 能以 7/24 log 轉發器的標準長期運轉：不因單一故障永久卡死、不靜默遺失資料、故障可被觀測。

**Architecture:** 不改變既有架構（Source → Pipeline → Mapper → Sink，polling 與 SQS 兩種模式），全部是點狀強化：checkpoint 持久性與惰性初始化、S3 輪詢的競態與掃描間隙、SQS visibility 生命週期與毒訊息防護、sink 的失效偵測、以及啟動期 fail-fast 驗證。每個 task 獨立可測試、可獨立 commit。

**Tech Stack:** Python 3.9+（既有）、pytest + moto（既有測試框架）、boto3 / pydantic v2 / APScheduler / simpleeval（既有依賴，不新增）。

## Global Constraints

- Python 3.9 相容：檔案已有 `from __future__ import annotations`，annotation 可用新語法；**執行期程式碼**不可用 3.10+ 語法（如 match statement）。
- Windows 相容：目錄 fsync 與 TCP socket 選項一律用 `hasattr` 防護（README 明載支援 Windows 服務部署）。
- TDD：每個 task 先寫失敗測試、確認失敗、再實作、確認通過。
- 每個 task 結尾必須跑 `.venv/bin/pytest -q` 全套通過後才 commit。
- Commit message 用英文 conventional commits（`fix:`/`feat:`/`docs:`/`test:`），不加 emoji。
- 不重排、不「順手美化」未涉及的程式碼；diff 只含該 task 需要的行。
- 測試指令一律用 `.venv/bin/pytest`（repo 根目錄執行）。

---

## Phase A — Checkpoint 與啟動可靠性

### Task 1: CheckpointStore.save 加入 fsync

**問題：** `core/checkpoint.py:74-86` 用 mkstemp + `os.replace` 只防 process crash，不防斷電。ext4 delayed allocation 下斷電可能留下空的 checkpoint 檔。

**Files:**
- Modify: `core/checkpoint.py:74-86`
- Test: `tests/test_checkpoint.py`

**Interfaces:**
- Produces: `CheckpointStore.save(cp)` 語意不變（同步、丟例外即失敗），但完成後資料保證落盤。

- [ ] **Step 1: 寫失敗測試**

在 `tests/test_checkpoint.py` 末尾加入：

```python
def test_save_fsyncs_file_before_replace(tmp_state_dir, monkeypatch):
    """Power-loss safety: the temp file must be fsynced before os.replace."""
    import os as _os
    from core.checkpoint import CheckpointStore

    fsynced = []
    real_fsync = _os.fsync

    def spy_fsync(fd):
        fsynced.append(fd)
        return real_fsync(fd)

    monkeypatch.setattr(_os, "fsync", spy_fsync)
    store = CheckpointStore(tmp_state_dir)
    store.save(store.fresh("p1", initial_lookback_hours=24))
    assert len(fsynced) >= 1
```

- [ ] **Step 2: 確認失敗**

Run: `.venv/bin/pytest tests/test_checkpoint.py::test_save_fsyncs_file_before_replace -v`
Expected: FAIL（`assert 0 >= 1`）

- [ ] **Step 3: 實作**

`core/checkpoint.py` 的 `save` 改為：

```python
    def save(self, cp: Checkpoint) -> None:
        final = self._path(cp.pipeline)
        tmp_fd, tmp_path = tempfile.mkstemp(dir=self.dir, prefix=final.name + ".", suffix=".tmp")
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as fp:
                json.dump(cp.to_dict(), fp, indent=2)
                fp.flush()
                os.fsync(fp.fileno())
            os.replace(tmp_path, final)
            # Persist the rename itself; skipped on platforms without O_DIRECTORY (Windows).
            if hasattr(os, "O_DIRECTORY"):
                dir_fd = os.open(self.dir, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
```

- [ ] **Step 4: 確認通過**

Run: `.venv/bin/pytest tests/test_checkpoint.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 全套測試 + commit**

Run: `.venv/bin/pytest -q` → 全綠

```bash
git add core/checkpoint.py tests/test_checkpoint.py
git commit -m "fix(checkpoint): fsync temp file and directory in save for power-loss safety"
```

---

### Task 2: checkpoint 惰性初始化 — 修復啟動 crash loop 與 dry-run 副作用

**問題：** `core/pipeline.py:243` 在 build 期直接 `store.load()`，checkpoint 損毀時丟 `CheckpointError`，`main()` 沒接 → traceback 結束 → systemd `Restart=always` 造成無限重啟迴圈。`tick()` 內明明有復原邏輯卻被啟動路徑繞過。同時 build 期 `store.save(fresh)` 讓 `--dry-run` 對生產 state 目錄產生寫入副作用。

**修法：** build 期完全不碰 checkpoint；`tick()` 在 load 之後若 `last_modified is None`（首次執行）就地初始化 fresh checkpoint。

**Files:**
- Modify: `core/pipeline.py:51-64`（tick）、`core/pipeline.py:236-244`（_build_polling）
- Test: `tests/test_pipeline.py`

**Interfaces:**
- Consumes: `CheckpointStore.load/save/fresh`（Task 1 語意）。
- Produces: `Pipeline.tick()` 首次執行會自行建立 fresh checkpoint（lookback = `recovery_lookback_hours`）；`build_pipelines_from_config` 保證不讀寫任何 checkpoint 檔。

- [ ] **Step 1: 寫失敗測試**

在 `tests/test_pipeline.py` 中（`test_tick_recovers_from_corrupt_checkpoint` 之後）加入。檔案頂部 `from datetime import datetime` 改為 `from datetime import datetime, timezone`：

```python
def test_tick_initializes_missing_checkpoint_with_lookback(tmp_state_dir):
    """First tick with no checkpoint file must create one honoring the lookback."""
    store = CheckpointStore(tmp_state_dir)
    p = Pipeline(
        name="p1", log_type="auditable",
        source=FakeSource([]), mapper=FakeMapper(), sink=FakeSink(),
        checkpoint_store=store, filter_fn=None,
        max_files_per_tick=100, recovery_lookback_hours=12,
    )
    p.tick()
    cp = store.load("p1")
    assert cp.last_modified is not None
    age_h = (datetime.now(timezone.utc) - cp.last_modified).total_seconds() / 3600
    assert 11.9 < age_h < 12.1
```

在檔案末尾（factory 測試區）加入：

```python
def _factory_cfg_text(tmp_path):
    return f"""
aws: {{ region: us-east-1, access_key: x, secret_key: y }}
source: {{ type: s3, bucket: b, fqdn: pce.example.com, org_id: '1' }}
checkpoint: {{ dir: {tmp_path}/state, initial_lookback_hours: 24 }}
logging: {{ dir: {tmp_path}/logs, level: INFO }}
pipelines:
  - name: a
    enabled: true
    log_type: auditable
    poll_interval_sec: 60
    mapper: {{ format: json }}
    sink: {{ type: file, path: {tmp_path}/x.log }}
"""


def test_factory_survives_corrupt_checkpoint(tmp_path, monkeypatch):
    """Startup must not crash-loop on a corrupt checkpoint file."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "y")
    state = tmp_path / "state"
    state.mkdir()
    (state / "a.json").write_text("{not valid json", encoding="utf-8")
    p = tmp_path / "cfg.yaml"
    p.write_text(_factory_cfg_text(tmp_path))
    from core.config import load_config
    from core.pipeline import build_pipelines_from_config
    result = build_pipelines_from_config(load_config(str(p)))  # must not raise
    assert result.mode == "polling"


def test_factory_does_not_touch_checkpoint_files(tmp_path, monkeypatch):
    """build must be side-effect free w.r.t. checkpoints (dry-run safety)."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "y")
    p = tmp_path / "cfg.yaml"
    p.write_text(_factory_cfg_text(tmp_path))
    from core.config import load_config
    from core.pipeline import build_pipelines_from_config
    build_pipelines_from_config(load_config(str(p)))
    assert list((tmp_path / "state").glob("*.json")) == []
```

- [ ] **Step 2: 確認失敗**

Run: `.venv/bin/pytest tests/test_pipeline.py -v -k "initializes_missing or survives_corrupt or does_not_touch"`
Expected: 3 個測試 FAIL（initializes: fresh checkpoint 是 build 建立不是 tick；survives: 丟 CheckpointError；does_not_touch: state 目錄有 a.json）

- [ ] **Step 3: 實作**

`core/pipeline.py` `tick()` 開頭（load / recovery 之後）加入初始化：

```python
        try:
            cp = self.checkpoint_store.load(self.name)
        except CheckpointError as e:
            cp_path = self.checkpoint_store._path(self.name)
            self.log.error(
                "checkpoint corrupted at %s (%s); resetting to fresh "
                "checkpoint with lookback=%dh",
                cp_path, e, self.recovery_lookback_hours,
            )
            cp = self.checkpoint_store.fresh(
                self.name, initial_lookback_hours=self.recovery_lookback_hours)
            self.checkpoint_store.save(cp)
        if cp.last_modified is None:
            cp = self.checkpoint_store.fresh(
                self.name, initial_lookback_hours=self.recovery_lookback_hours)
            self.checkpoint_store.save(cp)
            self.log.info("initialized fresh checkpoint with lookback=%dh",
                          self.recovery_lookback_hours)
```

`_build_polling` 移除這兩行（`core/pipeline.py:243-244`）：

```python
        if store.load(pc.name).last_modified is None:
            store.save(store.fresh(pc.name, lookback))
```

- [ ] **Step 4: 確認通過**

Run: `.venv/bin/pytest tests/test_pipeline.py -v`
Expected: 全部 PASS（既有測試不受影響：它們都經由 tick 建立 checkpoint）

- [ ] **Step 5: 全套測試 + commit**

Run: `.venv/bin/pytest -q` → 全綠

```bash
git add core/pipeline.py tests/test_pipeline.py
git commit -m "fix(pipeline): lazy checkpoint init in tick; build no longer reads/writes checkpoints (fixes startup crash-loop on corrupt checkpoint)"
```

---

### Task 3: polling 模式的 SIGTERM 優雅關閉

**問題：** `collector.py:114-121` 只在 SQS 分支註冊 SIGTERM。polling 模式下 `systemctl stop` 直接殺死 process，`PipelineScheduler.run_forever` 的 finally（flush/close sinks）不會執行，FileSink 緩衝區內已 checkpoint 的事件永久遺失。

**修法：** 新增 `_install_sigterm_exit()`，把 SIGTERM 轉成 `SystemExit`，讓 `BlockingScheduler.start()` 丟出、由 `run_forever` 的 `except (KeyboardInterrupt, SystemExit)` 走既有 finally 清理路徑。

**Files:**
- Modify: `collector.py`
- Test: `tests/test_collector.py`（新檔）

**Interfaces:**
- Produces: `collector._install_sigterm_exit() -> None`，polling 分支在 `scheduler.run_forever()` 前呼叫。

- [ ] **Step 1: 寫失敗測試**

建立 `tests/test_collector.py`：

```python
"""collector.py entry-point behaviors."""
import signal

import pytest


def test_sigterm_handler_raises_systemexit():
    from collector import _install_sigterm_exit
    old = signal.getsignal(signal.SIGTERM)
    try:
        _install_sigterm_exit()
        handler = signal.getsignal(signal.SIGTERM)
        assert callable(handler)
        with pytest.raises(SystemExit):
            handler(signal.SIGTERM, None)
    finally:
        signal.signal(signal.SIGTERM, old)
```

- [ ] **Step 2: 確認失敗**

Run: `.venv/bin/pytest tests/test_collector.py -v`
Expected: FAIL（ImportError: cannot import name `_install_sigterm_exit`）

- [ ] **Step 3: 實作**

`collector.py`：頂部 import 區加 `import signal`（並刪除 sqs 分支內的 `import signal` 那一行）。在 `_sink_desc` 之後加：

```python
def _install_sigterm_exit() -> None:
    """Polling mode: make SIGTERM behave like SIGINT so BlockingScheduler
    unwinds through run_forever's finally block (sinks get flushed/closed)."""
    def _raise_exit(signum, frame):
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, _raise_exit)
```

polling 分支 `scheduler = PipelineScheduler(pipelines)` 之前加一行：

```python
        _install_sigterm_exit()
        scheduler = PipelineScheduler(pipelines)
        scheduler.run_forever()
        return 0
```

- [ ] **Step 4: 確認通過**

Run: `.venv/bin/pytest tests/test_collector.py -v`
Expected: PASS

- [ ] **Step 5: 全套測試 + commit**

Run: `.venv/bin/pytest -q` → 全綠

```bash
git add collector.py tests/test_collector.py
git commit -m "fix(collector): graceful SIGTERM shutdown in polling mode so sinks flush on systemctl stop"
```

---

## Phase B — S3 輪詢來源

### Task 4: gunzip 失敗改為「重試 N 次才跳過」，不再靜默永久跳檔

**問題：** `sources/s3_source.py:120-124` 解壓失敗只 log 後 `continue`，後續檔案照常 yield，checkpoint 越過壞檔 → 暫時性下載截斷造成的失敗變成永久資料遺失。

**修法：** `S3Source` 記錄每個 key 的失敗次數；未達上限就丟 `SourceError` 中止本次 tick（下次 tick 重新下載重試），達上限（預設 3 次）才記 ERROR 並跳過。

**Files:**
- Modify: `sources/s3_source.py`
- Test: `tests/test_s3_source.py`

**Interfaces:**
- Produces: `S3Source(..., max_gunzip_retries: int = 3)`；行為：同一 key 前 `max_gunzip_retries - 1` 次解壓失敗 → raise `SourceError`；第 `max_gunzip_retries` 次 → log ERROR + skip。

- [ ] **Step 1: 寫失敗測試**

在 `tests/test_s3_source.py` 加入（檔案已 import `gzip`, `datetime`, `timezone`, `Checkpoint`, `S3Source`；補 `import pytest` 已存在）：

```python
def test_gunzip_failure_retries_then_skips(s3_env):
    """A corrupt object aborts the tick (retry) until max attempts, then is skipped."""
    from core.exceptions import SourceError
    base = f"{FQDN}/org_id={ORG}/auditable/"
    _put(s3_env, base + "20260420_aaa.jsonl.gz", b"this is not gzip")
    _put(s3_env, base + "20260420_bbb.jsonl.gz", _gz('{"x":1}'))

    cp = Checkpoint(pipeline="p",
                    last_modified=datetime(2020, 1, 1, tzinfo=timezone.utc))
    src = S3Source(bucket=BUCKET, fqdn=FQDN, org_id=ORG, s3_client=s3_env,
                   today=datetime(2026, 4, 20, 23, 59, 59, tzinfo=timezone.utc),
                   max_gunzip_retries=3)

    # Attempts 1 and 2: abort the tick so the file is retried next tick.
    for _ in range(2):
        with pytest.raises(SourceError):
            list(src.iter_new_files("auditable", cp))

    # Attempt 3: give up on the corrupt file, later files flow again.
    results = list(src.iter_new_files("auditable", cp))
    assert [r[0] for r in results] == [base + "20260420_bbb.jsonl.gz"]
```

- [ ] **Step 2: 確認失敗**

Run: `.venv/bin/pytest tests/test_s3_source.py::test_gunzip_failure_retries_then_skips -v`
Expected: FAIL（現行程式第一次呼叫就 skip 壞檔、yield bbb，`pytest.raises(SourceError)` 不成立）

- [ ] **Step 3: 實作**

`sources/s3_source.py`：`__init__` 加參數與狀態：

```python
    def __init__(
        self,
        bucket: str,
        fqdn: str,
        org_id: str,
        s3_client=None,
        today: Optional[datetime] = None,
        max_gunzip_retries: int = 3,
    ):
        self.bucket = bucket
        self.fqdn = fqdn
        self.org_id = org_id
        self.s3 = s3_client or boto3.client("s3")
        self._today_override = today
        self.max_gunzip_retries = max_gunzip_retries
        self._gunzip_failures: dict[str, int] = {}
```

`iter_new_files` 的下載段改為：

```python
        for lm, key in candidates:
            try:
                resp = self.s3.get_object(Bucket=self.bucket, Key=key)
                raw = resp["Body"].read()
            except ClientError as e:
                raise SourceError(f"S3 get_object failed for {key}: {e}") from e
            try:
                body = gzip.decompress(raw)
            except OSError as e:
                n = self._gunzip_failures.get(key, 0) + 1
                if n < self.max_gunzip_retries:
                    self._gunzip_failures[key] = n
                    raise SourceError(
                        f"gunzip failed for {key} "
                        f"(attempt {n}/{self.max_gunzip_retries}): {e}; "
                        "aborting tick to retry") from e
                self._gunzip_failures.pop(key, None)
                log.error("gunzip failed for %s after %d attempts; "
                          "permanently skipping: %s", key, n, e)
                continue
            self._gunzip_failures.pop(key, None)
            yield key, lm, body
```

- [ ] **Step 4: 確認通過**

Run: `.venv/bin/pytest tests/test_s3_source.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 全套測試 + commit**

Run: `.venv/bin/pytest -q` → 全綠

```bash
git add sources/s3_source.py tests/test_s3_source.py
git commit -m "fix(s3): retry corrupt-gzip downloads before skipping instead of silently losing the file"
```

---

### Task 5: S3 安全延遲（safety lag），關閉同秒競態漏檔窗口

**問題：** S3 LastModified 只有秒級精度，list 進行中完成上傳的物件可能沒被列到；下一輪其 `(LastModified, Key)` 已不大於 checkpoint → 永遠漏掉。

**修法：** 只處理 `LastModified <= now - safety_lag_sec` 的物件。`S3Source` 建構子預設 `0`（不影響既有測試），config `source.type=s3` 新增 `safety_lag_sec` 預設 `60`，由 factory 接線。

**Files:**
- Modify: `sources/s3_source.py`、`core/config.py`（S3SourceConfig）、`core/pipeline.py`（_build_polling）
- Test: `tests/test_s3_source.py`、`tests/test_config.py`

**Interfaces:**
- Produces: `S3Source(..., safety_lag_sec: int = 0)`；`S3SourceConfig.safety_lag_sec: int = 60`。

- [ ] **Step 1: 寫失敗測試**

`tests/test_s3_source.py` 加入：

```python
def test_safety_lag_excludes_recent_objects(s3_env):
    """Objects newer than now - safety_lag are deferred to a later tick,
    closing the same-second listing race."""
    now = datetime.now(timezone.utc)
    base = f"{FQDN}/org_id={ORG}/auditable/"
    _put(s3_env, base + f"{now:%Y%m%d}_recent.jsonl.gz", _gz('{"x":1}'))
    cp = Checkpoint(pipeline="p",
                    last_modified=datetime(2020, 1, 1, tzinfo=timezone.utc))

    lagged = S3Source(bucket=BUCKET, fqdn=FQDN, org_id=ORG, s3_client=s3_env,
                      safety_lag_sec=300)
    assert list(lagged.iter_new_files("auditable", cp)) == []

    no_lag = S3Source(bucket=BUCKET, fqdn=FQDN, org_id=ORG, s3_client=s3_env,
                      safety_lag_sec=0)
    assert len(list(no_lag.iter_new_files("auditable", cp))) == 1
```

`tests/test_config.py` 加入：

```python
def test_s3_source_safety_lag_default():
    from core.config import S3SourceConfig
    cfg = S3SourceConfig(bucket="b", fqdn="f", org_id="1")
    assert cfg.safety_lag_sec == 60
```

- [ ] **Step 2: 確認失敗**

Run: `.venv/bin/pytest tests/test_s3_source.py::test_safety_lag_excludes_recent_objects tests/test_config.py::test_s3_source_safety_lag_default -v`
Expected: 兩個都 FAIL（TypeError: unexpected keyword / AttributeError）

- [ ] **Step 3: 實作**

`sources/s3_source.py` `__init__` 參數列（接續 Task 4 的簽名）加 `safety_lag_sec: int = 0`、`self.safety_lag_sec = safety_lag_sec`。`iter_new_files` 掃描段改為：

```python
        base = self._base_prefix(log_type)
        scan_prefixes = self._scan_date_prefixes(base, checkpoint)
        log.debug("scan prefixes for %s: %s", log_type, scan_prefixes)

        cutoff = (self._today() - timedelta(seconds=self.safety_lag_sec)
                  if self.safety_lag_sec else None)
        candidates: list[tuple[datetime, str]] = []
        for prefix in scan_prefixes:
            try:
                paginator = self.s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                    for obj in page.get("Contents", []) or []:
                        if cutoff is not None and obj["LastModified"] > cutoff:
                            continue
                        if self._is_new(obj, checkpoint):
```

（其餘 insort 邏輯不動。）

`core/config.py` `S3SourceConfig` 加欄位：

```python
class S3SourceConfig(BaseModel):
    type: Literal["s3"] = "s3"
    bucket: str
    fqdn: str
    org_id: str
    safety_lag_sec: int = Field(
        default=60, ge=0,
        description="Only process objects older than now - lag; closes the "
                    "same-second S3 listing race. 0 disables.",
    )
```

`core/pipeline.py` `_build_polling` 的 `S3Source(...)` 呼叫加 `safety_lag_sec=cfg.source.safety_lag_sec,`。

- [ ] **Step 4: 確認通過**

Run: `.venv/bin/pytest tests/test_s3_source.py tests/test_config.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 全套測試 + commit**

Run: `.venv/bin/pytest -q` → 全綠

```bash
git add sources/s3_source.py core/config.py core/pipeline.py tests/test_s3_source.py tests/test_config.py
git commit -m "fix(s3): add safety lag so objects landing during a listing are never skipped"
```

---

### Task 6: 日期前綴掃描補洞 — 一律從 checkpoint 的日曆日掃到今天

**問題：** `sources/s3_source.py:73-83` 只有 `(today - lm) > 48h` 才 fan-out，否則固定掃「今天＋昨天」。停機 24–48 小時後重啟時（例：現在週三 02:00、checkpoint 停在週一 20:00），週一前綴底下、週一 20:00 之後上傳的檔案永遠掃不到。

**修法：** 統一邏輯：`start_date = min(lm.date(), 昨天)`，從 start_date 掃到今天；`lm is None` 沿用 24 小時 lookback。

**Files:**
- Modify: `sources/s3_source.py:73-83`
- Test: `tests/test_s3_source.py`

**Interfaces:**
- Produces: `_scan_date_prefixes` 回傳值語意不變（`{base}{YYYYMMDD}_` 前綴列表，遞增）。

- [ ] **Step 1: 寫失敗測試**

`tests/test_s3_source.py` 加入：

```python
def _dates_of(prefixes):
    return [p.rsplit("/", 1)[-1] for p in prefixes]


def test_scan_prefixes_cover_checkpoint_date_after_outage(s3_env):
    """A 30h outage spanning 3 calendar days must rescan the checkpoint's date."""
    src = S3Source(bucket=BUCKET, fqdn=FQDN, org_id=ORG, s3_client=s3_env,
                   today=datetime(2026, 4, 22, 2, 0, tzinfo=timezone.utc))
    cp = Checkpoint(pipeline="p",
                    last_modified=datetime(2026, 4, 20, 20, 0, tzinfo=timezone.utc))
    prefixes = src._scan_date_prefixes(f"{FQDN}/org_id={ORG}/auditable/", cp)
    assert _dates_of(prefixes) == ["20260420_", "20260421_", "20260422_"]


def test_scan_prefixes_steady_state_is_yesterday_and_today(s3_env):
    src = S3Source(bucket=BUCKET, fqdn=FQDN, org_id=ORG, s3_client=s3_env,
                   today=datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc))
    cp = Checkpoint(pipeline="p",
                    last_modified=datetime(2026, 4, 22, 11, 55, tzinfo=timezone.utc))
    prefixes = src._scan_date_prefixes(f"{FQDN}/org_id={ORG}/auditable/", cp)
    assert _dates_of(prefixes) == ["20260421_", "20260422_"]


def test_scan_prefixes_no_checkpoint_covers_last_24h(s3_env):
    src = S3Source(bucket=BUCKET, fqdn=FQDN, org_id=ORG, s3_client=s3_env,
                   today=datetime(2026, 4, 22, 2, 0, tzinfo=timezone.utc))
    cp = Checkpoint(pipeline="p", last_modified=None)
    prefixes = src._scan_date_prefixes(f"{FQDN}/org_id={ORG}/auditable/", cp)
    assert _dates_of(prefixes) == ["20260421_", "20260422_"]
```

- [ ] **Step 2: 確認失敗**

Run: `.venv/bin/pytest tests/test_s3_source.py -v -k scan_prefixes`
Expected: `cover_checkpoint_date_after_outage` FAIL（現行只回 21、22 兩天）；其餘兩個 PASS（行為守恆確認）

- [ ] **Step 3: 實作**

`_scan_date_prefixes` 整段改為：

```python
    def _scan_date_prefixes(self, base: str, checkpoint: Checkpoint) -> List[str]:
        today = self._today()
        lm = checkpoint.last_modified
        if lm is None:
            start_date = (today - timedelta(hours=24)).date()
        else:
            # Late uploads land in old date prefixes: always rescan from the
            # checkpoint's calendar date, and never scan fewer than
            # {yesterday, today}.
            start_date = min(lm.date(), (today - timedelta(days=1)).date())
        days = (today.date() - start_date).days
        return [f"{base}{(start_date + timedelta(days=i)).strftime('%Y%m%d')}_"
                for i in range(days + 1)]
```

- [ ] **Step 4: 確認通過**

Run: `.venv/bin/pytest tests/test_s3_source.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 全套測試 + commit**

Run: `.venv/bin/pytest -q` → 全綠

```bash
git add sources/s3_source.py tests/test_s3_source.py
git commit -m "fix(s3): scan date prefixes from checkpoint's calendar date, closing the 24-48h outage gap"
```

---

## Phase C — SQS 模式

### Task 7: visibility 計時基準改為批次 receive 時間

**問題：** `sources/sqs_s3_source.py:123` 在 `_handle_message` 開始才設 `last_extend_at = time.monotonic()`，但 visibility 時鐘從 `receive_message` 就開始跑。一批 10 則序列處理時，後段訊息開始處理時 window 可能已過半甚至過期 → 重複投遞、舊 receipt handle 失效、delete 失敗。

**Files:**
- Modify: `sources/sqs_s3_source.py`（consume_one_batch、_handle_message）
- Test: `tests/test_sqs_s3_dispatcher.py`

**Interfaces:**
- Produces: `_handle_message(msg, received_at: Optional[float] = None)`；`consume_one_batch` 在 receive 之後立刻記錄 `received_at` 並傳入每則訊息。

- [ ] **Step 1: 寫失敗測試**

`tests/test_sqs_s3_dispatcher.py` 加入：

```python
@mock_aws
def test_handle_message_extends_based_on_receive_time(aws_env):
    """Visibility clock starts at receive, not at handle start: a message
    picked up late in a batch must be extended immediately."""
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    key = "pce.example.com/org_id=1/auditable/x.json.gz"
    _put_object(s3, bucket, key, [{"a": 1}])
    _enqueue_event(sqs, qurl, bucket, key)

    msgs = sqs.receive_message(QueueUrl=qurl, WaitTimeSeconds=0,
                               VisibilityTimeout=60)["Messages"]

    real_change = sqs.change_message_visibility
    calls = []
    def spy_change(**kwargs):
        calls.append(kwargs)
        return real_change(**kwargs)
    sqs.change_message_visibility = spy_change

    sink = CapturingSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)
    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="test-bucket", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10,
                       visibility_timeout_sec=60,
                       visibility_extension_sec=120)
    # Simulate: the batch was received 40s ago (>= 60/2), processing starts now.
    d._handle_message(msgs[0], received_at=time.monotonic() - 40)

    assert len(calls) >= 1
    assert calls[0]["VisibilityTimeout"] == 120
```

- [ ] **Step 2: 確認失敗**

Run: `.venv/bin/pytest tests/test_sqs_s3_dispatcher.py::test_handle_message_extends_based_on_receive_time -v`
Expected: FAIL（TypeError: unexpected keyword `received_at`）

- [ ] **Step 3: 實作**

`sources/sqs_s3_source.py`。`typing` import 加 `Optional`。`consume_one_batch`：

```python
    def consume_one_batch(self) -> int:
        """Run one receive_message cycle and process its messages. Returns count."""
        resp = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=self.max_messages_per_receive,
            WaitTimeSeconds=self.wait_time_sec,
            VisibilityTimeout=self.visibility_timeout_sec,
        )
        received_at = time.monotonic()
        messages = resp.get("Messages", []) or []
        for msg in messages:
            self._handle_message(msg, received_at=received_at)
        return len(messages)
```

`_handle_message` 簽名與計時起點：

```python
    def _handle_message(self, msg: dict, received_at: Optional[float] = None) -> None:
        body = msg.get("Body", "")
        receipt = msg["ReceiptHandle"]
        # The visibility clock started when the batch was received, not when
        # this message's turn came up in the serial loop.
        last_extend_at = received_at if received_at is not None else time.monotonic()
```

（其餘 `extend_if_needed` 邏輯不動。）

- [ ] **Step 4: 確認通過**

Run: `.venv/bin/pytest tests/test_sqs_s3_dispatcher.py -v`
Expected: 全部 PASS（既有 slow-processing 測試行為不變）

- [ ] **Step 5: 全套測試 + commit**

Run: `.venv/bin/pytest -q` → 全綠

```bash
git add sources/sqs_s3_source.py tests/test_sqs_s3_dispatcher.py
git commit -m "fix(sqs): base visibility-extension timer on batch receive time, not per-message handle start"
```

---

### Task 8: 事件處理迴圈內定期延長 visibility

**問題：** `sources/sqs_s3_source.py:190` 只在檔案邊界延長 visibility；`pipeline.process()` 處理大檔（含 sink 重試 backoff）可能超過 window → 重複投遞。

**Files:**
- Modify: `core/sqs_pipeline.py`、`sources/sqs_s3_source.py:190`
- Test: `tests/test_sqs_pipeline.py`、`tests/test_sqs_s3_dispatcher.py`

**Interfaces:**
- Produces: `SqsPipeline.process(body: bytes, on_progress: Optional[Callable[[], None]] = None) -> bool`，每處理 200 行呼叫一次 `on_progress`；dispatcher 傳入 `extend_if_needed`。

- [ ] **Step 1: 寫失敗測試**

`tests/test_sqs_pipeline.py` 加入：

```python
def test_process_calls_on_progress_periodically():
    from core.sqs_pipeline import SqsPipeline
    from mappers.passthrough import PassthroughMapper

    class OkSink:
        def send(self, w): return True
        def flush(self): return True
        def close(self): pass

    body = b"\n".join(b'{"a":%d}' % i for i in range(500))
    calls = []
    p = SqsPipeline(name="p", log_type="auditable",
                    mapper=PassthroughMapper(), sink=OkSink(), filter_fn=None)
    assert p.process(body, on_progress=lambda: calls.append(1)) is True
    assert len(calls) == 2  # fired at line 200 and 400
```

`tests/test_sqs_s3_dispatcher.py` 加入：

```python
@mock_aws
def test_dispatcher_passes_on_progress_to_pipeline(aws_env):
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    key = "pce.example.com/org_id=1/auditable/x.json.gz"
    _put_object(s3, bucket, key, [{"a": 1}])
    _enqueue_event(sqs, qurl, bucket, key)

    seen = {}

    class StubPipeline:
        name = "stub"
        log_type = "auditable"
        def process(self, body, on_progress=None):
            seen["on_progress"] = on_progress
            return True

    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="test-bucket", fqdn="pce.example.com", org_id="1",
                       pipelines=[StubPipeline()], wait_time_sec=0,
                       max_messages_per_receive=10)
    d.consume_one_batch()
    assert callable(seen.get("on_progress"))
```

- [ ] **Step 2: 確認失敗**

Run: `.venv/bin/pytest tests/test_sqs_pipeline.py::test_process_calls_on_progress_periodically tests/test_sqs_s3_dispatcher.py::test_dispatcher_passes_on_progress_to_pipeline -v`
Expected: 兩個都 FAIL（TypeError: unexpected keyword `on_progress` / seen 為空）

- [ ] **Step 3: 實作**

`core/sqs_pipeline.py`：module 常數與 `process` 簽名：

```python
_PROGRESS_EVERY = 200
```

```python
    def process(self, body: bytes,
                on_progress: Optional[Callable[[], None]] = None) -> bool:
```

迴圈內 `stats["read"] += 1` 之後加：

```python
                stats["read"] += 1
                if on_progress and stats["read"] % _PROGRESS_EVERY == 0:
                    on_progress()
```

`sources/sqs_s3_source.py` `_handle_object` 最後一行改為：

```python
        extend_visibility()
        return pipeline.process(decoded, on_progress=extend_visibility)
```

- [ ] **Step 4: 確認通過**

Run: `.venv/bin/pytest tests/test_sqs_pipeline.py tests/test_sqs_s3_dispatcher.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 全套測試 + commit**

Run: `.venv/bin/pytest -q` → 全綠

```bash
git add core/sqs_pipeline.py sources/sqs_s3_source.py tests/test_sqs_pipeline.py tests/test_sqs_s3_dispatcher.py
git commit -m "fix(sqs): extend message visibility during long pipeline processing, not only at file boundaries"
```

---

### Task 9: RedrivePolicy 啟動檢查 + 毒訊息上限

**問題：** 毒訊息（格式錯誤、bucket 不符、永久 gunzip 失敗）完全依賴「隊列有設 DLQ」的外部假設。沒設 DLQ 時每個 visibility 週期無限重試、灌 error log。

**修法：** (a) `run_forever` 啟動時查 `RedrivePolicy`，缺少時打 WARNING；(b) receive 帶 `AttributeNames=["ApproximateReceiveCount"]`，超過 `poison_receive_count`（預設 10，0 停用）時記 ERROR（含 body 前 500 字元）並刪除。

**Files:**
- Modify: `sources/sqs_s3_source.py`、`core/config.py`（SqsS3SourceConfig）、`core/pipeline.py`（_build_sqs）
- Test: `tests/test_sqs_s3_dispatcher.py`、`tests/test_config.py`

**Interfaces:**
- Produces: `SqsS3Dispatcher(..., poison_receive_count: int = 10)`；`SqsS3SourceConfig.poison_receive_count: int = 10`；`SqsS3Dispatcher._warn_if_no_dlq() -> None`。

- [ ] **Step 1: 寫失敗測試**

`tests/test_sqs_s3_dispatcher.py` 加入（檔案頂部測試區已有 import；本測試另需 `import logging`，加在 `import gzip` 附近）：

```python
@mock_aws
def test_poison_message_deleted_after_receive_count_threshold(aws_env):
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    sqs.send_message(QueueUrl=qurl, MessageBody="not json — poison")

    sink = CapturingSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)
    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="test-bucket", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10,
                       visibility_timeout_sec=1,
                       poison_receive_count=2)
    d.consume_one_batch()          # receive #1: kept (malformed, below threshold)
    time.sleep(1.5)
    d.consume_one_batch()          # receive #2: hits threshold -> deleted
    time.sleep(1.5)
    msgs = sqs.receive_message(QueueUrl=qurl, WaitTimeSeconds=0,
                               VisibilityTimeout=1).get("Messages", [])
    assert msgs == []


@mock_aws
def test_warns_when_queue_has_no_dlq(aws_env, caplog):
    import logging
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    sink = CapturingSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)
    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="test-bucket", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10)
    with caplog.at_level(logging.WARNING):
        d._warn_if_no_dlq()
    assert any("RedrivePolicy" in r.getMessage() for r in caplog.records)
```

`tests/test_config.py` 加入：

```python
def test_sqs_source_poison_receive_count_default():
    from core.config import SqsS3SourceConfig
    cfg = SqsS3SourceConfig(queue_url="https://q", bucket="b", fqdn="f", org_id="1")
    assert cfg.poison_receive_count == 10
```

- [ ] **Step 2: 確認失敗**

Run: `.venv/bin/pytest tests/test_sqs_s3_dispatcher.py -v -k "poison or no_dlq" && .venv/bin/pytest tests/test_config.py::test_sqs_source_poison_receive_count_default -v`
Expected: 全部 FAIL（unexpected keyword / AttributeError）

- [ ] **Step 3: 實作**

`core/config.py` `SqsS3SourceConfig` 加欄位：

```python
    poison_receive_count: int = Field(
        default=10, ge=0,
        description="Delete a message after this many receives (poison guard "
                    "when the queue has no DLQ). 0 disables.",
    )
```

`sources/sqs_s3_source.py`：`__init__` 參數列加 `poison_receive_count: int = 10`、`self.poison_receive_count = poison_receive_count`。

`consume_one_batch` 的 `receive_message` 加 `AttributeNames=["ApproximateReceiveCount"],`。

`_handle_message` 在 `last_extend_at = ...` 之前加：

```python
        body = msg.get("Body", "")
        receipt = msg["ReceiptHandle"]
        attrs = msg.get("Attributes") or {}
        try:
            receive_count = int(attrs.get("ApproximateReceiveCount", "1"))
        except ValueError:
            receive_count = 1
        if self.poison_receive_count and receive_count >= self.poison_receive_count:
            log.error("poison message: received %d times (threshold %d); "
                      "deleting. body starts with: %.500r",
                      receive_count, self.poison_receive_count, body)
            self._delete(receipt)
            return
```

新增方法並在 `run_forever` 開頭（log.info 之後）呼叫 `self._warn_if_no_dlq()`：

```python
    def _warn_if_no_dlq(self) -> None:
        try:
            resp = self.sqs.get_queue_attributes(
                QueueUrl=self.queue_url, AttributeNames=["RedrivePolicy"])
            if "RedrivePolicy" not in (resp.get("Attributes") or {}):
                log.warning(
                    "SQS queue has no RedrivePolicy (DLQ). Poison messages "
                    "will retry until poison_receive_count=%d, then be "
                    "deleted and logged.", self.poison_receive_count)
        except Exception as e:  # noqa: BLE001
            log.warning("could not inspect queue RedrivePolicy: %s", e)
```

`core/pipeline.py` `_build_sqs` 的 `SqsS3Dispatcher(...)` 呼叫加 `poison_receive_count=cfg.source.poison_receive_count,`。

- [ ] **Step 4: 確認通過**

Run: `.venv/bin/pytest tests/test_sqs_s3_dispatcher.py tests/test_config.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 全套測試 + commit**

Run: `.venv/bin/pytest -q` → 全綠

```bash
git add sources/sqs_s3_source.py core/config.py core/pipeline.py tests/test_sqs_s3_dispatcher.py tests/test_config.py
git commit -m "feat(sqs): poison-message guard via ApproximateReceiveCount and startup DLQ (RedrivePolicy) check"
```

---

### Task 10: SQS dispatcher heartbeat

**問題：** 隊列閒置時 dispatcher 完全不輸出 log，外部監控無法區分「閒置」與「卡死」。systemd 只救 crash，不救 hang。

**Files:**
- Modify: `sources/sqs_s3_source.py`（run_forever、__init__）
- Test: `tests/test_sqs_s3_dispatcher.py`

**Interfaces:**
- Produces: `SqsS3Dispatcher(..., heartbeat_interval_sec: int = 300)`；每 interval 輸出一行 `heartbeat: batches=N messages=N` INFO。

- [ ] **Step 1: 寫失敗測試**

`tests/test_sqs_s3_dispatcher.py` 加入：

```python
@mock_aws
def test_dispatcher_heartbeat_logged(aws_env, caplog):
    import logging
    import threading as _t
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    sink = CapturingSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)
    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="test-bucket", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10,
                       heartbeat_interval_sec=0)
    t = _t.Thread(target=d.run_forever, daemon=True)
    with caplog.at_level(logging.INFO):
        t.start()
        time.sleep(0.3)
        d.request_stop()
        t.join(timeout=5)
    assert any("heartbeat" in r.getMessage() for r in caplog.records)
```

- [ ] **Step 2: 確認失敗**

Run: `.venv/bin/pytest tests/test_sqs_s3_dispatcher.py::test_dispatcher_heartbeat_logged -v`
Expected: FAIL（TypeError: unexpected keyword `heartbeat_interval_sec`）

- [ ] **Step 3: 實作**

`__init__` 參數列加 `heartbeat_interval_sec: int = 300`、`self.heartbeat_interval_sec = heartbeat_interval_sec`。`run_forever` 改為：

```python
    def run_forever(self) -> None:
        log.info("SQS dispatcher starting, queue=...%s", self.queue_url[-32:])
        self._warn_if_no_dlq()
        batches = 0
        messages = 0
        last_heartbeat = time.monotonic()
        while not self._stop_event.is_set():
            try:
                n = self.consume_one_batch()
                batches += 1
                messages += n
            except Exception as e:  # noqa: BLE001
                log.exception("consume batch failed: %s", e)
                self._stop_event.wait(timeout=5)
            if time.monotonic() - last_heartbeat >= self.heartbeat_interval_sec:
                log.info("heartbeat: batches=%d messages=%d", batches, messages)
                last_heartbeat = time.monotonic()
        log.info("SQS dispatcher stopped")
```

- [ ] **Step 4: 確認通過**

Run: `.venv/bin/pytest tests/test_sqs_s3_dispatcher.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 全套測試 + commit**

Run: `.venv/bin/pytest -q` → 全綠

```bash
git add sources/sqs_s3_source.py tests/test_sqs_s3_dispatcher.py
git commit -m "feat(sqs): periodic heartbeat log so external monitoring can distinguish idle from hung"
```

---

### Task 11: SQS 模式 S3 對帳掃描（reconciliation sweep）

**問題：** SNS→SQS 投遞失敗時，該 S3 物件永遠不會被處理且無人知曉（SQS 模式沒有 checkpoint）。

**修法：** dispatcher 在記憶體維護「近期已處理 key」集合；每 `reconcile_interval_sec`（預設 3600，0 停用）列出各啟用 log_type 近 `reconcile_window_sec`（預設 7200）的 S3 物件，凡「本次啟動之後建立、且不在已處理集合」者直接補處理並記 WARNING。只看啟動時間之後的物件，重啟不會造成重複。

**Files:**
- Modify: `sources/s3_source.py`（抽出 `base_prefix` module 函式）、`sources/sqs_s3_source.py`、`core/config.py`（SqsS3SourceConfig）、`core/pipeline.py`（_build_sqs）
- Test: `tests/test_sqs_s3_dispatcher.py`、`tests/test_config.py`

**Interfaces:**
- Consumes: `SqsPipeline.process(body, on_progress=None)`（Task 8）。
- Produces: `sources.s3_source.base_prefix(fqdn: str, org_id: str, log_type: str) -> str`；`SqsS3Dispatcher(..., reconcile_interval_sec: int = 0, reconcile_window_sec: int = 7200, reconcile_safety_lag_sec: int = 300)` 與 `_reconcile() -> None`；config 欄位 `reconcile_interval_sec: int = 3600`、`reconcile_window_sec: int = 7200`。

- [ ] **Step 1: 寫失敗測試**

`tests/test_sqs_s3_dispatcher.py` 加入（測試區已有 `from datetime import ...`？沒有 — 在測試檔 `import gzip` 附近加 `from datetime import datetime, timedelta, timezone`）：

```python
@mock_aws
def test_reconcile_processes_object_missed_by_queue(aws_env):
    """An object that never produced an SQS message is picked up by the sweep."""
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    now = datetime.now(timezone.utc)
    key = f"pce.example.com/org_id=1/auditable/{now:%Y%m%d}_missed.json.gz"
    _put_object(s3, bucket, key, [{"a": 1}])
    # Deliberately NO SQS message — simulates SNS delivery loss.

    sink = CapturingSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)
    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="test-bucket", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10,
                       reconcile_interval_sec=3600,
                       reconcile_window_sec=7200,
                       reconcile_safety_lag_sec=0)
    d._started_at_wall = now - timedelta(hours=1)
    d._reconcile()
    assert len(sink.sent) == 1


@mock_aws
def test_reconcile_skips_keys_already_processed_via_queue(aws_env):
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    now = datetime.now(timezone.utc)
    key = f"pce.example.com/org_id=1/auditable/{now:%Y%m%d}_x.json.gz"
    _put_object(s3, bucket, key, [{"a": 1}])
    _enqueue_event(sqs, qurl, bucket, key)

    sink = CapturingSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)
    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="test-bucket", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10,
                       reconcile_interval_sec=3600,
                       reconcile_window_sec=7200,
                       reconcile_safety_lag_sec=0)
    d._started_at_wall = now - timedelta(hours=1)
    d.consume_one_batch()          # processed via queue; key recorded
    assert len(sink.sent) == 1
    d._reconcile()                 # must NOT process it again
    assert len(sink.sent) == 1
```

`tests/test_config.py` 加入：

```python
def test_sqs_source_reconcile_defaults():
    from core.config import SqsS3SourceConfig
    cfg = SqsS3SourceConfig(queue_url="https://q", bucket="b", fqdn="f", org_id="1")
    assert cfg.reconcile_interval_sec == 3600
    assert cfg.reconcile_window_sec == 7200
```

- [ ] **Step 2: 確認失敗**

Run: `.venv/bin/pytest tests/test_sqs_s3_dispatcher.py -v -k reconcile && .venv/bin/pytest tests/test_config.py::test_sqs_source_reconcile_defaults -v`
Expected: 全部 FAIL

- [ ] **Step 3: 實作**

`sources/s3_source.py`：在 `path_to_log_type` 之前加 module 函式，並讓 `S3Source._base_prefix` 委派：

```python
def base_prefix(fqdn: str, org_id: str, log_type: str) -> str:
    """S3 key prefix for a log_type: {fqdn}/org_id={org_id}/<sub_path>/."""
    if log_type not in _LOG_TYPE_PATH:
        raise SourceError(f"unknown log_type: {log_type}")
    return f"{fqdn}/org_id={org_id}/{_LOG_TYPE_PATH[log_type]}/"
```

```python
    def _base_prefix(self, log_type: str) -> str:
        return base_prefix(self.fqdn, self.org_id, log_type)
```

`sources/sqs_s3_source.py`：
- import 區加 `from datetime import datetime, timedelta, timezone` 與 `from sources.s3_source import base_prefix, path_to_log_type`（合併既有 import）。
- `__init__` 參數列加 `reconcile_interval_sec: int = 0, reconcile_window_sec: int = 7200, reconcile_safety_lag_sec: int = 300`，並設定：

```python
        self.reconcile_interval_sec = reconcile_interval_sec
        self.reconcile_window_sec = reconcile_window_sec
        self.reconcile_safety_lag_sec = reconcile_safety_lag_sec
        self._processed_keys: Dict[str, float] = {}
        self._started_at_wall: Optional[datetime] = None
        self._last_reconcile = time.monotonic()
```

- `_handle_object` 尾段改為成功才記錄 key（接續 Task 8 的版本）：

```python
        extend_visibility()
        ok = pipeline.process(decoded, on_progress=extend_visibility)
        if ok:
            self._processed_keys[ref.key] = time.monotonic()
        return ok
```

- `run_forever`（接續 Task 10 的版本）：`self._warn_if_no_dlq()` 之後加 `self._started_at_wall = datetime.now(timezone.utc)`；`messages += n` 之後、except 之前加：

```python
                if (self.reconcile_interval_sec
                        and time.monotonic() - self._last_reconcile
                        >= self.reconcile_interval_sec):
                    self._reconcile()
                    self._last_reconcile = time.monotonic()
```

- 新增方法：

```python
    def _reconcile(self) -> None:
        """Sweep recent S3 objects and process any the queue never delivered.

        Only objects created after process start are considered, so restarts
        never cause duplicate forwarding from this path.
        """
        now_mono = time.monotonic()
        for k, t in list(self._processed_keys.items()):
            if now_mono - t > self.reconcile_window_sec:
                del self._processed_keys[k]

        now = datetime.now(timezone.utc)
        low = now - timedelta(seconds=self.reconcile_window_sec)
        if self._started_at_wall and self._started_at_wall > low:
            low = self._started_at_wall
        high = now - timedelta(seconds=self.reconcile_safety_lag_sec)
        if low >= high:
            return

        missed = 0
        for log_type in self._by_log_type:
            prefix_root = base_prefix(self.fqdn, self.org_id, log_type)
            day = low.date()
            while day <= high.date():
                prefix = f"{prefix_root}{day.strftime('%Y%m%d')}_"
                paginator = self.s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                    for obj in page.get("Contents", []) or []:
                        if not (low < obj["LastModified"] <= high):
                            continue
                        if obj["Key"] in self._processed_keys:
                            continue
                        log.warning("reconcile: %s never arrived via SQS; "
                                    "processing directly", obj["Key"])
                        if self._handle_object(
                                S3ObjectRef(bucket=self.bucket, key=obj["Key"])):
                            missed += 1
                day += timedelta(days=1)
        if missed:
            log.warning("reconcile: recovered %d missed objects", missed)
        else:
            log.info("reconcile: no missed objects")
```

`core/config.py` `SqsS3SourceConfig` 加欄位：

```python
    reconcile_interval_sec: int = Field(
        default=3600, ge=0,
        description="Sweep S3 for objects the queue never delivered every N "
                    "seconds. 0 disables.",
    )
    reconcile_window_sec: int = Field(
        default=7200, ge=600,
        description="How far back the reconcile sweep looks.",
    )
```

`core/pipeline.py` `_build_sqs` 的 `SqsS3Dispatcher(...)` 呼叫加：

```python
        reconcile_interval_sec=cfg.source.reconcile_interval_sec,
        reconcile_window_sec=cfg.source.reconcile_window_sec,
```

- [ ] **Step 4: 確認通過**

Run: `.venv/bin/pytest tests/test_sqs_s3_dispatcher.py tests/test_config.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 全套測試 + commit**

Run: `.venv/bin/pytest -q` → 全綠

```bash
git add sources/s3_source.py sources/sqs_s3_source.py core/config.py core/pipeline.py tests/test_sqs_s3_dispatcher.py tests/test_config.py
git commit -m "feat(sqs): hourly S3 reconciliation sweep recovers objects whose SQS notification was lost"
```

---

## Phase D — Sinks

### Task 12: TCP/TLS keepalive 調校 + 半開連線偵測

**問題：** `sinks/tcp_sink.py` 只開 `SO_KEEPALIVE`（Linux 預設 2 小時才偵測死連線）；對端 FIN 之後 `sendall` 仍靜默成功，事件寫進死連線後遺失且 checkpoint 已前進。

**修法：** (a) 統一 `_tune_keepalive()`：KEEPIDLE=30、KEEPINTVL=10、KEEPCNT=3、TCP_USER_TIMEOUT=60s（皆 hasattr 防護）；(b) send 前偵測 socket 可讀 EOF（`MSG_PEEK`），可讀即代表 FIN/RST → 重連再送。TLS 的 SSLSocket 不支援 MSG_PEEK，override 為不檢查（靠 TCP_USER_TIMEOUT）。

**Files:**
- Modify: `sinks/tcp_sink.py`、`sinks/tls_sink.py`
- Test: `tests/test_sinks_tcp_tls.py`

**Interfaces:**
- Produces: `sinks.tcp_sink._tune_keepalive(sock) -> None`；`TcpSink._peer_closed() -> bool`（TlsSink override 回傳 False）。

- [ ] **Step 1: 寫失敗測試**

`tests/test_sinks_tcp_tls.py` 加入：

```python
def test_tcp_socket_keepalive_tuned(tcp_listener):
    host, port, _ = tcp_listener
    sink = TcpSink(host=host, port=port, max_retries=0, retry_backoff_sec=[])
    assert sink.send(b"x") is True
    s = sink.sock
    assert s.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) == 1
    if hasattr(socket, "TCP_KEEPIDLE"):
        assert s.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE) == 30
    if hasattr(socket, "TCP_USER_TIMEOUT"):
        assert s.getsockopt(socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT) == 60_000
    sink.close()


def test_tcp_detects_peer_close_and_reconnects(tcp_listener):
    """After the server closes the connection (FIN), the next send must
    detect it and reconnect instead of writing into a dead socket."""
    host, port, received = tcp_listener
    sink = TcpSink(host=host, port=port, max_retries=2,
                   retry_backoff_sec=[0.01, 0.01])
    assert sink.send(b"first") is True
    time.sleep(0.6)   # listener's 0.3s idle timeout closes the connection
    assert sink.send(b"second") is True
    sink.close()
    time.sleep(0.5)
    assert b"second\n" in b"".join(received)
```

- [ ] **Step 2: 確認失敗**

Run: `.venv/bin/pytest tests/test_sinks_tcp_tls.py -v -k "keepalive_tuned or peer_close"`
Expected: `keepalive_tuned` FAIL（TCP_KEEPIDLE 非 30）；`peer_close` FAIL（"second" 寫進死連線，received 收不到）

- [ ] **Step 3: 實作**

`sinks/tcp_sink.py`：import 加 `import select`；module 層加：

```python
_KEEPIDLE_SEC = 30
_KEEPINTVL_SEC = 10
_KEEPCNT = 3
_USER_TIMEOUT_MS = 60_000


def _tune_keepalive(sock: socket.socket) -> None:
    """Detect dead peers in ~1 minute instead of the kernel default (~2h)."""
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    if hasattr(socket, "TCP_KEEPIDLE"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, _KEEPIDLE_SEC)
    if hasattr(socket, "TCP_KEEPINTVL"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, _KEEPINTVL_SEC)
    if hasattr(socket, "TCP_KEEPCNT"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, _KEEPCNT)
    if hasattr(socket, "TCP_USER_TIMEOUT"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT, _USER_TIMEOUT_MS)
```

`TcpSink._connect` 改為：

```python
    def _connect(self) -> socket.socket:
        s = socket.create_connection((self.host, self.port), timeout=self.timeout)
        _tune_keepalive(s)
        return s
```

新增方法：

```python
    def _peer_closed(self) -> bool:
        """A readable socket on a send-only syslog connection almost always
        means FIN/RST. MSG_PEEK leaves any real data intact."""
        if self.sock is None:
            return False
        try:
            r, _, _ = select.select([self.sock], [], [], 0)
            if not r:
                return False
            return self.sock.recv(1, socket.MSG_PEEK) == b""
        except OSError:
            return True
```

`send` 的 try 區塊改為：

```python
            try:
                s = self._ensure_socket()
                if self._peer_closed():
                    raise OSError("peer closed connection")
                s.sendall(frame)
                return True
```

`sinks/tls_sink.py`：import 改 `from sinks.tcp_sink import TcpSink, _tune_keepalive`；`_connect` 內兩行 `raw.setsockopt(...)` 換成 `_tune_keepalive(raw)`；新增：

```python
    def _peer_closed(self) -> bool:
        # SSLSocket does not support MSG_PEEK; rely on TCP_USER_TIMEOUT
        # and keepalive from _tune_keepalive instead.
        return False
```

- [ ] **Step 4: 確認通過**

Run: `.venv/bin/pytest tests/test_sinks_tcp_tls.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 全套測試 + commit**

Run: `.venv/bin/pytest -q` → 全綠

```bash
git add sinks/tcp_sink.py sinks/tls_sink.py tests/test_sinks_tcp_tls.py
git commit -m "fix(tcp): aggressive keepalive + TCP_USER_TIMEOUT and pre-send half-open detection to shrink silent-loss window"
```

---

### Task 13: TCP/TLS 截斷改為可設定，預設不截斷

**問題：** `sinks/tcp_sink.py:14-21` 硬性 8192 截斷會產生無法解析的半截 JSON——靜默資料毀損。RFC 6587 non-transparent framing 沒有 8K 上限。

**修法：** `NetworkSinkConfig.max_bytes` 改為 `Optional[int] = None`（None = 依 type 預設：udp 8192、tcp/tls 0 即不截斷）；`TcpSink` 增加 `max_bytes` 參數（0 = 不截斷），module 函式 `_truncate_if_needed` 改為 method。

**Files:**
- Modify: `sinks/tcp_sink.py`、`sinks/tls_sink.py`、`core/config.py`（NetworkSinkConfig）、`core/pipeline.py`（_build_sink）
- Test: `tests/test_sinks_tcp_tls.py`

**Interfaces:**
- Produces: `TcpSink(..., max_bytes: int = 0)`、`TlsSink(..., max_bytes: int = 0)`、`TcpSink._truncate_if_needed(wire: bytes) -> bytes`；`NetworkSinkConfig.max_bytes: Optional[int] = None`。

- [ ] **Step 1: 改寫測試**

`tests/test_sinks_tcp_tls.py`：頂部 import 改為 `from sinks.tcp_sink import TcpSink`（移除 `_truncate_if_needed`）；把 `test_tcp_truncates_over_8192` 整個換成：

```python
def test_tcp_no_truncation_by_default():
    sink = TcpSink(host="127.0.0.1", port=1)
    big = b"A" * 10000
    assert sink._truncate_if_needed(big) == big


def test_tcp_truncates_when_max_bytes_set():
    sink = TcpSink(host="127.0.0.1", port=1, max_bytes=8192)
    out = sink._truncate_if_needed(b"A" * 10000)
    assert len(out) == 8192
```

- [ ] **Step 2: 確認失敗**

Run: `.venv/bin/pytest tests/test_sinks_tcp_tls.py -v -k truncat`
Expected: 兩個都 FAIL（TypeError / AttributeError）

- [ ] **Step 3: 實作**

`sinks/tcp_sink.py`：刪除 module 層 `_TCP_MAX` 與 `_truncate_if_needed` 函式；`__init__` 參數列加 `max_bytes: int = 0`、`self.max_bytes = max_bytes`；新增 method 並在 `send` 開頭使用：

```python
    def _truncate_if_needed(self, wire: bytes) -> bytes:
        if self.max_bytes and len(wire) > self.max_bytes:
            log.warning("TCP payload %d > max_bytes=%d; truncating "
                        "(the truncated event will not parse as JSON)",
                        len(wire), self.max_bytes)
            return wire[:self.max_bytes]
        return wire

    def send(self, wire: bytes) -> bool:
        wire = self._truncate_if_needed(wire)
        frame = wire + b"\n"
```

`sinks/tls_sink.py` `__init__` 參數列加 `max_bytes: int = 0` 並傳給 `super().__init__(..., max_bytes=max_bytes)`。

`core/config.py` `NetworkSinkConfig`：

```python
    max_bytes: Optional[int] = Field(
        default=None, ge=0,
        description="Max payload bytes before truncation. None = per-type "
                    "default: udp 8192, tcp/tls 0 (no truncation).",
    )
```

`core/pipeline.py` `_build_sink`：

```python
    if sc.type == "udp":
        max_bytes = sc.max_bytes if sc.max_bytes is not None else 8192
        return UdpSink(host=sc.host, port=sc.port, max_bytes=max_bytes)
    if sc.type == "tcp":
        return TcpSink(host=sc.host, port=sc.port,
                       timeout_sec=sc.timeout_sec,
                       max_retries=sc.max_retries,
                       retry_backoff_sec=sc.retry_backoff_sec,
                       max_bytes=sc.max_bytes or 0)
    if sc.type == "tls":
        tls = sc.tls
        return TlsSink(host=sc.host, port=sc.port,
                       verify=tls.verify if tls else True,
                       ca_file=tls.ca_file if tls else None,
                       timeout_sec=sc.timeout_sec,
                       max_retries=sc.max_retries,
                       retry_backoff_sec=sc.retry_backoff_sec,
                       max_bytes=sc.max_bytes or 0)
```

- [ ] **Step 4: 確認通過**

Run: `.venv/bin/pytest tests/test_sinks_tcp_tls.py tests/test_sinks_udp.py tests/test_config.py -v`
Expected: 全部 PASS（UDP 預設 8192 行為不變）

- [ ] **Step 5: 全套測試 + commit**

Run: `.venv/bin/pytest -q` → 全綠

```bash
git add sinks/tcp_sink.py sinks/tls_sink.py core/config.py core/pipeline.py tests/test_sinks_tcp_tls.py
git commit -m "fix(tcp): make truncation opt-in (default off for tcp/tls) to stop silently corrupting large events"
```

---

### Task 14: MultiSink 新增 require: any|all

**問題：** `sinks/multi_sink.py` 固定 `any()`：任一子 sink 成功即 checkpoint 前進，失敗子 sink 的事件不重放。「SIEM + 本機備份」組態下 SIEM 斷線期間資料只存在本機檔案。

**Files:**
- Modify: `sinks/multi_sink.py`、`core/config.py`（MultiSinkConfig）、`core/pipeline.py`（_build_sink）
- Test: `tests/test_file_sink.py`（MultiSink 測試已在此檔）

**Interfaces:**
- Produces: `MultiSink(sinks, require: str = "any")`；`MultiSinkConfig.require: Literal["any", "all"] = "any"`。

- [ ] **Step 1: 寫失敗測試**

`tests/test_file_sink.py` MultiSink 區加入：

```python
def test_multi_require_all_fails_on_partial_failure():
    ms = MultiSink([_OkSink(), _FailSink()], require="all")
    assert ms.send(b"x") is False
    assert ms.flush() is False


def test_multi_require_all_ok_when_all_ok():
    ms = MultiSink([_OkSink(), _OkSink()], require="all")
    assert ms.send(b"x") is True


def test_multi_sink_config_require_field():
    from core.config import PipelineConfig
    pc = PipelineConfig(**{
        "name": "dual", "log_type": "pd2", "poll_interval_sec": 60,
        "mapper": {"format": "syslog_json"},
        "sink": {"type": "multi", "require": "all", "sinks": [
            {"type": "tls", "host": "siem.example.com", "port": 6514},
            {"type": "file", "path": "/var/log/illumio/events.log"},
        ]},
    })
    assert pc.sink.require == "all"
```

- [ ] **Step 2: 確認失敗**

Run: `.venv/bin/pytest tests/test_file_sink.py -v -k require`
Expected: 全部 FAIL

- [ ] **Step 3: 實作**

`sinks/multi_sink.py`：

```python
class MultiSink(Sink):
    def __init__(self, sinks: List[Sink], require: str = "any"):
        self._sinks = sinks
        self.require = require

    def _combine(self, results: List[bool]) -> bool:
        return all(results) if self.require == "all" else any(results)

    def send(self, wire: bytes) -> bool:
        results = []
        for s in self._sinks:
            ok = s.send(wire)
            if not ok:
                log.warning("%s.send failed", type(s).__name__)
            results.append(ok)
        return self._combine(results)

    def flush(self) -> bool:
        results = []
        for s in self._sinks:
            ok = s.flush()
            if not ok:
                log.warning("%s.flush failed", type(s).__name__)
            results.append(ok)
        return self._combine(results)
```

（close 不動。docstring 補一句：`require="all"` 時任一子 sink 失敗即回 False、checkpoint 不前進。）

`core/config.py` `MultiSinkConfig` 加欄位：

```python
    require: Literal["any", "all"] = "any"
```

`core/pipeline.py` `_build_sink`：

```python
    if sc.type == "multi":
        return MultiSink([_build_sink(sub) for sub in sc.sinks], require=sc.require)
```

- [ ] **Step 4: 確認通過**

Run: `.venv/bin/pytest tests/test_file_sink.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 全套測試 + commit**

Run: `.venv/bin/pytest -q` → 全綠

```bash
git add sinks/multi_sink.py core/config.py core/pipeline.py tests/test_file_sink.py
git commit -m "feat(multi-sink): configurable require=any|all delivery semantics"
```

---

### Task 15: FileSink — 惰性開檔、孤兒檔清理、fsync 選項、死碼修正

**問題：** `sinks/file_sink.py` (a) `__init__` 立即 mkdir + 開檔（dry-run 副作用；`if self._path.exists()` 恆真，else 分支死碼）；(b) 輪替時 gzip 失敗留下的 `.log` 純文字檔不在 `*.log.gz` 清理範圍，無限累積；(c) `flush()` 不落盤，斷電遺失已 checkpoint 的事件。

**Files:**
- Modify: `sinks/file_sink.py`、`core/config.py`（FileSinkConfig）、`core/pipeline.py`（_build_sink 的 file 分支）
- Test: `tests/test_file_sink.py`

**Interfaces:**
- Produces: `FileSink(..., fsync: bool = False)`；建構不再開檔（首次 `send` 才開）；`FileSinkConfig.fsync: bool = False`。

- [ ] **Step 1: 寫失敗測試**

`tests/test_file_sink.py` 加入：

```python
def test_constructor_has_no_filesystem_side_effects(tmp_path):
    """dry-run safety: building the sink must not create dirs or files."""
    p = tmp_path / "sub" / "events.log"
    FileSink(str(p), prefix="")
    assert not p.exists()
    assert not p.parent.exists()


def test_cleanup_deletes_orphaned_plain_logs(tmp_path):
    """A .log left behind by a failed gzip during rotation must be reaped."""
    p = tmp_path / "events.log"
    sink = FileSink(str(p), retention_days=1, prefix="")
    sink.send(b"x")  # open the active file

    orphan = tmp_path / "events.20200101T000000.log"
    orphan.touch()
    old_time = time.time() - 2 * 86400
    os.utime(orphan, (old_time, old_time))

    sink._cleanup_old()
    sink.close()
    assert not orphan.exists()
    assert p.exists()  # active file untouched


def test_flush_fsyncs_when_enabled(tmp_path, monkeypatch):
    fsynced = []
    real_fsync = os.fsync
    monkeypatch.setattr(os, "fsync", lambda fd: (fsynced.append(fd), real_fsync(fd))[1])
    p = tmp_path / "events.log"
    sink = FileSink(str(p), prefix="", fsync=True)
    sink.send(b"data")
    assert sink.flush() is True
    sink.close()
    assert len(fsynced) >= 1
```

- [ ] **Step 2: 確認失敗**

Run: `.venv/bin/pytest tests/test_file_sink.py -v -k "no_filesystem_side_effects or orphaned_plain or fsyncs_when_enabled"`
Expected: 3 個都 FAIL

- [ ] **Step 3: 實作**

`sinks/file_sink.py`：

```python
    def __init__(
        self,
        path: str,
        rotation_mb: int = 200,
        rotation_hours: int = 24,
        retention_days: int = 30,
        prefix: str = "ILLUMIO_FLOW: ",
        fsync: bool = False,
    ):
        self._path = Path(path)
        self._max_bytes = rotation_mb * 1024 * 1024
        self._rotation_secs = rotation_hours * 3600
        self._retention_secs = retention_days * 86400
        self._prefix = prefix.encode("utf-8") if prefix else b""
        self._fsync = fsync
        self._fh = None
        self._size = 0
        self._file_opened_at = time.time()

    def _ensure_open(self) -> None:
        if self._fh is not None:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        if self._path.exists():
            st = self._path.stat()
            self._size = st.st_size
            # Preserve real file age across restarts so rotation_hours still fires.
            self._file_opened_at = st.st_mtime
        else:
            self._size = 0
            self._file_opened_at = time.time()
        self._fh = self._path.open("ab")
```

`send` 開頭加 `self._ensure_open()`：

```python
    def send(self, wire: bytes) -> bool:
        self._ensure_open()
        line = self._prefix + wire.rstrip(b"\n") + b"\n"
```

`flush` / `close`：

```python
    def flush(self) -> bool:
        if self._fh is None:
            return True
        try:
            self._fh.flush()
            if self._fsync:
                os.fsync(self._fh.fileno())
            return True
        except OSError as exc:
            log.error("file flush error: %s", exc)
            return False

    def close(self) -> None:
        if self._fh is None:
            return
        try:
            self._fh.flush()
            self._fh.close()
        except OSError:
            pass
```

`_cleanup_old` 改為兩種 pattern（`events.*.log` 不會匹配作用中的 `events.log`，因為 pattern 要求多一個點段）：

```python
    def _cleanup_old(self) -> None:
        cutoff = time.time() - self._retention_secs
        stem = self._path.stem
        for pattern in (f"{stem}.*.log.gz", f"{stem}.*.log"):
            for f in self._path.parent.glob(pattern):
                try:
                    if f.stat().st_mtime < cutoff:
                        f.unlink()
                        log.info("deleted expired log: %s", f.name)
                except OSError:
                    pass
```

`core/config.py` `FileSinkConfig` 加欄位：

```python
    fsync: bool = Field(
        default=False,
        description="fsync on every flush. Safest for compliance archives; "
                    "adds one disk sync per processed file.",
    )
```

`core/pipeline.py` `_build_sink` file 分支加 `fsync=sc.fsync,`。

- [ ] **Step 4: 確認通過**

Run: `.venv/bin/pytest tests/test_file_sink.py -v`
Expected: 全部 PASS（既有測試都先 `send` 才驗檔案內容，與惰性開檔相容）

- [ ] **Step 5: 全套測試 + commit**

Run: `.venv/bin/pytest -q` → 全綠

```bash
git add sinks/file_sink.py core/config.py core/pipeline.py tests/test_file_sink.py
git commit -m "fix(file-sink): lazy open, reap orphaned rotation leftovers, optional fsync, remove dead stat branch"
```

---

## Phase E — Filter 與其他強化

### Task 16: filter 運算式啟動驗證 + AST 預編譯 + main 統一 fail-fast

**問題：** `core/expression_filter.py` (a) 語法錯誤要到執行期才發現，且評估失敗 = 100% 靜默丟事件、只警告一次；(b) 每個事件重建 evaluator 並重新 parse，高流量下浪費 CPU。另外 `main()` 沒接 `build_pipelines_from_config` 的 `ConfigError`。

**修法：** compile 時 parse 一次（`SyntaxError` → `ConfigError`，服務啟動即失敗、訊息清楚）；match 重用 evaluator 與 parsed AST（每條 pipeline 一個 filter_fn、APScheduler `max_instances=1`，單執行緒使用安全）；執行期錯誤行為維持「回 False + 警告一次」。`collector.main` 把 build 包進 ConfigError 處理。

**Files:**
- Modify: `core/expression_filter.py`、`collector.py`
- Test: `tests/test_filter.py`

**Interfaces:**
- Produces: `compile_expression(expression: str)` 於語法錯誤時 raise `core.exceptions.ConfigError`；其餘簽名不變。

- [ ] **Step 1: 改寫測試**

`tests/test_filter.py`：頂部加 import：

```python
import pytest

from core.exceptions import ConfigError
from core.expression_filter import compile_expression
```

把 `test_invalid_expression_always_false` 換成：

```python
def test_invalid_expression_raises_config_error_at_compile():
    """Fail fast at startup: a typo must not silently drop 100% of events."""
    with pytest.raises(ConfigError):
        compile_expression("this is not valid python")


def test_runtime_error_returns_false():
    match = compile_expression("ev.x / 0")
    assert match({"x": 1}) is False
```

（`test_dangerous_builtins_blocked` 不動：`__import__('os')` 語法合法、於評估期被 simpleeval 擋下回 False。）

- [ ] **Step 2: 確認失敗**

Run: `.venv/bin/pytest tests/test_filter.py -v`
Expected: `test_invalid_expression_raises_config_error_at_compile` FAIL（沒有 raise），其餘 PASS

- [ ] **Step 3: 實作**

`core/expression_filter.py`：import 加 `from core.exceptions import ConfigError`；`compile_expression` 改為：

```python
def compile_expression(expression: str) -> Callable[[dict], bool]:
    """Return a function ``match(event_dict) -> bool``.

    The expression is parsed once at compile time; a syntax error raises
    ConfigError so the service fails fast at startup. Evaluation errors at
    runtime return False (logged once) — bad filters drop events, which is
    visible in tick/process log counts.

    The returned closure reuses one evaluator instance and the parsed AST.
    This is safe because each pipeline gets its own filter_fn and pipelines
    run single-threaded (APScheduler max_instances=1 / serial SQS dispatch).
    """
    safe_funcs = {"str": str, "len": len, **DEFAULT_FUNCTIONS}
    evaluator = EvalWithCompoundTypes(functions=safe_funcs)
    try:
        parsed = evaluator.parse(expression)
    except SyntaxError as e:
        raise ConfigError(f"invalid filter expression {expression!r}: {e}") from e
    _warned = {"once": False}

    def match(event: dict) -> bool:
        try:
            evaluator.names = {"ev": DotDict(event)}
            result = _run_sandboxed(evaluator, expression, parsed)
            return bool(result)
        except Exception as e:  # noqa: BLE001 - intentional broad catch
            if not _warned["once"]:
                log.warning("filter expression error (suppressed after first): %s", e)
                _warned["once"] = True
            return False

    return match
```

`collector.py` `main()`：

```python
    result = build_pipelines_from_config(cfg)
```

改為：

```python
    try:
        result = build_pipelines_from_config(cfg)
    except ConfigError as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 2
```

- [ ] **Step 4: 確認通過**

Run: `.venv/bin/pytest tests/test_filter.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 全套測試 + commit**

Run: `.venv/bin/pytest -q` → 全綠

```bash
git add core/expression_filter.py collector.py tests/test_filter.py
git commit -m "feat(filter): validate expression syntax at startup (ConfigError) and reuse precompiled AST per event"
```

---

### Task 17: 壞 JSON 行改為每檔/每訊息彙總一次警告

**問題：** `core/pipeline.py:104` 與 `core/sqs_pipeline.py:53` 每一行壞 JSON 都 log 一次 WARNING——一個毀損的百萬行檔案灌出百萬行 log。

**Files:**
- Modify: `core/pipeline.py`（_process_file）、`core/sqs_pipeline.py`（process）
- Test: `tests/test_pipeline.py`

**Interfaces:**
- Produces: 行為不變（壞行照樣 skip、計入 mapper_err），僅 log 頻率改為每檔案/每訊息至多一則 WARNING。

- [ ] **Step 1: 寫失敗測試**

`tests/test_pipeline.py` 加入（頂部加 `import logging`）：

```python
def test_bad_json_lines_warned_once_per_file(tmp_state_dir, caplog):
    body = gzip.compress(b'bad1\nbad2\n{"a":1}\nbad3\n')
    files = [("k1", _lm("2026-04-20T10:00:00+00:00"), body)]
    sink = FakeSink()
    p = Pipeline(
        name="p1", log_type="auditable",
        source=FakeSource(files), mapper=FakeMapper(), sink=sink,
        checkpoint_store=CheckpointStore(tmp_state_dir),
        filter_fn=None, max_files_per_tick=100,
    )
    with caplog.at_level(logging.WARNING, logger="p1"):
        p.tick()
    warnings = [r for r in caplog.records if "bad JSON" in r.getMessage()]
    assert len(warnings) == 1
    assert "3" in warnings[0].getMessage()
    assert len(sink.sent) == 1
```

- [ ] **Step 2: 確認失敗**

Run: `.venv/bin/pytest tests/test_pipeline.py::test_bad_json_lines_warned_once_per_file -v`
Expected: FAIL（收到 3 則 warning）

- [ ] **Step 3: 實作**

`core/pipeline.py` `_process_file`：

```python
    def _process_file(self, key: str, body: bytes, stats: dict) -> tuple[bool, int]:
        sent_in_file = 0
        bad_json = 0
        try:
            body = gzip.decompress(body)
        except Exception:
            pass  # not gzipped; use as-is
        for raw_line in body.splitlines():
            if not raw_line.strip():
                continue
            stats["read"] += 1
            try:
                ev = json.loads(raw_line)
            except Exception:
                stats["mapper_err"] += 1
                bad_json += 1
                continue
```

迴圈結束後（`if sent_in_file and not self.sink.flush():` 之前）加：

```python
        if bad_json:
            self.log.warning("%d bad JSON lines in %s; skipped", bad_json, key)
```

`core/sqs_pipeline.py` `process` 同樣處理：`stats` 初始化後加 `bad_json = 0`；壞行分支改為：

```python
                except Exception:
                    stats["mapper_err"] += 1
                    bad_json += 1
                    continue
```

迴圈結束後（`if sent_in_msg and not self.sink.flush():` 之前）加：

```python
            if bad_json:
                self.log.warning("%d bad JSON lines in message; skipped", bad_json)
```

- [ ] **Step 4: 確認通過**

Run: `.venv/bin/pytest tests/test_pipeline.py tests/test_sqs_pipeline.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 全套測試 + commit**

Run: `.venv/bin/pytest -q` → 全綠

```bash
git add core/pipeline.py core/sqs_pipeline.py tests/test_pipeline.py
git commit -m "fix(pipeline): aggregate bad-JSON warnings per file/message to prevent log floods"
```

---

### Task 18: boto3 client 明確 retry/timeout 設定

**問題：** boto3 client 用隱含預設；行為不可預期也不可調校。

**Files:**
- Modify: `core/pipeline.py`（_build_polling、_build_sqs）
- Test: `tests/test_pipeline.py`

**Interfaces:**
- Produces: 兩種模式的 s3/sqs client 一律帶 `botocore.config.Config(retries={"mode": "standard", "max_attempts": 5}, connect_timeout=10, read_timeout=70)`。read_timeout 70 必須大於 SQS long-poll 的 20 秒。

- [ ] **Step 1: 寫失敗測試**

`tests/test_pipeline.py` factory 區加入：

```python
def test_boto_clients_use_standard_retries(tmp_path, monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "y")
    p = tmp_path / "cfg.yaml"
    p.write_text(_factory_cfg_text(tmp_path))
    from core.config import load_config
    from core.pipeline import build_pipelines_from_config
    result = build_pipelines_from_config(load_config(str(p)))
    s3_client = result.polling[0][0].source.s3
    assert s3_client.meta.config.retries["mode"] == "standard"
    assert s3_client.meta.config.read_timeout == 70
    assert s3_client.meta.config.connect_timeout == 10
```

- [ ] **Step 2: 確認失敗**

Run: `.venv/bin/pytest tests/test_pipeline.py::test_boto_clients_use_standard_retries -v`
Expected: FAIL（預設 retries mode 是 legacy）

- [ ] **Step 3: 實作**

`core/pipeline.py`：module 層（factory 區開頭）加：

```python
def _boto_config():
    from botocore.config import Config as BotoConfig
    return BotoConfig(
        retries={"mode": "standard", "max_attempts": 5},
        connect_timeout=10,
        read_timeout=70,  # must exceed the SQS long-poll wait (20s)
    )
```

`_build_polling`：`s3_client = session.client("s3", config=_boto_config())`。
`_build_sqs`：

```python
    sqs_client = session.client("sqs", config=_boto_config())
    s3_client = session.client("s3", config=_boto_config())
```

- [ ] **Step 4: 確認通過**

Run: `.venv/bin/pytest tests/test_pipeline.py -v`
Expected: 全部 PASS

- [ ] **Step 5: 全套測試 + commit**

Run: `.venv/bin/pytest -q` → 全綠

```bash
git add core/pipeline.py tests/test_pipeline.py
git commit -m "feat(aws): explicit standard retry mode and connect/read timeouts on all boto3 clients"
```

---

## Phase F — 設定範本、文件與部署

### Task 19: config.example.yaml 與 OPERATIONS.md 更新

**內容：** 把 Task 5/9/11/13/14/15 新增的設定欄位寫進範本（含註解），console 預設建議改 false，並在 OPERATIONS.md 新增「可靠性語意」章節。

**Files:**
- Modify: `config.example.yaml`、`docs/OPERATIONS.md`

**Interfaces:**
- Consumes: Task 5/9/11/13/14/15 的 config 欄位名稱與預設值（`safety_lag_sec=60`、`poison_receive_count=10`、`reconcile_interval_sec=3600`、`reconcile_window_sec=7200`、`max_bytes=None`、`require=any`、`fsync=false`）。

- [ ] **Step 1: 編輯 config.example.yaml**

(a) `source:`（type: s3）區塊、`org_id:` 行之後加：

```yaml
  # safety_lag_sec: 60          # 只處理 LastModified 早於 now-60s 的物件，
                                 # 關閉 S3 列表同秒競態的漏檔窗口（0 = 停用）
```

(b) 註解掉的 sqs_s3 範例區塊、`#   # max_workers: 1` 行之後加：

```yaml
#   # poison_receive_count: 10       # 訊息重複收到 N 次後視為毒訊息：記 ERROR 後刪除（0 = 停用）
#   # reconcile_interval_sec: 3600   # 每 N 秒掃描 S3，補撿 SQS 從未送達的物件（0 = 停用）
#   # reconcile_window_sec: 7200     # 對帳掃描回看的時間窗
```

(c) `logging:` 區塊的 `console: true` 行改為：

```yaml
  console: false                   # 是否同時印到終端（systemd 部署建議 false，避免 journal 與檔案雙份）
```

(d) 在 pipelines 區段中第一個出現 `sink:` 範例處，於適當位置補上新欄位的註解說明（依實際檔案內容找 tcp/tls 與 multi、file 範例各補一行）：

- tcp/tls sink 範例內：`# max_bytes: 0            # 0 = 不截斷（預設）；設限會把超長事件截成無法解析的 JSON，不建議`
- multi sink 範例內：`# require: any            # any = 任一子 sink 成功即前進 checkpoint；all = 全部成功才前進`
- file sink 範例內：`# fsync: false            # true = 每次 flush 落盤（合規存證用；每檔多一次磁碟同步）`

- [ ] **Step 2: 驗證範本仍可解析**

Run: `.venv/bin/python collector.py --config config.example.yaml --dry-run`
Expected: 輸出 banner 與 `[dry-run] config OK, exiting.`，exit code 0（若範本含未填的占位 AWS 值導致 pydantic 錯誤，修正你加入的行直到 dry-run 通過；不動既有內容）

- [ ] **Step 3: 編輯 docs/OPERATIONS.md**

在文件末尾加入新章節：

```markdown
## 可靠性語意（Reliability Semantics）

### 傳遞保證

本 collector 提供 **at-least-once** 傳遞：故障重試路徑（S3 輪詢 checkpoint 重放、
SQS visibility 重新投遞、對帳掃描）都可能造成事件重複送達 SIEM，但不會靜默遺失。
SIEM 端如需去重，可用事件內的 `href` 或（syslog 格式）整行內容雜湊。

已知的殘餘遺失窗口（無法在無應答協定上完全消除）：

- **UDP sink**：fire-and-forget，網路丟包即遺失。只建議在同機房低丟包環境使用。
- **TCP/TLS sink**：`send` 成功只代表資料進了本機 kernel buffer。對端當機時
  in-flight 資料會遺失。collector 已用 keepalive（30s/10s/3 次）與
  TCP_USER_TIMEOUT（60s）把偵測窗口壓到約一分鐘，並在每次送出前偵測對端
  FIN；如果可靠性要求更高，請改用 HTTPS sink（有應答、有重試）。
- **multi sink 的 require: any（預設）**：任一子 sink 成功即前進 checkpoint，
  失敗子 sink 在該窗口的事件不會重放。若「SIEM 與本機備份都必須收到」，
  設 `require: all`（代價：任一目的地斷線會暫停整條 pipeline 的進度）。

### SQS 模式的 DLQ 與毒訊息

- 強烈建議在 SQS 隊列上設定 RedrivePolicy（DLQ）。collector 啟動時會檢查，
  未設定會記 WARNING。
- 未設 DLQ 時由 `poison_receive_count`（預設 10）兜底：同一訊息重複收到
  N 次後記 ERROR（含 body 開頭 500 字元）並刪除。請對 ERROR log 設告警。
- 監控 DLQ 深度（CloudWatch `ApproximateNumberOfMessagesVisible`）。

### 對帳掃描（reconcile）

SQS 模式下每 `reconcile_interval_sec`（預設 3600 秒）列出近
`reconcile_window_sec`（預設 7200 秒）內的 S3 物件，補處理 SQS 從未送達的
物件（SNS 投遞失敗的保險）。只考慮本次啟動之後建立的物件，重啟不會重複轉發。
每次補撿都會記 WARNING `reconcile: ... never arrived via SQS`——頻繁出現代表
SNS→SQS 訂閱有問題，需檢查。

### 健康監控

- polling 模式：每個 tick 記一行 INFO 統計（`tick: files=... sent=...`）。
- SQS 模式：每 5 分鐘記一行 `heartbeat: batches=N messages=N`。
- 監控建議：對「超過 15 分鐘沒有任何 tick/heartbeat 行」告警（服務卡死），
  對「sent 長期為 0 但 S3 有新物件」告警（靜默故障）。

### 吞吐上限

SQS 模式 dispatcher 為單執行緒（`max_workers` 目前僅支援 1）：一種 log_type
的慢 sink 會阻塞其他 log_type 的處理。單一 pipeline 的吞吐瓶頸通常在 sink
往返延遲。
```

- [ ] **Step 4: 全套測試 + commit**

Run: `.venv/bin/pytest -q` → 全綠

```bash
git add config.example.yaml docs/OPERATIONS.md
git commit -m "docs: document reliability semantics, new config knobs, DLQ/monitoring guidance"
```

---

### Task 20: requirements.lock 產生並納入安裝與離線打包流程

**問題：** `requirements.txt` 只鎖範圍（`boto3>=1.34,<2.0`），部署不可重現。

**Files:**
- Create: `requirements.lock`
- Modify: `scripts/install.sh`（65、89-92、101-102 行附近）、`scripts/build_offline_bundle.sh`（37-42、51 行附近）、`README.md`

**Interfaces:**
- Produces: `requirements.lock`（pip freeze 全鎖版本）；install/bundle 腳本優先使用 lock 檔。

- [ ] **Step 1: 產生 lock 檔**

```bash
python3 -m venv /tmp/illumio-lockenv
/tmp/illumio-lockenv/bin/pip install -q -r requirements.txt
/tmp/illumio-lockenv/bin/pip freeze > requirements.lock
rm -rf /tmp/illumio-lockenv
```

Run: `head requirements.lock`
Expected: 每行 `package==x.y.z` 的完整鎖定清單（含 boto3、botocore、pydantic 等傳遞依賴）

- [ ] **Step 2: 修改 scripts/install.sh**

(a) 第 65 行的複製清單 `requirements.txt` 之後加上 `requirements.lock`：

```bash
  for item in collector.py s3_log_checker.py core sources mappers sinks mappings siem_parser requirements.txt requirements.lock config.example.yaml; do
```

(b) 在檔案前段（第一次使用 requirements 之前）加輔助變數：

```bash
REQ_FILE="${INSTALL_DIR}/app/requirements.txt"
if [ -f "${INSTALL_DIR}/app/requirements.lock" ]; then
  REQ_FILE="${INSTALL_DIR}/app/requirements.lock"
fi
```

(c) 第 89-92 行與 101-102 行的 `-r "${INSTALL_DIR}/app/requirements.txt"` 改為 `-r "${REQ_FILE}"`。

- [ ] **Step 3: 修改 scripts/build_offline_bundle.sh**

第 37-42 行 `pip download` 的 `-r "${REPO_ROOT}/requirements.txt"` 改為優先 lock：

```bash
REQ_FILE="${REPO_ROOT}/requirements.txt"
[ -f "${REPO_ROOT}/requirements.lock" ] && REQ_FILE="${REPO_ROOT}/requirements.lock"
python3 -m pip download \
  ...（原有參數不動）...
  -r "${REQ_FILE}"
```

第 51 行附近的打包清單加入 `"${REPO_ROOT}/requirements.lock"`（若該清單逐檔列舉）。

- [ ] **Step 4: 語法驗證**

Run: `bash -n scripts/install.sh && bash -n scripts/build_offline_bundle.sh && echo OK`
Expected: `OK`

- [ ] **Step 5: 更新 README.md**

在 README 提到 `requirements.txt` 的安裝段落附近加一句說明：

```markdown
> 依賴版本管理：`requirements.txt` 是範圍宣告（開發用），`requirements.lock`
> 是全鎖定版本（部署用）。install.sh 與離線打包會優先使用 lock 檔。
> 升級依賴後請重新產生：`pip freeze > requirements.lock`（在乾淨 venv 內）。
```

- [ ] **Step 6: 全套測試 + commit**

Run: `.venv/bin/pytest -q` → 全綠

```bash
git add requirements.lock scripts/install.sh scripts/build_offline_bundle.sh README.md
git commit -m "build: add requirements.lock for reproducible deploys; install/bundle scripts prefer it"
```

---

## 驗收清單（全部完成後）

- [ ] `.venv/bin/pytest -q` 全綠
- [ ] `.venv/bin/python collector.py --config config.example.yaml --dry-run` 成功，且 `state/` 內沒有新增 checkpoint 檔（Task 2 驗證）
- [ ] `git log --oneline` 呈現 20 個獨立 commit，每個對應一個 task
- [ ] 手動驗證（有 AWS 沙盒時）：corrupt 一個 checkpoint 檔後啟動服務，確認服務正常啟動並記錄 reset 訊息而非 crash

## 對照表：code review 發現 → task

| Review 發現 | Task |
|---|---|
| 啟動 corrupt checkpoint crash loop | 2 |
| checkpoint 無 fsync | 1 |
| gunzip 失敗永久跳檔 | 4 |
| S3 同秒競態漏檔 | 5 |
| 24-48h 停機日期前綴間隙 | 6 |
| polling 無 SIGTERM 清理 | 3 |
| SQS visibility 計時基準錯誤 | 7 |
| process 期間不延長 visibility | 8 |
| 毒訊息依賴外部 DLQ 假設 | 9 |
| TCP 靜默遺失（keepalive 2h、半開連線） | 12 |
| 8192 截斷毀損 JSON | 13 |
| filter 錯誤靜默丟 100% 事件、不啟動驗證 | 16 |
| dry-run 副作用 | 2 + 15 |
| FileSink 孤兒檔／fsync／死碼 | 15 |
| MultiSink any() 語意 | 14 |
| 每事件重建 evaluator | 16 |
| 壞 JSON log 洪水 | 17 |
| boto3 未設 retry/timeout | 18 |
| 無健康訊號 | 10 + 19（監控指引） |
| SQS 無對帳機制 | 11 |
| at-least-once 未文件化 | 19 |
| console 雙份 log | 19 |
| 依賴未鎖定 | 20 |
| max_workers 吞吐上限未標明 | 19 |
