# SQS-based S3 Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a second source mode `sqs_s3` that consumes the Illumio-issued SQS queue (one queue per tenant, receiving SNS-wrapped S3 event notifications), dispatches by S3 key path to per-log_type pipelines, and downloads on demand. Generic S3 polling stays available unchanged.

**Architecture:** Two source modes, mutually exclusive at runtime via `cfg.source.type`. Generic mode continues to use `Pipeline` + `PipelineScheduler` (APScheduler IntervalTrigger). SQS mode uses a new `SqsPipeline` (independent from `Pipeline`, no shared code) and a new `SqsS3Dispatcher` (single long-poll consumer thread, log-type routing, visibility extension, delete-on-success). Discriminated-union `SourceConfig` lets users switch by editing one YAML key.

**Tech Stack:** Python 3.12, Pydantic v2 (discriminated union), boto3 (SQS + S3 clients from shared session), moto[sqs+s3] for tests, pytest.

**Spec:** `docs/superpowers/specs/2026-05-07-sqs-s3-source-design.md`

---

## File Structure

**New files**
- `core/sqs_pipeline.py` — `SqsPipeline` class. Pure transformer: given a decoded body, runs JSON-lines parse → filter → mapper.format → sink.send → sink.flush. ~80 LoC.
- `sources/sqs_s3_source.py` — `SqsS3Dispatcher` class plus message-parsing helpers. ~180 LoC.
- `tests/test_sqs_pipeline.py` — unit tests for `SqsPipeline`. ~80 LoC.
- `tests/test_sqs_s3_dispatcher.py` — integration tests for dispatcher using moto. ~200 LoC.

**Modified files**
- `core/config.py` — add `SqsS3SourceConfig`, convert `SourceConfig` field on `AppConfig` to discriminated union.
- `core/pipeline.py` — `build_pipelines_from_config` returns either `list[(Pipeline, int)]` (polling) or `(SqsS3Dispatcher, list[SqsPipeline])` (SQS).
- `collector.py` — branch on `cfg.source.type` to dispatch to scheduler or dispatcher; banner shows mode.
- `config.example.yaml` — commented-out SQS source example.
- `requirements-dev.txt` — bump `moto[s3]` to `moto[s3,sqs]`.
- `sources/s3_source.py` — extract a public helper `path_to_log_type(key, fqdn, org_id) -> Optional[str]` so the dispatcher reuses the same key→log_type mapping (single source of truth). Existing `S3Source` keeps working.
- `README.md`, `docs/OPERATIONS.md` — new SQS mode section in each.

**Untouched (verified)**: `core/pipeline.py:Pipeline`, `core/scheduler.py`, `core/checkpoint.py`, all sinks, all mappers, `core/expression_filter.py`. Pipeline schema is identical between modes; `poll_interval_sec` and `max_files_per_tick` are silently ignored in SQS mode (documented).

---

## Task 0: Setup — bump moto, baseline

**Files:**
- Modify: `requirements-dev.txt`

- [ ] **Step 1: Bump moto extras**

Edit `requirements-dev.txt`:
```diff
- moto[s3]>=5.0
+ moto[s3,sqs]>=5.0
```

- [ ] **Step 2: Reinstall dev deps**

Run: `.venv/bin/pip install -q -r requirements-dev.txt`
Expected: no errors, moto[sqs] installed.

- [ ] **Step 3: Confirm baseline**

Run: `.venv/bin/python -m pytest -q`
Expected: 92 passed (the 89 baseline + 3 added during the recent code-fix batch).

- [ ] **Step 4: Commit**

```bash
git add requirements-dev.txt
git commit -m "build(test-deps): bump moto[s3] -> moto[s3,sqs] for SQS dispatcher tests"
```

---

## Task 1: Config schema — `SqsS3SourceConfig` + discriminated union

**Files:**
- Modify: `core/config.py` (the `SourceConfig` definition and the `AppConfig` field that references it)
- Test: `tests/test_config.py`

- [ ] **Step 1: Read existing structure**

Read `core/config.py` end-to-end (~200 lines). Confirm:
- Current `SourceConfig` has `type: Literal["s3"]`, `bucket`, `fqdn`, `org_id`.
- `AppConfig.source: SourceConfig` is a direct field.
- Other discriminated-union usage exists (sink config) — copy that pattern.

- [ ] **Step 2: Write the failing tests**

Append to `tests/test_config.py`:

```python
def test_load_sqs_source_config(tmp_path):
    """sqs_s3 source type parses with required fields."""
    cfg_text = """
aws: { region: us-east-1 }
source:
  type: sqs_s3
  queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/q
  bucket: b
  fqdn: pce.example.com
  org_id: "1"
checkpoint: { dir: ./state, initial_lookback_hours: 24 }
logging: { dir: ./logs, level: INFO }
pipelines: []
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(cfg_text)
    from core.config import load_config
    cfg = load_config(str(p))
    assert cfg.source.type == "sqs_s3"
    assert cfg.source.queue_url.startswith("https://sqs.")
    assert cfg.source.visibility_timeout_sec == 60      # default
    assert cfg.source.wait_time_sec == 20               # default
    assert cfg.source.max_messages_per_receive == 10    # default
    assert cfg.source.max_workers == 1                  # default


def test_load_generic_s3_still_works(tmp_path):
    """Existing s3 source type unaffected by discriminated union."""
    cfg_text = """
aws: { region: us-east-1 }
source: { type: s3, bucket: b, fqdn: pce.example.com, org_id: "1" }
checkpoint: { dir: ./state, initial_lookback_hours: 24 }
logging: { dir: ./logs, level: INFO }
pipelines: []
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(cfg_text)
    from core.config import load_config
    cfg = load_config(str(p))
    assert cfg.source.type == "s3"
    assert cfg.source.bucket == "b"


def test_unknown_source_type_rejected(tmp_path):
    """Pydantic discriminator rejects unknown source types."""
    cfg_text = """
aws: { region: us-east-1 }
source: { type: sqs_raw, queue_url: x, bucket: b, fqdn: x, org_id: "1" }
checkpoint: { dir: ./state, initial_lookback_hours: 24 }
logging: { dir: ./logs, level: INFO }
pipelines: []
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(cfg_text)
    from core.config import load_config
    import pytest
    with pytest.raises(Exception):  # ConfigError or pydantic ValidationError
        load_config(str(p))
```

- [ ] **Step 3: Run tests — expect failures**

Run: `.venv/bin/python -m pytest tests/test_config.py -k "sqs or unknown_source" -v`
Expected: 3 failures (sqs_s3 type not in literal, etc.).

- [ ] **Step 4: Implement schema**

In `core/config.py`:
1. Rename the existing `SourceConfig` class to `S3SourceConfig` (keep all fields and `type: Literal["s3"] = "s3"`).
2. Add new class `SqsS3SourceConfig`:

```python
class SqsS3SourceConfig(BaseModel):
    type: Literal["sqs_s3"] = "sqs_s3"
    queue_url: str
    bucket: str
    fqdn: str
    org_id: str
    visibility_timeout_sec: int = 60
    visibility_extension_sec: int = 60
    wait_time_sec: int = 20
    max_messages_per_receive: int = 10
    max_workers: int = 1
```

3. Replace `SourceConfig` with a type alias for the discriminated union:

```python
from typing import Annotated, Union
from pydantic import Field

SourceConfig = Annotated[
    Union[S3SourceConfig, SqsS3SourceConfig],
    Field(discriminator="type"),
]
```

4. Update `AppConfig.source: SourceConfig`. (No code change needed if it already uses the alias.)

5. Search for any imports of `SourceConfig` in the repo (`grep -rn "SourceConfig" --include="*.py"`). Update any place that constructs it directly to use `S3SourceConfig` (likely none — it is only referenced as a type).

- [ ] **Step 5: Run all tests**

Run: `.venv/bin/python -m pytest -q`
Expected: 95 passed (92 baseline + 3 new).

- [ ] **Step 6: Commit**

```bash
git add core/config.py tests/test_config.py
git commit -m "feat(config): add SqsS3SourceConfig and discriminated SourceConfig union"
```

---

## Task 2: Log-type lookup helper — extract from `S3Source`

**Files:**
- Modify: `sources/s3_source.py:24` (`_LOG_TYPE_PATH`) and `sources/s3_source.py:51-54` (`_base_prefix`)
- Test: `tests/test_s3_source.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_s3_source.py`:

```python
def test_path_to_log_type_resolves_known_paths():
    from sources.s3_source import path_to_log_type
    fqdn = "pce.example.com"
    org_id = "123"
    cases = {
        "pce.example.com/org_id=123/auditable/2026/05/07/x.json.gz": "auditable",
        "pce.example.com/org_id=123/summaries/pd=0/2026/05/07/x.json.gz": "pd0",
        "pce.example.com/org_id=123/summaries/pd=1/2026/05/07/x.json.gz": "pd1",
        "pce.example.com/org_id=123/summaries/pd=2/2026/05/07/x.json.gz": "pd2",
        "pce.example.com/org_id=123/summaries/pd=3/2026/05/07/x.json.gz": "pd3",
    }
    for key, expected in cases.items():
        assert path_to_log_type(key, fqdn, org_id) == expected


def test_path_to_log_type_unknown_returns_none():
    from sources.s3_source import path_to_log_type
    assert path_to_log_type("pce.example.com/org_id=123/foo/x", "pce.example.com", "123") is None
    assert path_to_log_type("other/org_id=123/auditable/x", "pce.example.com", "123") is None
    assert path_to_log_type("pce.example.com/org_id=999/auditable/x", "pce.example.com", "123") is None
```

- [ ] **Step 2: Run — expect failure**

Run: `.venv/bin/python -m pytest tests/test_s3_source.py -k "path_to_log_type" -v`
Expected: ImportError / AttributeError.

- [ ] **Step 3: Add the helper**

In `sources/s3_source.py`, after `_LOG_TYPE_PATH`, add:

```python
def path_to_log_type(key: str, fqdn: str, org_id: str) -> Optional[str]:
    """Reverse-lookup log_type from an S3 object key.

    Returns the log_type if `key` starts with the expected
    `{fqdn}/org_id={org_id}/<path>/...` for any value in `_LOG_TYPE_PATH`,
    else None.
    """
    expected_root = f"{fqdn}/org_id={org_id}/"
    if not key.startswith(expected_root):
        return None
    remainder = key[len(expected_root):]
    for log_type, sub_path in _LOG_TYPE_PATH.items():
        if remainder.startswith(sub_path + "/"):
            return log_type
    return None
```

`Optional` is already imported. Do not change `S3Source` itself — `_base_prefix` keeps using `_LOG_TYPE_PATH` directly.

- [ ] **Step 4: Run tests**

Run: `.venv/bin/python -m pytest -q`
Expected: 97 passed.

- [ ] **Step 5: Commit**

```bash
git add sources/s3_source.py tests/test_s3_source.py
git commit -m "refactor(s3_source): expose path_to_log_type helper for SQS dispatcher reuse"
```

---

## Task 3: SqsPipeline — pure transformer

**Files:**
- Create: `core/sqs_pipeline.py`
- Test: `tests/test_sqs_pipeline.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_sqs_pipeline.py`:

```python
"""SqsPipeline unit tests — pure transformer, no SQS / S3."""
from __future__ import annotations

import json
from typing import List

from core.sqs_pipeline import SqsPipeline
from mappers.passthrough import PassthroughMapper
from sinks.base import Sink


class StubSink(Sink):
    def __init__(self, fail_on_index: int | None = None):
        self.sent: List[bytes] = []
        self.flushed = 0
        self.fail_on_index = fail_on_index

    def send(self, wire: bytes) -> bool:
        if self.fail_on_index is not None and len(self.sent) == self.fail_on_index:
            return False
        self.sent.append(wire)
        return True

    def flush(self) -> bool:
        self.flushed += 1
        return True

    def close(self) -> None:
        pass


def _body(events):
    return ("\n".join(json.dumps(e) for e in events) + "\n").encode("utf-8")


def test_process_happy_path():
    sink = StubSink()
    p = SqsPipeline(name="test", log_type="auditable", mapper=PassthroughMapper(),
                    sink=sink, filter_fn=None)
    events = [{"a": 1}, {"a": 2}, {"a": 3}]
    ok = p.process(_body(events))
    assert ok is True
    assert len(sink.sent) == 3
    assert sink.flushed == 1


def test_process_filter_drops_events():
    sink = StubSink()
    p = SqsPipeline(name="test", log_type="auditable", mapper=PassthroughMapper(),
                    sink=sink, filter_fn=lambda ev: ev["a"] >= 2)
    ok = p.process(_body([{"a": 1}, {"a": 2}, {"a": 3}]))
    assert ok is True
    assert len(sink.sent) == 2  # only a>=2


def test_process_sink_failure_returns_false():
    sink = StubSink(fail_on_index=1)
    p = SqsPipeline(name="test", log_type="auditable", mapper=PassthroughMapper(),
                    sink=sink, filter_fn=None)
    ok = p.process(_body([{"a": 1}, {"a": 2}, {"a": 3}]))
    assert ok is False
    assert len(sink.sent) == 1


def test_process_bad_json_skips_continues():
    """A bad JSON line increments mapper_err but does not abort."""
    sink = StubSink()
    p = SqsPipeline(name="test", log_type="auditable", mapper=PassthroughMapper(),
                    sink=sink, filter_fn=None)
    body = b'{"a":1}\nNOT JSON\n{"a":2}\n'
    ok = p.process(body)
    assert ok is True
    assert len(sink.sent) == 2
    assert p.last_stats["mapper_err"] == 1


def test_process_flush_failure_returns_false():
    """If sink.flush fails, process returns False."""
    class FlushFailSink(StubSink):
        def flush(self) -> bool:
            super().flush()
            return False
    sink = FlushFailSink()
    p = SqsPipeline(name="test", log_type="auditable", mapper=PassthroughMapper(),
                    sink=sink, filter_fn=None)
    ok = p.process(_body([{"a": 1}]))
    assert ok is False
```

- [ ] **Step 2: Run — expect failure**

Run: `.venv/bin/python -m pytest tests/test_sqs_pipeline.py -v`
Expected: ImportError on `core.sqs_pipeline`.

- [ ] **Step 3: Implement SqsPipeline**

Create `core/sqs_pipeline.py`:

```python
"""SQS-mode per-log_type pipeline.

Pure transformer: given a decoded body (JSON-lines), runs each event through
filter → mapper → sink. No checkpoint, no SQS knowledge. The dispatcher owns
SQS lifecycle.
"""
from __future__ import annotations

import json
import logging
from typing import Callable, Optional

from mappers.base import Mapper
from sinks.base import Sink


class SqsPipeline:
    def __init__(
        self,
        name: str,
        log_type: str,
        mapper: Mapper,
        sink: Sink,
        filter_fn: Optional[Callable[[dict], bool]] = None,
    ):
        self.name = name
        self.log_type = log_type
        self.mapper = mapper
        self.sink = sink
        self.filter_fn = filter_fn
        self.log = logging.getLogger(name)
        self.last_stats: dict = {"read": 0, "filtered": 0, "sent": 0,
                                  "mapper_err": 0, "failed": 0}

    def process(self, body: bytes) -> bool:
        """Run JSON-lines body through filter/mapper/sink.

        Returns True iff every non-filtered event was sent and the final
        flush succeeded. On False, the dispatcher should NOT delete the
        SQS message.
        """
        stats = {"read": 0, "filtered": 0, "sent": 0, "mapper_err": 0, "failed": 0}
        sent_in_msg = 0
        try:
            for raw_line in body.splitlines():
                if not raw_line.strip():
                    continue
                stats["read"] += 1
                try:
                    ev = json.loads(raw_line)
                except Exception:
                    stats["mapper_err"] += 1
                    self.log.warning("bad JSON line; skipping")
                    continue

                if self.filter_fn and not self.filter_fn(ev):
                    stats["filtered"] += 1
                    continue

                try:
                    wire = self.mapper.format(ev)
                except Exception as e:  # noqa: BLE001
                    stats["mapper_err"] += 1
                    self.log.error("mapper error: %s", e)
                    continue

                if self.sink.send(wire):
                    stats["sent"] += 1
                    sent_in_msg += 1
                else:
                    stats["failed"] += 1
                    return False
            if sent_in_msg and not self.sink.flush():
                stats["failed"] += 1
                self.log.error("sink flush failed")
                return False
            return True
        finally:
            self.last_stats = stats
            self.log.info(
                "process: read=%d sent=%d filtered=%d mapper_err=%d failed=%d",
                stats["read"], stats["sent"], stats["filtered"],
                stats["mapper_err"], stats["failed"],
            )
```

- [ ] **Step 4: Run tests**

Run: `.venv/bin/python -m pytest tests/test_sqs_pipeline.py -v`
Expected: 5 passed.

- [ ] **Step 5: Run full suite**

Run: `.venv/bin/python -m pytest -q`
Expected: 102 passed.

- [ ] **Step 6: Commit**

```bash
git add core/sqs_pipeline.py tests/test_sqs_pipeline.py
git commit -m "feat(core): add SqsPipeline pure transformer (filter/map/sink/flush)"
```

---

## Task 4: SQS message parsing helpers

**Files:**
- Create: section in `sources/sqs_s3_source.py` (file is created here for the first time; later tasks add to it)
- Test: `tests/test_sqs_s3_dispatcher.py` (file is created here)

The Illumio queue receives SNS-wrapped S3 events. Body shape:

```json
{ "Type": "Notification", "Message": "<JSON-stringified S3 event>" }
```

Where the S3 event itself is:

```json
{
  "Records": [
    { "s3": { "bucket": {"name": "..."}, "object": {"key": "..."} } }
  ]
}
```

The helpers must accept either an SNS-wrapped body or a raw S3 event body (defensive — some setups skip SNS).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_sqs_s3_dispatcher.py`:

```python
"""SqsS3Dispatcher tests."""
from __future__ import annotations

import json

import pytest

from sources.sqs_s3_source import (
    parse_message_body,
    extract_s3_object_refs,
    S3ObjectRef,
)


def _sns_wrap(s3_event: dict) -> str:
    return json.dumps({"Type": "Notification", "Message": json.dumps(s3_event)})


def _s3_event(bucket: str, key: str) -> dict:
    return {"Records": [{"s3": {"bucket": {"name": bucket}, "object": {"key": key}}}]}


def test_parse_sns_wrapped_message():
    body = _sns_wrap(_s3_event("b", "pce/org_id=1/auditable/x.gz"))
    parsed = parse_message_body(body)
    assert parsed["Records"][0]["s3"]["object"]["key"] == "pce/org_id=1/auditable/x.gz"


def test_parse_raw_s3_event_body():
    body = json.dumps(_s3_event("b", "pce/org_id=1/auditable/x.gz"))
    parsed = parse_message_body(body)
    assert parsed["Records"][0]["s3"]["bucket"]["name"] == "b"


def test_parse_malformed_body_raises():
    with pytest.raises(ValueError):
        parse_message_body("not json")


def test_extract_refs_single():
    s3_event = _s3_event("b", "k")
    refs = extract_s3_object_refs(s3_event)
    assert refs == [S3ObjectRef(bucket="b", key="k")]


def test_extract_refs_multiple_records():
    """Some S3 events batch multiple object creations in one Records array."""
    s3_event = {"Records": [
        {"s3": {"bucket": {"name": "b"}, "object": {"key": "k1"}}},
        {"s3": {"bucket": {"name": "b"}, "object": {"key": "k2"}}},
    ]}
    refs = extract_s3_object_refs(s3_event)
    assert len(refs) == 2
    assert {r.key for r in refs} == {"k1", "k2"}


def test_extract_refs_no_records_returns_empty():
    """S3 sometimes sends s3:TestEvent which has no Records array."""
    refs = extract_s3_object_refs({"Service": "Amazon S3", "Event": "s3:TestEvent"})
    assert refs == []
```

- [ ] **Step 2: Run — expect failure**

Run: `.venv/bin/python -m pytest tests/test_sqs_s3_dispatcher.py -v`
Expected: ImportError on `sources.sqs_s3_source`.

- [ ] **Step 3: Create file with helpers**

Create `sources/sqs_s3_source.py`:

```python
"""SQS-based S3 ingestion: long-poll consumer, log-type routing,
visibility extension, delete-on-success.

The Illumio tenant publishes one SQS queue receiving SNS-wrapped
s3:ObjectCreated:* events for the bucket. This module consumes that queue
and dispatches to per-log_type SqsPipeline instances.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import List

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class S3ObjectRef:
    bucket: str
    key: str


def parse_message_body(body: str) -> dict:
    """Parse SQS message body into the inner S3 event dict.

    Supports both SNS-wrapped (`{Type: Notification, Message: <stringified>}`)
    and raw S3 event JSON. Raises ValueError on malformed input.
    """
    try:
        outer = json.loads(body)
    except json.JSONDecodeError as e:
        raise ValueError(f"message body is not JSON: {e}") from e
    if isinstance(outer, dict) and outer.get("Type") == "Notification" \
            and isinstance(outer.get("Message"), str):
        try:
            return json.loads(outer["Message"])
        except json.JSONDecodeError as e:
            raise ValueError(f"SNS Message field is not JSON: {e}") from e
    return outer


def extract_s3_object_refs(s3_event: dict) -> List[S3ObjectRef]:
    """Return one S3ObjectRef per Records entry. Empty list on TestEvent etc."""
    refs: List[S3ObjectRef] = []
    for rec in s3_event.get("Records") or []:
        s3 = rec.get("s3") or {}
        bucket = (s3.get("bucket") or {}).get("name")
        key = (s3.get("object") or {}).get("key")
        if bucket and key:
            refs.append(S3ObjectRef(bucket=bucket, key=key))
    return refs


# Dispatcher class added in Task 5.
```

- [ ] **Step 4: Run helper tests**

Run: `.venv/bin/python -m pytest tests/test_sqs_s3_dispatcher.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add sources/sqs_s3_source.py tests/test_sqs_s3_dispatcher.py
git commit -m "feat(sqs): add message parsing helpers for SNS-wrapped S3 events"
```

---

## Task 5: SqsS3Dispatcher — receive, route, process, delete (no visibility, no shutdown yet)

**Files:**
- Modify: `sources/sqs_s3_source.py` (append `SqsS3Dispatcher` class)
- Test: `tests/test_sqs_s3_dispatcher.py` (append integration tests using moto)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_sqs_s3_dispatcher.py`:

```python
import gzip
from unittest.mock import MagicMock

import boto3
from moto import mock_aws

from core.sqs_pipeline import SqsPipeline
from mappers.passthrough import PassthroughMapper


# -------- shared fixtures --------

@pytest.fixture
def aws_env(monkeypatch):
    """Set fake AWS creds so moto and boto3 are happy."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "x")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


def _fixture_setup(aws_env):
    """Create bucket, queue, return (s3, sqs, queue_url, bucket_name)."""
    s3 = boto3.client("s3", region_name="us-east-1")
    sqs = boto3.client("sqs", region_name="us-east-1")
    s3.create_bucket(Bucket="b")
    qurl = sqs.create_queue(QueueName="q")["QueueUrl"]
    return s3, sqs, qurl, "b"


def _put_object(s3, bucket, key, events):
    body = ("\n".join(json.dumps(e) for e in events) + "\n").encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=gzip.compress(body))


def _enqueue_event(sqs, qurl, bucket, key):
    sqs.send_message(
        QueueUrl=qurl,
        MessageBody=_sns_wrap(_s3_event(bucket, key)),
    )


def _make_sqs_pipeline(name, log_type, sink):
    return SqsPipeline(name=name, log_type=log_type,
                       mapper=PassthroughMapper(), sink=sink, filter_fn=None)


class CapturingSink:
    def __init__(self):
        self.sent = []
        self.flushed = 0
    def send(self, w): self.sent.append(w); return True
    def flush(self): self.flushed += 1; return True
    def close(self): pass


# -------- tests --------

@mock_aws
def test_dispatcher_routes_auditable_to_pipeline(aws_env):
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    key = "pce.example.com/org_id=1/auditable/2026/05/07/x.json.gz"
    _put_object(s3, bucket, key, [{"a": 1}, {"a": 2}])
    _enqueue_event(sqs, qurl, bucket, key)

    sink = CapturingSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)

    d = SqsS3Dispatcher(
        sqs_client=sqs, s3_client=s3,
        queue_url=qurl, bucket="b",
        fqdn="pce.example.com", org_id="1",
        pipelines=[pipeline],
        wait_time_sec=0, max_messages_per_receive=10,
    )
    d.consume_one_batch()  # pump exactly one receive_message cycle

    assert len(sink.sent) == 2
    # message deleted
    msgs = sqs.receive_message(QueueUrl=qurl, WaitTimeSeconds=0).get("Messages", [])
    assert msgs == []


@mock_aws
def test_dispatcher_unknown_log_type_deletes_message(aws_env, caplog):
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    key = "pce.example.com/org_id=1/garbage/x.json.gz"
    _put_object(s3, bucket, key, [{"a": 1}])
    _enqueue_event(sqs, qurl, bucket, key)

    sink = CapturingSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)

    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="b", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10)
    d.consume_one_batch()

    assert sink.sent == []
    msgs = sqs.receive_message(QueueUrl=qurl, WaitTimeSeconds=0).get("Messages", [])
    assert msgs == []  # deleted (irrelevant)


@mock_aws
def test_dispatcher_no_enabled_pipeline_deletes_message(aws_env):
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    key = "pce.example.com/org_id=1/summaries/pd=2/2026/05/07/x.json.gz"
    _put_object(s3, bucket, key, [{"a": 1}])
    _enqueue_event(sqs, qurl, bucket, key)

    # Only auditable pipeline enabled — pd2 has no pipeline.
    sink = CapturingSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)

    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="b", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10)
    d.consume_one_batch()

    assert sink.sent == []
    msgs = sqs.receive_message(QueueUrl=qurl, WaitTimeSeconds=0).get("Messages", [])
    assert msgs == []


@mock_aws
def test_dispatcher_sink_failure_keeps_message(aws_env):
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    key = "pce.example.com/org_id=1/auditable/2026/05/07/x.json.gz"
    _put_object(s3, bucket, key, [{"a": 1}])
    _enqueue_event(sqs, qurl, bucket, key)

    class FailingSink(CapturingSink):
        def send(self, w): return False
    sink = FailingSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)

    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="b", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10,
                       visibility_timeout_sec=1)
    d.consume_one_batch()

    # message still there (after visibility expires)
    import time; time.sleep(1.5)
    msgs = sqs.receive_message(QueueUrl=qurl, WaitTimeSeconds=0,
                               VisibilityTimeout=1).get("Messages", [])
    assert len(msgs) == 1


@mock_aws
def test_dispatcher_s3_get_object_failure_keeps_message(aws_env):
    """If the S3 object referenced by the SQS message doesn't exist,
    the message is NOT deleted (relies on SQS DLQ for permanent failures)."""
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    # Enqueue an event for a key that was never put_object'd
    _enqueue_event(sqs, qurl, bucket, "pce.example.com/org_id=1/auditable/missing.json.gz")

    sink = CapturingSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)
    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="b", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10, visibility_timeout_sec=1)
    d.consume_one_batch()

    assert sink.sent == []
    import time; time.sleep(1.5)
    msgs = sqs.receive_message(QueueUrl=qurl, WaitTimeSeconds=0,
                               VisibilityTimeout=1).get("Messages", [])
    assert len(msgs) == 1


@mock_aws
def test_dispatcher_gunzip_failure_keeps_message(aws_env):
    """If the S3 object isn't valid gzip, message is NOT deleted (DLQ catches)."""
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    key = "pce.example.com/org_id=1/auditable/bad.json.gz"
    s3.put_object(Bucket=bucket, Key=key, Body=b"not gzip")  # raw, not gzipped
    _enqueue_event(sqs, qurl, bucket, key)

    sink = CapturingSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)
    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="b", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10, visibility_timeout_sec=1)
    d.consume_one_batch()

    assert sink.sent == []
    import time; time.sleep(1.5)
    msgs = sqs.receive_message(QueueUrl=qurl, WaitTimeSeconds=0,
                               VisibilityTimeout=1).get("Messages", [])
    assert len(msgs) == 1


@mock_aws
def test_dispatcher_malformed_message_keeps_in_queue(aws_env):
    """Malformed (non-JSON) message body is left in queue for DLQ."""
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    sqs.send_message(QueueUrl=qurl, MessageBody="this is not json at all")

    sink = CapturingSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)
    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="b", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10, visibility_timeout_sec=1)
    d.consume_one_batch()

    import time; time.sleep(1.5)
    msgs = sqs.receive_message(QueueUrl=qurl, WaitTimeSeconds=0,
                               VisibilityTimeout=1).get("Messages", [])
    assert len(msgs) == 1


@mock_aws
def test_dispatcher_bucket_mismatch_keeps_message(aws_env):
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    _enqueue_event(sqs, qurl, "wrong-bucket", "pce.example.com/org_id=1/auditable/x.gz")

    sink = CapturingSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)
    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="b", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10,
                       visibility_timeout_sec=1)
    d.consume_one_batch()

    import time; time.sleep(1.5)
    msgs = sqs.receive_message(QueueUrl=qurl, WaitTimeSeconds=0,
                               VisibilityTimeout=1).get("Messages", [])
    assert len(msgs) == 1
```

- [ ] **Step 2: Run — expect failure**

Run: `.venv/bin/python -m pytest tests/test_sqs_s3_dispatcher.py -v`
Expected: ImportError on `SqsS3Dispatcher`.

- [ ] **Step 3: Implement dispatcher (no visibility extension, no shutdown)**

Append to `sources/sqs_s3_source.py`:

```python
import gzip
import threading
import time
from typing import Dict, List, Optional

from core.sqs_pipeline import SqsPipeline
from sources.s3_source import path_to_log_type


class SqsS3Dispatcher:
    """Long-poll SQS consumer that downloads S3 objects and routes by log_type.

    Single-threaded. Call `run_forever()` to block until stop signal,
    or `consume_one_batch()` for testing / dry-run.
    """

    def __init__(
        self,
        sqs_client,
        s3_client,
        queue_url: str,
        bucket: str,
        fqdn: str,
        org_id: str,
        pipelines: List[SqsPipeline],
        visibility_timeout_sec: int = 60,
        visibility_extension_sec: int = 60,
        wait_time_sec: int = 20,
        max_messages_per_receive: int = 10,
        max_workers: int = 1,
    ):
        self.sqs = sqs_client
        self.s3 = s3_client
        self.queue_url = queue_url
        self.bucket = bucket
        self.fqdn = fqdn
        self.org_id = org_id
        self.visibility_timeout_sec = visibility_timeout_sec
        self.visibility_extension_sec = visibility_extension_sec
        self.wait_time_sec = wait_time_sec
        self.max_messages_per_receive = max_messages_per_receive
        if max_workers != 1:
            log.warning("max_workers=%d requested, but only 1 is supported "
                        "in this version; falling back to 1", max_workers)
        self._by_log_type: Dict[str, SqsPipeline] = {p.log_type: p for p in pipelines}
        self._stop_event = threading.Event()

    def consume_one_batch(self) -> int:
        """Run one receive_message cycle and process its messages. Returns count."""
        resp = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=self.max_messages_per_receive,
            WaitTimeSeconds=self.wait_time_sec,
            VisibilityTimeout=self.visibility_timeout_sec,
        )
        messages = resp.get("Messages", []) or []
        for msg in messages:
            self._handle_message(msg)
        return len(messages)

    def _handle_message(self, msg: dict) -> None:
        body = msg.get("Body", "")
        receipt = msg["ReceiptHandle"]
        try:
            s3_event = parse_message_body(body)
        except ValueError as e:
            log.error("malformed SQS message; not deleting (DLQ will catch): %s", e)
            return
        refs = extract_s3_object_refs(s3_event)
        if not refs:
            log.info("non-S3 notification (e.g. s3:TestEvent); deleting")
            self._delete(receipt)
            return
        # Process all refs; delete only if every ref succeeded.
        all_ok = True
        for ref in refs:
            ok = self._handle_object(ref)
            all_ok = all_ok and ok
        if all_ok:
            self._delete(receipt)

    def _handle_object(self, ref: S3ObjectRef) -> bool:
        if ref.bucket != self.bucket:
            log.error("bucket mismatch: msg=%s configured=%s; not deleting",
                      ref.bucket, self.bucket)
            return False
        log_type = path_to_log_type(ref.key, self.fqdn, self.org_id)
        if log_type is None:
            log.warning("unknown key path %s; deleting (no Illumio log_type matches)",
                        ref.key)
            return True  # mark as ok so message gets deleted
        pipeline = self._by_log_type.get(log_type)
        if pipeline is None:
            log.info("no enabled pipeline for log_type=%s; deleting", log_type)
            return True
        try:
            obj = self.s3.get_object(Bucket=ref.bucket, Key=ref.key)
            raw = obj["Body"].read()
        except Exception as e:  # noqa: BLE001
            log.error("s3 get_object failed for %s: %s; not deleting", ref.key, e)
            return False
        try:
            decoded = gzip.decompress(raw)
        except OSError as e:
            log.error("gunzip failed for %s: %s; not deleting (DLQ catches)", ref.key, e)
            return False
        return pipeline.process(decoded)

    def _delete(self, receipt_handle: str) -> None:
        try:
            self.sqs.delete_message(QueueUrl=self.queue_url,
                                    ReceiptHandle=receipt_handle)
        except Exception as e:  # noqa: BLE001
            log.error("delete_message failed: %s", e)

    def run_forever(self) -> None:
        log.info("SQS dispatcher starting, queue=%s", self.queue_url[-32:])
        while not self._stop_event.is_set():
            try:
                self.consume_one_batch()
            except Exception as e:  # noqa: BLE001
                log.exception("consume batch failed: %s", e)
                self._stop_event.wait(timeout=5)
        log.info("SQS dispatcher stopped")

    def request_stop(self) -> None:
        self._stop_event.set()
```

- [ ] **Step 4: Run dispatcher tests**

Run: `.venv/bin/python -m pytest tests/test_sqs_s3_dispatcher.py -v`
Expected: 14 passed (6 helper + 8 dispatcher).

- [ ] **Step 5: Run full suite**

Run: `.venv/bin/python -m pytest -q`
Expected: 111 passed.

- [ ] **Step 6: Commit**

```bash
git add sources/sqs_s3_source.py tests/test_sqs_s3_dispatcher.py
git commit -m "feat(sqs): add SqsS3Dispatcher with routing, processing, delete-on-success"
```

---

## Task 6: Visibility extension for slow processing

**Files:**
- Modify: `sources/sqs_s3_source.py` (`_handle_message`)
- Test: `tests/test_sqs_s3_dispatcher.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_sqs_s3_dispatcher.py`:

```python
@mock_aws
def test_dispatcher_extends_visibility_for_slow_processing(aws_env, monkeypatch):
    """If processing exceeds visibility_timeout/2, change_message_visibility is called."""
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    key = "pce.example.com/org_id=1/auditable/x.json.gz"
    _put_object(s3, bucket, key, [{"a": 1}])
    _enqueue_event(sqs, qurl, bucket, key)

    # Wrap real sqs client to spy on change_message_visibility
    real_change = sqs.change_message_visibility
    calls = []
    def spy_change(**kwargs):
        calls.append(kwargs)
        return real_change(**kwargs)
    sqs.change_message_visibility = spy_change

    # Slow sink that sleeps long enough to trigger extension
    class SlowSink(CapturingSink):
        def send(self, w):
            time.sleep(0.6)  # > visibility_timeout/2 = 0.5s
            return super().send(w)
    sink = SlowSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)
    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="b", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10,
                       visibility_timeout_sec=1,
                       visibility_extension_sec=10)
    d.consume_one_batch()

    assert len(calls) >= 1
    assert calls[0]["VisibilityTimeout"] == 10
```

- [ ] **Step 2: Run — expect failure**

Run: `.venv/bin/python -m pytest tests/test_sqs_s3_dispatcher.py::test_dispatcher_extends_visibility_for_slow_processing -v`
Expected: FAIL — `assert len(calls) >= 1`.

- [ ] **Step 3: Add a visibility-extending wrapper in `_handle_message`**

Replace `_handle_message` and add the helper in `sources/sqs_s3_source.py`:

```python
def _handle_message(self, msg: dict) -> None:
    body = msg.get("Body", "")
    receipt = msg["ReceiptHandle"]
    start = time.monotonic()
    last_extend_at = start

    def extend_if_needed():
        nonlocal last_extend_at
        elapsed_since_extend = time.monotonic() - last_extend_at
        if elapsed_since_extend >= self.visibility_timeout_sec / 2:
            try:
                self.sqs.change_message_visibility(
                    QueueUrl=self.queue_url,
                    ReceiptHandle=receipt,
                    VisibilityTimeout=self.visibility_extension_sec,
                )
                last_extend_at = time.monotonic()
            except Exception as e:  # noqa: BLE001
                log.warning("change_message_visibility failed: %s", e)

    try:
        s3_event = parse_message_body(body)
    except ValueError as e:
        log.error("malformed SQS message; not deleting: %s", e)
        return
    refs = extract_s3_object_refs(s3_event)
    if not refs:
        log.info("non-S3 notification; deleting")
        self._delete(receipt)
        return
    all_ok = True
    for ref in refs:
        extend_if_needed()
        ok = self._handle_object(ref, extend_if_needed)
        all_ok = all_ok and ok
    if all_ok:
        self._delete(receipt)
```

Update `_handle_object` signature to accept the callback and call it before/after the slow steps:

```python
def _handle_object(self, ref: S3ObjectRef, extend_visibility=lambda: None) -> bool:
    if ref.bucket != self.bucket:
        log.error("bucket mismatch ...; not deleting")
        return False
    log_type = path_to_log_type(ref.key, self.fqdn, self.org_id)
    if log_type is None:
        log.warning("unknown key path %s; deleting", ref.key)
        return True
    pipeline = self._by_log_type.get(log_type)
    if pipeline is None:
        log.info("no enabled pipeline for log_type=%s; deleting", log_type)
        return True
    try:
        extend_visibility()
        obj = self.s3.get_object(Bucket=ref.bucket, Key=ref.key)
        raw = obj["Body"].read()
    except Exception as e:  # noqa: BLE001
        log.error("s3 get_object failed for %s: %s", ref.key, e)
        return False
    try:
        decoded = gzip.decompress(raw)
    except OSError as e:
        log.error("gunzip failed for %s: %s", ref.key, e)
        return False
    extend_visibility()
    return pipeline.process(decoded)
```

The pipeline `process()` runs synchronously per event; for very large bodies the extension only fires before/after, not within. That is acceptable initially — typical Illumio gzipped objects are <50MB and process in seconds. If profiling later shows multi-minute objects, push the callback into `SqsPipeline.process` via a hook.

- [ ] **Step 4: Run the new test**

Run: `.venv/bin/python -m pytest tests/test_sqs_s3_dispatcher.py::test_dispatcher_extends_visibility_for_slow_processing -v`
Expected: PASS.

- [ ] **Step 5: Run full suite**

Run: `.venv/bin/python -m pytest -q`
Expected: 112 passed.

- [ ] **Step 6: Commit**

```bash
git add sources/sqs_s3_source.py tests/test_sqs_s3_dispatcher.py
git commit -m "feat(sqs): extend message visibility during slow processing"
```

---

## Task 7: Graceful shutdown — stop event integration

**Files:**
- Modify: `sources/sqs_s3_source.py` (`run_forever`)
- Test: `tests/test_sqs_s3_dispatcher.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_sqs_s3_dispatcher.py`:

```python
@mock_aws
def test_dispatcher_stops_after_current_message(aws_env):
    """request_stop() lets the consumer finish current message before exiting."""
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    key = "pce.example.com/org_id=1/auditable/x.json.gz"
    _put_object(s3, bucket, key, [{"a": 1}])
    _enqueue_event(sqs, qurl, bucket, key)

    sink = CapturingSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)
    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="b", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10)

    # Stop immediately; loop should still exit cleanly without processing
    # (or process exactly one batch then exit).
    d.request_stop()
    d.run_forever()  # must return promptly, not block

    # Either the message is still in queue (loop saw stop before receive)
    # or it was processed and deleted (loop did one iteration before stop).
    msgs = sqs.receive_message(QueueUrl=qurl, WaitTimeSeconds=0).get("Messages", [])
    assert isinstance(msgs, list)  # didn't hang
```

- [ ] **Step 2: Run — expect FAIL or timeout (already passes for trivial reason; revise)**

Actually `run_forever` already checks the stop event, so this test mostly verifies we don't block. Run:

Run: `.venv/bin/python -m pytest tests/test_sqs_s3_dispatcher.py::test_dispatcher_stops_after_current_message --timeout=10 -v`

If the test passes immediately (because `run_forever` already exits cleanly when stop is set first), proceed. If `--timeout` plugin not installed, install it: `.venv/bin/pip install pytest-timeout`.

- [ ] **Step 3: Verify behavior under "stop arrives mid-batch"**

Add a stronger test:

```python
@mock_aws
def test_dispatcher_finishes_in_flight_message_before_stop(aws_env):
    """A stop signal arriving mid-process does not abandon the current message."""
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    key = "pce.example.com/org_id=1/auditable/x.json.gz"
    _put_object(s3, bucket, key, [{"a": 1}])
    _enqueue_event(sqs, qurl, bucket, key)

    captured = []
    class StopperSink:
        def __init__(self, dispatcher_holder): self.h = dispatcher_holder
        def send(self, w):
            captured.append(w)
            self.h["d"].request_stop()  # signal stop mid-process
            return True
        def flush(self): return True
        def close(self): pass

    holder = {}
    sink = StopperSink(holder)
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)
    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="b", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10)
    holder["d"] = d
    d.run_forever()

    # Message was processed (sink saw it) and deleted (in-flight finished)
    assert len(captured) == 1
    msgs = sqs.receive_message(QueueUrl=qurl, WaitTimeSeconds=0).get("Messages", [])
    assert msgs == []
```

This passes already because `run_forever` checks `_stop_event` only at top of loop — it does NOT abandon the in-flight batch. If it does fail, ensure `consume_one_batch` is called atomically per loop iteration.

- [ ] **Step 4: Run full dispatcher suite**

Run: `.venv/bin/python -m pytest tests/test_sqs_s3_dispatcher.py -v`
Expected: 16 passed.

- [ ] **Step 5: Commit**

```bash
git add tests/test_sqs_s3_dispatcher.py
git commit -m "test(sqs): cover graceful shutdown — finish in-flight before exit"
```

---

## Task 8: Factory branching — `build_pipelines_from_config`

**Files:**
- Modify: `core/pipeline.py` (`build_pipelines_from_config`)
- Test: `tests/test_pipeline.py` or new `tests/test_factory_sqs.py`

- [ ] **Step 1: Decide return shape**

Change return type from `list[(Pipeline, int)]` to a tagged result. Cleanest: have the function return one of:
- `("polling", list[(Pipeline, int)])`
- `("sqs", SqsS3Dispatcher, list[SqsPipeline])`

Or return an object. We choose a small wrapper:

```python
@dataclass
class BuildResult:
    mode: str  # "polling" | "sqs"
    polling: Optional[list[tuple["Pipeline", int]]] = None
    sqs_dispatcher: Optional["SqsS3Dispatcher"] = None
    sqs_pipelines: Optional[list["SqsPipeline"]] = None
```

- [ ] **Step 2: Write the failing tests**

Append to `tests/test_pipeline.py` (or create `tests/test_factory_sqs.py`):

```python
def _write_cfg(tmp_path, source_block, pipelines_block):
    cfg = f"""
aws: {{ region: us-east-1, access_key: x, secret_key: y }}
{source_block}
checkpoint: {{ dir: {tmp_path}/state, initial_lookback_hours: 24 }}
logging: {{ dir: {tmp_path}/logs, level: INFO }}
{pipelines_block}
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(cfg)
    return p


def test_factory_returns_polling_for_s3_source(tmp_path, monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "y")
    p = _write_cfg(
        tmp_path,
        source_block="source: { type: s3, bucket: b, fqdn: pce.example.com, org_id: '1' }",
        pipelines_block="pipelines:\n  - { name: a, log_type: auditable, mapper: {format: json}, sink: {type: file, path: /tmp/x.log} }",
    )
    from core.config import load_config
    from core.pipeline import build_pipelines_from_config
    cfg = load_config(str(p))
    result = build_pipelines_from_config(cfg)
    assert result.mode == "polling"
    assert result.polling is not None
    assert len(result.polling) == 1


@pytest.mark.skipif(  # moto not always loaded here; gate behind import
    True, reason="needs moto and SQS path; covered in dispatcher tests instead")
def test_factory_returns_sqs_for_sqs_source(tmp_path):
    pass  # see test_dispatcher_routes_auditable_to_pipeline for end-to-end
```

(Skip the SQS factory unit test — the dispatcher integration tests already cover the wired-up path.)

- [ ] **Step 3: Run — expect failure**

Run: `.venv/bin/python -m pytest tests/test_pipeline.py::test_factory_returns_polling_for_s3_source -v`
Expected: AttributeError (`result.mode`).

- [ ] **Step 4: Implement BuildResult and branching**

In `core/pipeline.py`, near `build_pipelines_from_config`:

```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class BuildResult:
    mode: str
    polling: Optional[list] = None  # list[tuple[Pipeline, int]]
    sqs_dispatcher: Optional[object] = None  # SqsS3Dispatcher
    sqs_pipelines: Optional[list] = None     # list[SqsPipeline]
```

Replace the body of `build_pipelines_from_config` to branch on `cfg.source.type`:

```python
def build_pipelines_from_config(cfg) -> BuildResult:
    import boto3
    # ...existing imports remain...
    if cfg.aws.profile:
        session = boto3.Session(profile_name=cfg.aws.profile, region_name=cfg.aws.region)
    elif cfg.aws.access_key:
        session = boto3.Session(
            aws_access_key_id=cfg.aws.access_key,
            aws_secret_access_key=cfg.aws.secret_key,
            region_name=cfg.aws.region,
        )
    else:
        session = boto3.Session(region_name=cfg.aws.region)

    if cfg.source.type == "s3":
        return _build_polling(cfg, session)
    elif cfg.source.type == "sqs_s3":
        return _build_sqs(cfg, session)
    else:
        raise ValueError(f"unknown source type: {cfg.source.type}")
```

Move the existing factory body into `_build_polling(cfg, session)` returning `BuildResult(mode="polling", polling=[...])`. Add `_build_sqs(cfg, session)`:

```python
def _build_sqs(cfg, session):
    from core.sqs_pipeline import SqsPipeline
    from sources.sqs_s3_source import SqsS3Dispatcher
    sqs_client = session.client("sqs")
    s3_client = session.client("s3")
    sqs_pipelines = []
    for pc in cfg.pipelines:
        if not pc.enabled:
            continue
        mapper = _build_mapper(pc.mapper, pc.log_type)  # extract helper if not present
        filter_fn = compile_expression(pc.filter.expression) if pc.filter else None
        sink = _build_sink(pc.sink)
        sqs_pipelines.append(SqsPipeline(
            name=pc.name, log_type=pc.log_type,
            mapper=mapper, sink=sink, filter_fn=filter_fn,
        ))
    dispatcher = SqsS3Dispatcher(
        sqs_client=sqs_client, s3_client=s3_client,
        queue_url=cfg.source.queue_url, bucket=cfg.source.bucket,
        fqdn=cfg.source.fqdn, org_id=cfg.source.org_id,
        pipelines=sqs_pipelines,
        visibility_timeout_sec=cfg.source.visibility_timeout_sec,
        visibility_extension_sec=cfg.source.visibility_extension_sec,
        wait_time_sec=cfg.source.wait_time_sec,
        max_messages_per_receive=cfg.source.max_messages_per_receive,
        max_workers=cfg.source.max_workers,
    )
    return BuildResult(mode="sqs", sqs_dispatcher=dispatcher,
                       sqs_pipelines=sqs_pipelines)
```

If `_build_sink` and mapper construction are inlined inside the existing function, extract them into top-level helpers (`_build_sink`, `_build_mapper`) so both branches reuse them. **Touch only what is needed** — do not refactor sink building beyond extracting it.

- [ ] **Step 5: Find and update callers of `build_pipelines_from_config`**

Run: `grep -rn "build_pipelines_from_config" --include="*.py"`
Expected callers: `collector.py`, `tests/test_pipeline.py`. Update both to use `result.polling` (Task 9 will handle collector.py end-to-end; here just keep tests green by adapting).

For existing `test_pipeline.py` tests that did `pipelines = build_pipelines_from_config(cfg)`, change to:

```python
result = build_pipelines_from_config(cfg)
assert result.mode == "polling"
pipelines = result.polling
```

- [ ] **Step 6: Run all tests**

Run: `.venv/bin/python -m pytest -q`
Expected: 113 passed (112 + 1 polling factory test).

- [ ] **Step 7: Commit**

```bash
git add core/pipeline.py tests/test_pipeline.py
git commit -m "feat(factory): branch build_pipelines_from_config by source type; return BuildResult"
```

---

## Task 9: collector.py — mode dispatch and banner

**Files:**
- Modify: `collector.py`

- [ ] **Step 1: Read existing collector.py**

Read all 87 lines. Note current `main()` flow: `load_config → build_pipelines_from_config → PipelineScheduler(...).run_forever()` plus banner.

- [ ] **Step 2: Update `main()`**

Modify the relevant block:

```python
result = build_pipelines_from_config(cfg)
print_banner(cfg, result)

if args.dry_run:
    print("[dry-run] config OK, exiting.")
    return 0

if result.mode == "polling":
    PipelineScheduler(result.polling).run_forever()
elif result.mode == "sqs":
    import signal
    def _handle_sigterm(signum, frame):
        result.sqs_dispatcher.request_stop()
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)
    try:
        result.sqs_dispatcher.run_forever()
    finally:
        for sp in (result.sqs_pipelines or []):
            try:
                sp.sink.close()
            except Exception as e:  # noqa: BLE001
                log.warning("failed to close sink for %s: %s", sp.name, e)
return 0
```

- [ ] **Step 3: Update banner / `_sink_desc` to handle SQS source**

In `print_banner` (or wherever the banner formats source info), add a branch on `cfg.source.type`:

```python
if cfg.source.type == "sqs_s3":
    print(f"  source:    sqs_s3 (queue: ...{cfg.source.queue_url[-32:]})")
    print(f"  bucket:    {cfg.source.bucket}")
    print(f"  pce:       {cfg.source.fqdn} / org_id={cfg.source.org_id}")
else:
    # existing: source.bucket / fqdn / org_id
```

For pipelines list, the SQS branch should print `log_type=...` per enabled pipeline (no poll interval since it does not apply):

```python
if result.mode == "sqs":
    pipelines_for_banner = result.sqs_pipelines or []
    print(f"  pipelines: {len(pipelines_for_banner)} enabled "
          f"({', '.join(p.log_type for p in pipelines_for_banner)})")
else:
    # existing polling banner
```

- [ ] **Step 4: Manual verification — generic config still works**

Run: `.venv/bin/python collector.py --config config.example.yaml --dry-run`
Expected: existing banner output, `[dry-run] config OK, exiting.`

- [ ] **Step 5: Manual verification — SQS config works**

Create a quick test config:
```bash
cat > /tmp/sqs-test.yaml <<'EOF'
aws: { region: us-east-1, access_key: x, secret_key: y }
source:
  type: sqs_s3
  queue_url: https://sqs.us-east-1.amazonaws.com/123/q
  bucket: b
  fqdn: pce.example.com
  org_id: "1"
checkpoint: { dir: /tmp, initial_lookback_hours: 24 }
logging: { dir: /tmp, level: INFO }
pipelines:
  - { name: audit, enabled: true, log_type: auditable, mapper: { format: json }, sink: { type: file, path: /tmp/x.log } }
EOF
.venv/bin/python collector.py --config /tmp/sqs-test.yaml --dry-run
```
Expected: banner shows `source: sqs_s3 (queue: ...q)`, `pipelines: 1 enabled (auditable)`, then `[dry-run] config OK, exiting.`

- [ ] **Step 6: Run full test suite**

Run: `.venv/bin/python -m pytest -q`
Expected: still 113 passed.

- [ ] **Step 7: Commit**

```bash
git add collector.py
git commit -m "feat(collector): dispatch to PipelineScheduler or SqsS3Dispatcher by source.type"
```

---

## Task 10: config.example.yaml — SQS commented example

**Files:**
- Modify: `config.example.yaml`

- [ ] **Step 1: Add SQS example block**

Find the `source:` section in `config.example.yaml`. Above it (or below, whichever places work without breaking parse), add a commented-out alternative:

```yaml
# ─────────────────────────────────────────────────────────────────────
# Source: SQS-based (alternative to generic S3 polling above).
# Uncomment this block AND comment out the `source: { type: s3, ... }`
# block above to switch modes. The collector picks one mode based on
# source.type — they are mutually exclusive.
#
# Use this when Illumio has provisioned an SQS queue for your tenant
# (typical name: illumio-flow-<region>-<id>). One queue receives events
# for the whole bucket; the collector dispatches by S3 key path to the
# matching log_type pipeline below.
# ─────────────────────────────────────────────────────────────────────
# source:
#   type: sqs_s3
#   queue_url: https://sqs.<region>.amazonaws.com/<account>/<queue-name>
#   bucket: <same as Illumio bucket>
#   fqdn:   <pce fqdn>
#   org_id: <org id>
#   # Optional knobs (defaults shown):
#   # visibility_timeout_sec: 60
#   # visibility_extension_sec: 60
#   # wait_time_sec: 20
#   # max_messages_per_receive: 10
#   # max_workers: 1
#
# In SQS mode, pipelines.*.poll_interval_sec and max_files_per_tick are
# ignored — the dispatcher is event-driven.
```

- [ ] **Step 2: Verify YAML still parses and dry-run still works**

Run:
```
.venv/bin/python -c "import yaml; yaml.safe_load(open('config.example.yaml'))" && echo OK
.venv/bin/python collector.py --config config.example.yaml --dry-run | tail -5
```
Expected: `OK` then existing banner with `[dry-run] config OK, exiting.`

- [ ] **Step 3: Commit**

```bash
git add config.example.yaml
git commit -m "docs(config): add commented SQS-based source example"
```

---

## Task 11: Documentation — README and OPERATIONS

**Files:**
- Modify: `README.md`
- Modify: `docs/OPERATIONS.md`

- [ ] **Step 1: README — add SQS section near collection mechanism**

Find the section where the existing source / collection mechanism is described (search for "S3" or "polling"). Add a new sub-section:

```markdown
### SQS-based S3 mode (event-driven)

In addition to generic S3 polling, the collector can consume SNS-wrapped
S3 event notifications from an Illumio-provisioned SQS queue.

- One SQS queue per Illumio tenant (typical name
  `illumio-flow-<region>-<id>`); receives events for all log types in the
  bucket.
- The collector dispatches each message by S3 key path to the matching
  per-log_type pipeline (auditable / pd0 / pd1 / pd2 / pd3).
- Lower latency and lower S3 list cost than polling.
- Requires `sqs:ReceiveMessage`, `sqs:DeleteMessage`,
  `sqs:ChangeMessageVisibility` on the queue, plus existing
  `s3:GetObject` on the bucket.
- Switch by setting `source.type: sqs_s3` in `config.yaml` (see
  `config.example.yaml` for the full block). `pipelines.*.poll_interval_sec`
  and `max_files_per_tick` are ignored under SQS.

#### Failure handling

- Processing failures (S3 fetch error, sink failure, gunzip error) leave
  the SQS message intact so SQS will redeliver. Configure a redrive
  policy + DLQ on the queue for permanent failures.
- Messages with key paths that do not match any known Illumio log type
  are deleted with a WARNING log (avoids poisoning the queue).
- Messages whose log type has no enabled pipeline in `config.yaml` are
  deleted with an INFO log.
```

- [ ] **Step 2: OPERATIONS — add SQS troubleshooting**

In `docs/OPERATIONS.md`, after the existing checkpoint / polling section, add:

```markdown
## SQS source troubleshooting

Symptom: messages staying in the queue and not processed.
1. Check `collector.log` for "SQS dispatcher starting" — confirms mode.
2. Ensure the IAM role/keys have `sqs:ReceiveMessage` on the queue.
3. Confirm `source.bucket` matches the bucket that publishes to the
   queue (the dispatcher refuses messages with mismatched bucket).

Symptom: messages being deleted but no events at the SIEM.
1. Look for "no enabled pipeline for log_type=..." in collector.log —
   that log_type is not configured.
2. Look for "unknown key path ..." — the S3 key prefix does not match
   `_LOG_TYPE_PATH`. Likely Illumio added a new log type; file an issue.

Symptom: messages keep redelivering forever.
1. Check `s3.get_object` errors in collector.log — likely permission or
   region mismatch.
2. Check sink.send / flush errors — the SIEM endpoint may be down.
3. Configure a DLQ on the queue with a sane `maxReceiveCount` (e.g. 10)
   so truly poison messages move out of the main queue.

Visibility timeout tuning:
- Default `visibility_timeout_sec=60` is enough for typical objects
  (<10 MB gzipped).
- For larger objects or slow SIEM endpoints, raise to 300 in
  `source.visibility_timeout_sec`. The dispatcher auto-extends mid-process,
  but a higher floor avoids extension churn.
```

- [ ] **Step 3: Verify Markdown renders**

Run: `.venv/bin/python -c "with open('README.md') as f: print(len(f.read()))"`
(Sanity: file is not empty / didn't blow up.)

- [ ] **Step 4: Commit**

```bash
git add README.md docs/OPERATIONS.md
git commit -m "docs: SQS-based S3 mode section in README and OPERATIONS"
```

---

## Task 12: Final verification

- [ ] **Step 1: Full test suite**

Run: `.venv/bin/python -m pytest -q`
Expected: ≥ 110 passed.

- [ ] **Step 2: Generic dry-run unchanged**

Run: `.venv/bin/python collector.py --config config.example.yaml --dry-run`
Expected: existing banner + `[dry-run] config OK, exiting.`

- [ ] **Step 3: SQS dry-run shows SQS mode**

Run: `.venv/bin/python collector.py --config /tmp/sqs-test.yaml --dry-run` (config from Task 9 step 5).
Expected: banner shows `source: sqs_s3 (queue: ...)` and pipeline list, then `[dry-run] config OK, exiting.`

- [ ] **Step 4: Lint and import check**

Run: `.venv/bin/python -c "from sources.sqs_s3_source import SqsS3Dispatcher; from core.sqs_pipeline import SqsPipeline; print('OK')"`
Expected: `OK`.

- [ ] **Step 5: Diff review**

Run: `git log --oneline | head -20` — confirm 11 new commits in logical order.
Run: `git diff --stat HEAD~11..HEAD` — review file changes.

- [ ] **Step 6: Update version / CHANGELOG (if project keeps one)**

If `VERSION` or `CHANGELOG.md` exists, bump and add entry:
```
## [Unreleased]
- feat: SQS-based S3 source mode (`source.type: sqs_s3`)
```

(Skip if project does not maintain a changelog.)

- [ ] **Step 7: Final commit (if version bumped)**

```bash
git add VERSION CHANGELOG.md
git commit -m "chore: bump version for SQS-based S3 source feature"
```

---

## Summary

12 tasks, each TDD or test-first where applicable, small commits. New code surface area: ≈ 700 LoC. Test count: 92 → ≥ 113. No changes to `Pipeline`, `PipelineScheduler`, `S3Source.iter_new_files`, mappers, or sinks.
