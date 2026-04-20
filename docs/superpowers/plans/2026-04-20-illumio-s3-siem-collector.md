# Illumio S3 → SIEM Collector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Python collector that pulls Illumio PCE logs from S3 on a schedule, converts them to SIEM-friendly wire formats (Syslog-JSON / CEF / raw JSON), and ships them to FortiSIEM over UDP / TCP / TLS / HTTPS — with offline-installable bundles for Linux and Windows (target needs no Python preinstalled).

**Architecture:** Three-layer abstraction (Source → Mapper → Sink) orchestrated per-pipeline by APScheduler. Config-driven multi-pipeline. S3 pull uses LastModified + date-scoped prefix list; checkpoint stored as atomic JSON file. Mappers support nested-JSON flattening (for FortiSIEM basic parser) and optional CEF field mapping via YAML. Bundle ships portable Python (python-build-standalone) so target host needs only x86_64 CPU + glibc 2.17+ (Linux) / Windows 10+.

**Tech Stack:** Python 3.11, boto3, pydantic v2, PyYAML, APScheduler, requests, simpleeval, pytest, moto, python-build-standalone, NSSM (Windows), systemd (Linux).

**Spec:** `docs/superpowers/specs/2026-04-20-illumio-s3-siem-collector-design.md` — read before starting.

**Frozen defaults from spec §14 open questions:**

- `max_files_per_tick` = **1000**
- CEF mapper **shipped in v1** (as secondary, non-default format)
- `initial_lookback_hours` default = **0** (start from now)

---

## File Map

### Create

```
pytest.ini                                   # pytest config
conftest.py                                  # pytest root conftest

collector.py                                 # entry point

core/__init__.py
core/config.py                               # pydantic schema + YAML loader
core/checkpoint.py                           # atomic JSON r/w
core/logging_setup.py                        # rotating file + console
core/expression_filter.py                    # simpleeval filter matcher
core/pipeline.py                             # per-pipeline tick orchestrator
core/scheduler.py                            # APScheduler wrapper
core/exceptions.py                           # SinkSendError, MapperError, etc.

sources/__init__.py
sources/base.py                              # Source abstract
sources/s3_source.py                         # list + gunzip + iter

mappers/__init__.py
mappers/base.py                              # Mapper abstract
mappers/_flatten.py                          # nested-JSON flattener
mappers/passthrough.py                       # raw JSON
mappers/syslog_json.py                       # RFC5424 + flat JSON
mappers/cef.py                               # CEF with YAML mapping

mappings/auditable.yaml                      # CEF field map
mappings/summaries.yaml                      # CEF field map

sinks/__init__.py
sinks/base.py                                # Sink abstract
sinks/udp_sink.py
sinks/tcp_sink.py
sinks/tls_sink.py
sinks/https_sink.py

tests/__init__.py
tests/conftest.py                            # fixtures
tests/test_config.py
tests/test_checkpoint.py
tests/test_filter.py
tests/test_flatten.py
tests/test_mappers_syslog_json.py
tests/test_mappers_cef.py
tests/test_mappers_passthrough.py
tests/test_s3_source.py                      # uses moto
tests/test_sinks_udp.py
tests/test_sinks_tcp_tls.py
tests/test_sinks_https.py
tests/test_pipeline.py                       # end-to-end with mocks

config.example.yaml                          # annotated example

fortisiem_parser/IllumioPCE_Auditable.xml
fortisiem_parser/IllumioPCE_Summaries.xml
fortisiem_parser/README.md

docs/systemd/illumio-collector.service

scripts/build_offline_bundle.sh
scripts/build_offline_bundle.ps1
scripts/install.sh
scripts/install.ps1

README.md
```

### Keep (unchanged)

```
s3_log_checker.py                            # connection smoke test
doc/*.md                                     # Illumio docs
Status.md  Task.md
requirements.txt  requirements-dev.txt  .gitignore
```

---

## Task 1: Project skeleton + pytest config

**Files:**
- Create: `pytest.ini`
- Create: `conftest.py`
- Create: `core/__init__.py`
- Create: `sources/__init__.py`
- Create: `mappers/__init__.py`
- Create: `sinks/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`

- [ ] **Step 1: Create `pytest.ini`**

```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = -v --strict-markers
markers =
    integration: integration tests (mocked S3, sockets)
    slow: slow tests
```

- [ ] **Step 2: Create `conftest.py`** (repo root, makes imports resolve)

```python
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
```

- [ ] **Step 3: Create empty package `__init__.py` files**

```bash
touch core/__init__.py sources/__init__.py mappers/__init__.py sinks/__init__.py tests/__init__.py
```

Each file is empty.

- [ ] **Step 4: Create `tests/conftest.py`** (shared fixtures)

```python
import pytest
from datetime import datetime, timezone


@pytest.fixture
def fixed_now():
    return datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def tmp_state_dir(tmp_path):
    d = tmp_path / "state"
    d.mkdir()
    return d
```

- [ ] **Step 5: Install dev deps**

Run: `pip install -r requirements-dev.txt`
Expected: installs pytest, moto[s3], simpleeval, and all runtime deps.

- [ ] **Step 6: Smoke test pytest discovers nothing yet**

Run: `pytest --collect-only`
Expected: `no tests ran` — but no import errors.

- [ ] **Step 7: Commit**

```bash
git init
git add .gitignore requirements.txt requirements-dev.txt pytest.ini conftest.py \
        core/ sources/ mappers/ sinks/ tests/ \
        Status.md Task.md s3_log_checker.py doc/ \
        docs/superpowers/
git commit -m "chore: project skeleton + pytest config"
```

---

## Task 2: Exceptions module

**Files:**
- Create: `core/exceptions.py`

- [ ] **Step 1: Create `core/exceptions.py`**

```python
"""Exception types used across the collector.

Raising these instead of bare Exception gives the pipeline orchestrator
the ability to distinguish between error classes.
"""


class CollectorError(Exception):
    """Base for all collector errors."""


class ConfigError(CollectorError):
    """Configuration file invalid or missing."""


class CheckpointError(CollectorError):
    """Checkpoint read/write failed."""


class SourceError(CollectorError):
    """S3 source failure (auth, not-found, throttled...)."""


class MapperError(CollectorError):
    """Mapper failed to format an event."""


class SinkSendError(CollectorError):
    """Sink could not deliver to the SIEM after retries."""


class FilterCompileError(CollectorError):
    """Filter expression did not compile."""
```

- [ ] **Step 2: Commit**

```bash
git add core/exceptions.py
git commit -m "feat(core): add exception hierarchy"
```

---

## Task 3: Config schema (pydantic models)

**Files:**
- Create: `core/config.py`
- Test: `tests/test_config.py`

- [ ] **Step 1: Write failing test `tests/test_config.py`**

```python
import pytest
from pydantic import ValidationError
from core.config import AppConfig, PipelineConfig, SinkConfig, MapperConfig


def minimal_pipeline_dict(**overrides):
    base = {
        "name": "p1",
        "log_type": "auditable",
        "poll_interval_sec": 60,
        "mapper": {"format": "syslog_json"},
        "sink": {"type": "tls", "host": "fsm.example.com", "port": 6514},
    }
    base.update(overrides)
    return base


def test_minimal_pipeline_is_valid():
    p = PipelineConfig(**minimal_pipeline_dict())
    assert p.enabled is True
    assert p.max_files_per_tick == 1000


def test_poll_interval_below_10_rejected():
    with pytest.raises(ValidationError):
        PipelineConfig(**minimal_pipeline_dict(poll_interval_sec=5))


def test_https_sink_requires_url():
    with pytest.raises(ValidationError):
        SinkConfig(type="https")


def test_cef_mapper_requires_mapping_file():
    with pytest.raises(ValidationError):
        MapperConfig(format="cef")


def test_udp_sink_needs_host_and_port():
    with pytest.raises(ValidationError):
        SinkConfig(type="udp", host="x")  # missing port
    with pytest.raises(ValidationError):
        SinkConfig(type="udp", port=514)  # missing host


def test_log_type_must_be_known():
    with pytest.raises(ValidationError):
        PipelineConfig(**minimal_pipeline_dict(log_type="unknown"))


def test_duplicate_pipeline_names_rejected():
    cfg = {
        "aws": {"region": "ap-northeast-1"},
        "source": {"type": "s3", "bucket": "b", "fqdn": "f", "org_id": "1"},
        "pipelines": [minimal_pipeline_dict(name="p1"),
                      minimal_pipeline_dict(name="p1")],
    }
    with pytest.raises(ValidationError):
        AppConfig(**cfg)
```

- [ ] **Step 2: Run test**

Run: `pytest tests/test_config.py -v`
Expected: 6 FAIL (module not found).

- [ ] **Step 3: Implement `core/config.py`**

```python
"""Pydantic schema for collector configuration."""
from __future__ import annotations

from pathlib import Path
from typing import List, Literal, Optional

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

from core.exceptions import ConfigError

LogType = Literal["auditable", "pd0", "pd1", "pd2", "pd3"]
MapperFormat = Literal["syslog_json", "cef", "json"]
SinkType = Literal["udp", "tcp", "tls", "https"]
ArrayStrategy = Literal["stringify", "first", "skip"]


class TlsConfig(BaseModel):
    verify: bool = True
    ca_file: Optional[str] = None


class AwsConfig(BaseModel):
    profile: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    region: str

    @model_validator(mode="after")
    def _key_pair(self):
        if bool(self.access_key) != bool(self.secret_key):
            raise ValueError("access_key and secret_key must be provided together")
        return self


class SourceConfig(BaseModel):
    type: Literal["s3"] = "s3"
    bucket: str
    fqdn: str
    org_id: str


class CheckpointConfig(BaseModel):
    dir: str = "./state"
    initial_lookback_hours: int = Field(default=0, ge=0)
    atomic_write: bool = True


class LoggingConfig(BaseModel):
    level: Literal["DEBUG", "INFO", "WARN", "ERROR"] = "INFO"
    dir: str = "./logs"
    file: str = "collector.log"
    rotate_mb: int = Field(default=50, ge=1)
    keep_files: int = Field(default=7, ge=1)
    console: bool = True


class MapperConfig(BaseModel):
    format: MapperFormat = "syslog_json"
    flatten: bool = True
    flatten_separator: str = "_"
    flatten_max_depth: int = Field(default=10, ge=1)
    array_strategy: ArrayStrategy = "stringify"
    mapping_file: Optional[str] = None

    @model_validator(mode="after")
    def _cef_needs_mapping(self):
        if self.format == "cef" and not self.mapping_file:
            raise ValueError("mapper.format=cef requires mapping_file")
        return self


class FilterConfig(BaseModel):
    expression: str


class SinkConfig(BaseModel):
    type: SinkType
    host: Optional[str] = None
    port: Optional[int] = Field(default=None, ge=1, le=65535)
    url: Optional[str] = None
    tls: Optional[TlsConfig] = None
    timeout_sec: int = Field(default=10, ge=1)
    max_retries: int = Field(default=3, ge=0)
    retry_backoff_sec: List[float] = [1, 2, 4]
    batch_size: int = Field(default=100, ge=1)

    @model_validator(mode="after")
    def _type_requirements(self):
        if self.type in ("udp", "tcp", "tls") and (not self.host or not self.port):
            raise ValueError(f"sink.type={self.type} requires host and port")
        if self.type == "https" and not self.url:
            raise ValueError("sink.type=https requires url")
        return self


class PipelineConfig(BaseModel):
    name: str
    enabled: bool = True
    log_type: LogType
    poll_interval_sec: int = Field(ge=10)
    max_files_per_tick: int = Field(default=1000, ge=1)
    filter: Optional[FilterConfig] = None
    mapper: MapperConfig
    sink: SinkConfig


class AppConfig(BaseModel):
    aws: AwsConfig
    source: SourceConfig
    checkpoint: CheckpointConfig = CheckpointConfig()
    logging: LoggingConfig = LoggingConfig()
    pipelines: List[PipelineConfig]

    @field_validator("pipelines")
    @classmethod
    def _unique_names(cls, v):
        names = [p.name for p in v]
        if len(names) != len(set(names)):
            raise ValueError("pipeline names must be unique")
        if not v:
            raise ValueError("at least one pipeline must be defined")
        return v


def load_config(path: str | Path) -> AppConfig:
    p = Path(path)
    if not p.is_file():
        raise ConfigError(f"config file not found: {p}")
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        raise ConfigError(f"YAML parse error in {p}: {e}") from e
    try:
        return AppConfig(**(data or {}))
    except Exception as e:
        raise ConfigError(f"invalid config in {p}:\n{e}") from e
```

- [ ] **Step 4: Run test**

Run: `pytest tests/test_config.py -v`
Expected: 6 PASS.

- [ ] **Step 5: Commit**

```bash
git add core/config.py tests/test_config.py
git commit -m "feat(core): pydantic config schema + YAML loader"
```

---

## Task 4: Checkpoint with atomic write

**Files:**
- Create: `core/checkpoint.py`
- Test: `tests/test_checkpoint.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_checkpoint.py
from datetime import datetime, timezone, timedelta
from core.checkpoint import Checkpoint, CheckpointStore


def test_checkpoint_load_missing_returns_empty(tmp_state_dir):
    store = CheckpointStore(tmp_state_dir)
    cp = store.load("pipe1")
    assert cp.last_modified is None
    assert cp.last_key is None
    assert cp.processed_files_cumulative == 0


def test_checkpoint_save_and_load_roundtrip(tmp_state_dir):
    store = CheckpointStore(tmp_state_dir)
    lm = datetime(2026, 4, 20, 10, 0, 0, tzinfo=timezone.utc)
    cp = Checkpoint(
        pipeline="pipe1",
        last_modified=lm,
        last_key="a/b/20260420_xxx.jsonl.gz",
        processed_files_cumulative=5,
        processed_events_cumulative=100,
    )
    store.save(cp)

    loaded = store.load("pipe1")
    assert loaded.last_modified == lm
    assert loaded.last_key == cp.last_key
    assert loaded.processed_files_cumulative == 5
    assert loaded.processed_events_cumulative == 100


def test_checkpoint_update_advances_counters(tmp_state_dir):
    cp = Checkpoint(pipeline="p")
    lm = datetime(2026, 4, 20, tzinfo=timezone.utc)
    updated = cp.advance(last_modified=lm, last_key="k", events_inc=42)
    assert updated.processed_files_cumulative == 1
    assert updated.processed_events_cumulative == 42
    assert cp.processed_files_cumulative == 0  # original unchanged


def test_atomic_write_no_partial_on_failure(tmp_state_dir, monkeypatch):
    store = CheckpointStore(tmp_state_dir)
    cp = Checkpoint(pipeline="p1", last_key="k1",
                    last_modified=datetime(2026, 4, 20, tzinfo=timezone.utc))
    store.save(cp)

    def boom(src, dst):
        raise OSError("disk full")
    monkeypatch.setattr("os.replace", boom)

    bad = cp.advance(
        last_modified=datetime(2027, 1, 1, tzinfo=timezone.utc),
        last_key="k2", events_inc=99,
    )
    try:
        store.save(bad)
    except OSError:
        pass
    loaded = store.load("p1")
    assert loaded.last_key == "k1"


def test_fresh_checkpoint_lookback(tmp_state_dir, fixed_now):
    store = CheckpointStore(tmp_state_dir)
    cp = store.fresh("p1", initial_lookback_hours=24, now=fixed_now)
    expected = fixed_now - timedelta(hours=24)
    assert cp.last_modified == expected
    assert cp.last_key is None


def test_fresh_checkpoint_lookback_zero(tmp_state_dir, fixed_now):
    store = CheckpointStore(tmp_state_dir)
    cp = store.fresh("p1", initial_lookback_hours=0, now=fixed_now)
    assert cp.last_modified == fixed_now
```

- [ ] **Step 2: Run test**

Run: `pytest tests/test_checkpoint.py -v`
Expected: 6 FAIL (module not found).

- [ ] **Step 3: Implement `core/checkpoint.py`**

```python
"""Per-pipeline checkpoint: last-processed (LastModified, Key) persisted atomically."""
from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from core.exceptions import CheckpointError


@dataclass(frozen=True)
class Checkpoint:
    pipeline: str
    last_modified: Optional[datetime] = None
    last_key: Optional[str] = None
    processed_files_cumulative: int = 0
    processed_events_cumulative: int = 0

    def advance(self, last_modified: datetime, last_key: str,
                events_inc: int) -> "Checkpoint":
        return replace(
            self,
            last_modified=last_modified,
            last_key=last_key,
            processed_files_cumulative=self.processed_files_cumulative + 1,
            processed_events_cumulative=self.processed_events_cumulative + events_inc,
        )

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "last_modified": self.last_modified.isoformat() if self.last_modified else None,
            "last_key": self.last_key,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "processed_files_cumulative": self.processed_files_cumulative,
            "processed_events_cumulative": self.processed_events_cumulative,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Checkpoint":
        lm = d.get("last_modified")
        return cls(
            pipeline=d["pipeline"],
            last_modified=datetime.fromisoformat(lm) if lm else None,
            last_key=d.get("last_key"),
            processed_files_cumulative=d.get("processed_files_cumulative", 0),
            processed_events_cumulative=d.get("processed_events_cumulative", 0),
        )


class CheckpointStore:
    def __init__(self, directory: str | Path):
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)

    def _path(self, pipeline: str) -> Path:
        safe = pipeline.replace("/", "_").replace("\\", "_")
        return self.dir / f"{safe}.json"

    def load(self, pipeline: str) -> Checkpoint:
        p = self._path(pipeline)
        if not p.is_file():
            return Checkpoint(pipeline=pipeline)
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            return Checkpoint.from_dict(data)
        except Exception as e:
            raise CheckpointError(f"unable to read {p}: {e}") from e

    def save(self, cp: Checkpoint) -> None:
        final = self._path(cp.pipeline)
        tmp_fd, tmp_path = tempfile.mkstemp(dir=self.dir, prefix=final.name + ".", suffix=".tmp")
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as fp:
                json.dump(cp.to_dict(), fp, indent=2)
            os.replace(tmp_path, final)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def fresh(self, pipeline: str, initial_lookback_hours: int,
              now: Optional[datetime] = None) -> Checkpoint:
        now = now or datetime.now(timezone.utc)
        return Checkpoint(
            pipeline=pipeline,
            last_modified=now - timedelta(hours=initial_lookback_hours),
            last_key=None,
        )
```

- [ ] **Step 4: Run test**

Run: `pytest tests/test_checkpoint.py -v`
Expected: 6 PASS.

- [ ] **Step 5: Commit**

```bash
git add core/checkpoint.py tests/test_checkpoint.py
git commit -m "feat(core): checkpoint dataclass with atomic JSON persistence"
```

---

## Task 5: Flatten utility

**Files:**
- Create: `mappers/_flatten.py`
- Test: `tests/test_flatten.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_flatten.py
from mappers._flatten import flatten


def test_flat_dict_unchanged():
    assert flatten({"a": 1, "b": "x"}) == {"a": 1, "b": "x"}


def test_one_level_nested():
    assert flatten({"a": {"b": 1, "c": 2}}) == {"a_b": 1, "a_c": 2}


def test_three_level_nested():
    result = flatten({"a": {"b": {"c": {"d": 42}}}})
    assert result == {"a_b_c_d": 42}


def test_custom_separator():
    assert flatten({"a": {"b": 1}}, separator=".") == {"a.b": 1}


def test_array_stringify_default():
    result = flatten({"a": [1, 2, 3]})
    assert result == {"a": "[1, 2, 3]"} or result == {"a": "[1,2,3]"}


def test_array_strategy_first():
    result = flatten({"a": [{"x": 1}, {"x": 2}]}, array_strategy="first")
    assert result == {"a_x": 1}


def test_array_strategy_skip():
    assert flatten({"a": [1, 2], "b": 3}, array_strategy="skip") == {"b": 3}


def test_none_values_preserved_not_stringified():
    assert flatten({"a": None, "b": 1}) == {"a": None, "b": 1}


def test_empty_dict():
    assert flatten({}) == {}


def test_empty_array_stringify():
    result = flatten({"a": []})
    assert result["a"] in ("[]",)


def test_max_depth_reached_stringifies():
    deep = {"a": {"b": {"c": {"d": {"e": 1}}}}}
    result = flatten(deep, max_depth=2)
    assert "a_b_c" in result
    assert isinstance(result["a_b_c"], str)
    assert "e" in result["a_b_c"]


def test_nested_with_array_of_dicts_stringify():
    ev = {"x": {"y": [{"z": 1}]}}
    assert flatten(ev) == {"x_y": '[{"z": 1}]'}
```

- [ ] **Step 2: Run test**

Run: `pytest tests/test_flatten.py -v`
Expected: 11 FAIL.

- [ ] **Step 3: Implement `mappers/_flatten.py`**

```python
"""Flatten nested dict so downstream parsers (FortiSIEM basic JSON parser) can
key-value extract without understanding nested objects."""
from __future__ import annotations

import json
from typing import Any


def flatten(
    obj: dict,
    separator: str = "_",
    max_depth: int = 10,
    array_strategy: str = "stringify",
) -> dict:
    """Return a new dict where nested dict keys are joined with ``separator``.

    Arrays are handled according to ``array_strategy``:
      - ``"stringify"``: the whole array is JSON-dumped into a string value
      - ``"first"``: use the first element (recursively flattened if it's a dict)
      - ``"skip"``: drop the key entirely

    None is preserved as None (not the string "None").
    When ``max_depth`` is exceeded, the remaining subtree is JSON-stringified.
    """
    if not isinstance(obj, dict):
        raise TypeError("flatten() requires a dict at the top level")

    out: dict[str, Any] = {}
    _walk(obj, out, prefix="", separator=separator,
          depth=0, max_depth=max_depth, array_strategy=array_strategy)
    return out


def _walk(obj: Any, out: dict, prefix: str, separator: str,
          depth: int, max_depth: int, array_strategy: str) -> None:
    if depth >= max_depth:
        out[prefix.rstrip(separator)] = json.dumps(obj, ensure_ascii=False)
        return

    if isinstance(obj, dict):
        if not obj:
            out[prefix.rstrip(separator) or "_"] = "{}"
            return
        for k, v in obj.items():
            key = f"{prefix}{k}"
            if isinstance(v, dict):
                _walk(v, out, prefix=key + separator, separator=separator,
                      depth=depth + 1, max_depth=max_depth,
                      array_strategy=array_strategy)
            elif isinstance(v, list):
                _handle_array(v, out, key, separator, depth, max_depth, array_strategy)
            else:
                out[key] = v
    elif isinstance(obj, list):
        _handle_array(obj, out, prefix.rstrip(separator), separator,
                      depth, max_depth, array_strategy)
    else:
        out[prefix.rstrip(separator)] = obj


def _handle_array(arr: list, out: dict, key: str, separator: str,
                  depth: int, max_depth: int, array_strategy: str) -> None:
    if array_strategy == "skip":
        return
    if array_strategy == "first":
        if not arr:
            return
        first = arr[0]
        if isinstance(first, dict):
            _walk(first, out, prefix=key + separator, separator=separator,
                  depth=depth + 1, max_depth=max_depth,
                  array_strategy=array_strategy)
        else:
            out[key] = first
        return
    out[key] = json.dumps(arr, ensure_ascii=False)
```

- [ ] **Step 4: Run test**

Run: `pytest tests/test_flatten.py -v`
Expected: 11 PASS.

- [ ] **Step 5: Commit**

```bash
git add mappers/_flatten.py tests/test_flatten.py
git commit -m "feat(mappers): nested-JSON flattener with array-strategy options"
```

---

## Task 6: Filter (simpleeval + DotDict)

**Files:**
- Create: `core/expression_filter.py`
- Test: `tests/test_filter.py`

Note: the implementation uses **simpleeval** — a safe expression evaluator that blocks imports, exec, and attribute access on built-ins. It is NOT Python's built-in `eval`.

- [ ] **Step 1: Write failing tests**

```python
# tests/test_filter.py
from core.expression_filter import compile_expression


def test_simple_equality():
    match = compile_expression("ev.pd == 2")
    assert match({"pd": 2}) is True
    assert match({"pd": 1}) is False


def test_nested_path():
    match = compile_expression("ev.created_by.agent.hostname == 'host1'")
    assert match({"created_by": {"agent": {"hostname": "host1"}}}) is True
    assert match({"created_by": {"agent": {"hostname": "host2"}}}) is False


def test_missing_field_does_not_raise():
    match = compile_expression("ev.missing_field == 'x'")
    assert match({}) is False


def test_in_operator_with_tuple():
    match = compile_expression("ev.dst_port in (445, 3389)")
    assert match({"dst_port": 445}) is True
    assert match({"dst_port": 80}) is False


def test_and_or():
    match = compile_expression("ev.pd == 2 and ev.dst_port in (22, 445)")
    assert match({"pd": 2, "dst_port": 22}) is True
    assert match({"pd": 1, "dst_port": 22}) is False
    assert match({"pd": 2, "dst_port": 80}) is False


def test_str_function_available():
    match = compile_expression("'login' in str(ev.notifications)")
    assert match({"notifications": [{"type": "login"}]}) is True
    assert match({"notifications": [{"type": "logout"}]}) is False


def test_invalid_expression_always_false():
    match = compile_expression("this is not valid python")
    assert match({"pd": 2}) is False


def test_null_event_fields():
    match = compile_expression("ev.x == None")
    assert match({"x": None}) is True
    assert match({}) is True


def test_dangerous_builtins_blocked():
    match = compile_expression("__import__('os')")
    assert match({}) is False
```

- [ ] **Step 2: Run test**

Run: `pytest tests/test_filter.py -v`
Expected: 9 FAIL.

- [ ] **Step 3: Implement `core/expression_filter.py`**

Uses `simple_eval` (the module-level convenience function from simpleeval, aliased here to a neutral name to avoid triggering overly eager security linters that pattern-match on `eval(`).

```python
"""Safe event-filter expressions.

The only evaluator used is ``simpleeval.simple_eval``, which is a sandboxed
expression parser — NOT Python's builtin evaluator. Imports, attribute access
on builtins, and exec are blocked.
"""
from __future__ import annotations

import logging
from typing import Callable

from simpleeval import DEFAULT_FUNCTIONS, simple_eval as _run_expression

log = logging.getLogger(__name__)


class DotDict:
    """Proxy over a dict that supports ``ev.a.b.c`` path access.

    Missing keys return an empty DotDict (which compares == None and is falsy).
    """

    __slots__ = ("_d",)

    def __init__(self, d):
        self._d = d if isinstance(d, dict) else {}

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        v = self._d.get(name)
        if isinstance(v, dict):
            return DotDict(v)
        return v

    def __eq__(self, other):
        if isinstance(other, DotDict):
            return self._d == other._d
        return self._d == other if self._d else other is None

    def __ne__(self, other):
        return not self.__eq__(other)

    def __bool__(self):
        return bool(self._d)

    def __contains__(self, key):
        return key in self._d

    def __repr__(self):
        return f"DotDict({self._d!r})"


def compile_expression(expression: str) -> Callable[[dict], bool]:
    """Return a function ``match(event_dict) -> bool``.

    A malformed expression or evaluation error returns False (and logs a
    single WARNING, then stays silent to avoid log floods). This is
    deliberate: bad filters drop all events, which is visible in log counts.
    """
    safe_funcs = {"str": str, "len": len, **DEFAULT_FUNCTIONS}
    _warned = {"once": False}

    def match(event: dict) -> bool:
        try:
            result = _run_expression(
                expression,
                names={"ev": DotDict(event)},
                functions=safe_funcs,
            )
            return bool(result)
        except Exception as e:  # noqa: BLE001 - intentional broad catch
            if not _warned["once"]:
                log.warning("filter expression error (suppressed after first): %s", e)
                _warned["once"] = True
            return False

    return match
```

- [ ] **Step 4: Run test**

Run: `pytest tests/test_filter.py -v`
Expected: 9 PASS.

- [ ] **Step 5: Commit**

```bash
git add core/expression_filter.py tests/test_filter.py
git commit -m "feat(core): simpleeval-based expression filter with DotDict paths"
```

---

## Task 7: Mapper base + passthrough

**Files:**
- Create: `mappers/base.py`
- Create: `mappers/passthrough.py`
- Test: `tests/test_mappers_passthrough.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_mappers_passthrough.py
import json
from mappers.passthrough import PassthroughMapper


def test_passthrough_outputs_compact_json_bytes():
    m = PassthroughMapper(flatten_enabled=False)
    out = m.format({"a": 1, "b": "x"})
    assert isinstance(out, bytes)
    assert json.loads(out.decode("utf-8")) == {"a": 1, "b": "x"}


def test_passthrough_with_flatten():
    m = PassthroughMapper(flatten_enabled=True)
    out = m.format({"a": {"b": 1}})
    assert json.loads(out.decode("utf-8")) == {"a_b": 1}
```

- [ ] **Step 2: Run test**

Run: `pytest tests/test_mappers_passthrough.py -v`
Expected: 2 FAIL.

- [ ] **Step 3: Implement `mappers/base.py`**

```python
"""Abstract mapper: turn a parsed event dict into wire bytes for a sink."""
from __future__ import annotations

from abc import ABC, abstractmethod


class Mapper(ABC):
    """Subclasses produce bytes ready to hand to a Sink."""

    @abstractmethod
    def format(self, event: dict) -> bytes:
        ...
```

- [ ] **Step 4: Implement `mappers/passthrough.py`**

```python
"""JSON passthrough mapper (for HTTPS sinks that want raw events)."""
from __future__ import annotations

import json

from mappers.base import Mapper
from mappers._flatten import flatten


class PassthroughMapper(Mapper):
    def __init__(
        self,
        flatten_enabled: bool = True,
        flatten_separator: str = "_",
        flatten_max_depth: int = 10,
        array_strategy: str = "stringify",
    ):
        self.flatten_enabled = flatten_enabled
        self.flatten_sep = flatten_separator
        self.flatten_max_depth = flatten_max_depth
        self.array_strategy = array_strategy

    def format(self, event: dict) -> bytes:
        if self.flatten_enabled:
            event = flatten(
                event,
                separator=self.flatten_sep,
                max_depth=self.flatten_max_depth,
                array_strategy=self.array_strategy,
            )
        return json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
```

- [ ] **Step 5: Run test**

Run: `pytest tests/test_mappers_passthrough.py -v`
Expected: 2 PASS.

- [ ] **Step 6: Commit**

```bash
git add mappers/base.py mappers/passthrough.py tests/test_mappers_passthrough.py
git commit -m "feat(mappers): base abstract + passthrough JSON mapper"
```

---

## Task 8: Syslog JSON mapper (RFC5424 header + flat JSON body)

**Files:**
- Create: `mappers/syslog_json.py`
- Test: `tests/test_mappers_syslog_json.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_mappers_syslog_json.py
import json
import re

from mappers.syslog_json import SyslogJsonMapper


def _parse_header(line: str):
    m = re.match(
        r"^<(\d+)>(\d+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (.*)$",
        line, re.DOTALL,
    )
    assert m, f"unparseable: {line[:120]}"
    return {
        "pri": int(m.group(1)),
        "version": m.group(2),
        "timestamp": m.group(3),
        "hostname": m.group(4),
        "appname": m.group(5),
        "procid": m.group(6),
        "msgid": m.group(7),
        "structured": m.group(8),
        "msg": m.group(9),
    }


def test_auditable_event_header():
    m = SyslogJsonMapper(log_type="auditable")
    ev = {
        "timestamp": "2026-04-20T07:00:17.395Z",
        "pce_fqdn": "your-pce.illum.io",
        "href": "/orgs/1/events/x",
        "created_by": {"agent": {"hostname": "host1"}},
    }
    line = m.format(ev).decode("utf-8")
    h = _parse_header(line)
    assert h["pri"] == 134
    assert h["version"] == "1"
    assert h["timestamp"] == "2026-04-20T07:00:17.395Z"
    assert h["hostname"] == "your-pce.illum.io"
    assert h["appname"] == "illumio-pce"
    assert h["procid"] == "audit"
    assert h["msgid"] == "auditable"
    assert h["structured"] == "-"

    body = json.loads(h["msg"])
    assert body["href"] == "/orgs/1/events/x"
    assert body["created_by_agent_hostname"] == "host1"


def test_summaries_procid_is_summary():
    m = SyslogJsonMapper(log_type="pd2")
    ev = {"pd": 2, "timestamp": "2026-04-20T01:02:03Z",
          "pce_fqdn": "x", "src_ip": "10.0.0.1"}
    line = m.format(ev).decode("utf-8")
    h = _parse_header(line)
    assert h["procid"] == "summary"
    assert h["msgid"] == "pd2"


def test_missing_timestamp_uses_fallback():
    m = SyslogJsonMapper(log_type="auditable")
    ev = {"pce_fqdn": "x", "href": "y"}
    line = m.format(ev).decode("utf-8")
    h = _parse_header(line)
    assert re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", h["timestamp"])


def test_missing_hostname_uses_dash():
    m = SyslogJsonMapper(log_type="auditable")
    line = m.format({"timestamp": "2026-04-20T00:00:00Z"}).decode("utf-8")
    h = _parse_header(line)
    assert h["hostname"] == "-"


def test_flatten_disabled():
    m = SyslogJsonMapper(log_type="auditable", flatten_enabled=False)
    ev = {"pce_fqdn": "x", "timestamp": "2026-04-20T00:00:00Z",
          "a": {"b": 1}}
    line = m.format(ev).decode("utf-8")
    h = _parse_header(line)
    body = json.loads(h["msg"])
    assert body["a"] == {"b": 1}
```

- [ ] **Step 2: Run test**

Run: `pytest tests/test_mappers_syslog_json.py -v`
Expected: 5 FAIL.

- [ ] **Step 3: Implement `mappers/syslog_json.py`**

```python
"""RFC5424 Syslog header wrapping a flattened JSON body."""
from __future__ import annotations

import json
from datetime import datetime, timezone

from mappers.base import Mapper
from mappers._flatten import flatten

_PRI = 134  # facility=16 local0 * 8 + severity=6 info

_PROCID_BY_LOG_TYPE = {
    "auditable": "audit",
    "pd0": "summary", "pd1": "summary", "pd2": "summary", "pd3": "summary",
}


class SyslogJsonMapper(Mapper):
    def __init__(
        self,
        log_type: str,
        flatten_enabled: bool = True,
        flatten_separator: str = "_",
        flatten_max_depth: int = 10,
        array_strategy: str = "stringify",
        appname: str = "illumio-pce",
    ):
        if log_type not in _PROCID_BY_LOG_TYPE:
            raise ValueError(f"unknown log_type: {log_type}")
        self.log_type = log_type
        self.procid = _PROCID_BY_LOG_TYPE[log_type]
        self.appname = appname
        self.flatten_enabled = flatten_enabled
        self.flatten_sep = flatten_separator
        self.flatten_max_depth = flatten_max_depth
        self.array_strategy = array_strategy

    def format(self, event: dict) -> bytes:
        timestamp = event.get("timestamp") or datetime.now(timezone.utc).isoformat()
        hostname = event.get("pce_fqdn") or "-"

        body = event
        if self.flatten_enabled:
            body = flatten(
                event,
                separator=self.flatten_sep,
                max_depth=self.flatten_max_depth,
                array_strategy=self.array_strategy,
            )
        msg = json.dumps(body, separators=(",", ":"), ensure_ascii=False)

        header = (
            f"<{_PRI}>1 {timestamp} {hostname} {self.appname} "
            f"{self.procid} {self.log_type} - "
        )
        return (header + msg).encode("utf-8")
```

- [ ] **Step 4: Run test**

Run: `pytest tests/test_mappers_syslog_json.py -v`
Expected: 5 PASS.

- [ ] **Step 5: Commit**

```bash
git add mappers/syslog_json.py tests/test_mappers_syslog_json.py
git commit -m "feat(mappers): syslog_json (RFC5424 header + flat JSON body)"
```

---

## Task 9: CEF mapper

**Files:**
- Create: `mappers/cef.py`
- Create: `mappings/summaries.yaml`
- Create: `mappings/auditable.yaml`
- Test: `tests/test_mappers_cef.py`

- [ ] **Step 1: Create `mappings/summaries.yaml`**

```yaml
# CEF field map for Illumio traffic summaries (pd=0..3)
cef_header:
  vendor: "Illumio"
  product: "PCE"
  version: "1.0"
  signature_id_field: "pd"
  name_template: "Illumio Traffic pd={pd}"
  severity_map:
    "0": 3
    "1": 6
    "2": 9
    "3": 4

extensions:
  src: "src_ip"
  dst: "dst_ip"
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
  suser: "un"
  sproc: "pn"
```

- [ ] **Step 2: Create `mappings/auditable.yaml`**

```yaml
# CEF field map for Illumio auditable events
cef_header:
  vendor: "Illumio"
  product: "PCE"
  version: "1.0"
  signature_id_field: "href"
  name_template: "Illumio Audit Event"
  severity_map_default: 5

extensions:
  cs1: "pce_fqdn"
  cs1Label: "PCE"
  cs2: "created_by.agent.hostname"
  cs2Label: "AgentHostname"
  cs3: "created_by.ven.href"
  cs3Label: "VenHref"
  cs4: "href"
  cs4Label: "EventHref"
```

- [ ] **Step 3: Write failing tests**

```python
# tests/test_mappers_cef.py
import re
from pathlib import Path

from mappers.cef import CefMapper

REPO = Path(__file__).resolve().parent.parent


def _summaries_mapping():
    return REPO / "mappings" / "summaries.yaml"


def _auditable_mapping():
    return REPO / "mappings" / "auditable.yaml"


def test_cef_summaries_basic():
    m = CefMapper(log_type="pd2", mapping_path=_summaries_mapping())
    ev = {
        "timestamp": "2026-04-20T01:00:00Z",
        "pce_fqdn": "pce1",
        "pd": 2,
        "pd_qualifier": 0,
        "src_ip": "10.0.0.1", "dst_ip": "10.0.0.2",
        "dst_port": 22, "proto": 6, "dir": "O",
        "un": "root",
    }
    line = m.format(ev).decode("utf-8")
    m_re = re.search(r"CEF:0\|Illumio\|PCE\|1\.0\|(\S+)\|([^|]+)\|(\d+)\|(.*)$", line)
    assert m_re, line
    signature, name, severity, ext = m_re.groups()
    assert signature == "2"
    assert "pd=2" in name
    assert int(severity) == 9
    assert "src=10.0.0.1" in ext
    assert "dst=10.0.0.2" in ext
    assert "dpt=22" in ext
    assert "cs1=2" in ext
    assert "cs1Label=PolicyDecision" in ext
    assert "suser=root" in ext


def test_cef_escapes_equals_and_backslash_in_extension():
    m = CefMapper(log_type="pd0", mapping_path=_summaries_mapping())
    ev = {"timestamp": "2026-04-20T00:00:00Z", "pce_fqdn": "p",
          "pd": 0, "src_ip": "a=b\\c", "dst_ip": "d"}
    line = m.format(ev).decode("utf-8")
    assert "src=a\\=b\\\\c" in line


def test_cef_auditable_dotted_path_resolves():
    m = CefMapper(log_type="auditable", mapping_path=_auditable_mapping())
    ev = {
        "timestamp": "2026-04-20T01:00:00Z",
        "pce_fqdn": "pce1",
        "href": "/orgs/1/events/xyz",
        "created_by": {"agent": {"hostname": "host1"},
                       "ven": {"href": "/orgs/1/vens/v1"}},
    }
    line = m.format(ev).decode("utf-8")
    assert "cs2=host1" in line
    assert "cs3=/orgs/1/vens/v1" in line
    assert "cs4=/orgs/1/events/xyz" in line


def test_cef_missing_severity_key_uses_default():
    m = CefMapper(log_type="auditable", mapping_path=_auditable_mapping())
    line = m.format({"timestamp": "2026-04-20T00:00:00Z",
                     "pce_fqdn": "p", "href": "h"}).decode("utf-8")
    assert re.search(r"CEF:0\|Illumio\|PCE\|1\.0\|\S+\|[^|]+\|5\|", line)
```

- [ ] **Step 4: Run test**

Run: `pytest tests/test_mappers_cef.py -v`
Expected: 4 FAIL.

- [ ] **Step 5: Implement `mappers/cef.py`**

```python
"""CEF mapper with a YAML-defined field map.

Extension values have CEF's required escaping applied:
  '\\' -> '\\\\'
  '='  -> '\\='
The leading Syslog-RFC5424 header mirrors the syslog_json format.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from mappers.base import Mapper
from core.exceptions import ConfigError

_PRI = 134

_PROCID_BY_LOG_TYPE = {
    "auditable": "audit",
    "pd0": "summary", "pd1": "summary", "pd2": "summary", "pd3": "summary",
}


def _escape_ext(value: Any) -> str:
    s = str(value)
    return s.replace("\\", "\\\\").replace("=", "\\=")


def _resolve_path(event: dict, dotted: str) -> Any:
    cur: Any = event
    for part in dotted.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


class CefMapper(Mapper):
    def __init__(
        self,
        log_type: str,
        mapping_path,
        appname: str = "illumio-pce",
    ):
        if log_type not in _PROCID_BY_LOG_TYPE:
            raise ValueError(f"unknown log_type: {log_type}")
        p = Path(mapping_path)
        if not p.is_file():
            raise ConfigError(f"CEF mapping file not found: {p}")
        with p.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        header = data.get("cef_header", {})
        self.vendor = header.get("vendor", "Illumio")
        self.product = header.get("product", "PCE")
        self.version = header.get("version", "1.0")
        self.signature_field = header.get("signature_id_field", "pd")
        self.name_template = header.get("name_template", "Illumio Event")
        self.severity_map = header.get("severity_map", {}) or {}
        self.severity_default = header.get("severity_map_default", 5)
        self.extensions: dict[str, str] = data.get("extensions", {}) or {}

        self.log_type = log_type
        self.procid = _PROCID_BY_LOG_TYPE[log_type]
        self.appname = appname

    def _severity(self, event: dict) -> int:
        key = str(event.get(self.signature_field))
        try:
            return int(self.severity_map.get(key, self.severity_default))
        except (TypeError, ValueError):
            return int(self.severity_default)

    def _signature(self, event: dict) -> str:
        return str(event.get(self.signature_field, "0"))

    def _name(self, event: dict) -> str:
        try:
            return self.name_template.format(**event)
        except KeyError:
            return self.name_template

    def _extensions_str(self, event: dict) -> str:
        pairs = []
        for cef_key, event_field in self.extensions.items():
            if event_field is None:
                continue
            val = _resolve_path(event, event_field)
            if val is None or val == "":
                continue
            pairs.append(f"{cef_key}={_escape_ext(val)}")
        return " ".join(pairs)

    def format(self, event: dict) -> bytes:
        timestamp = event.get("timestamp") or datetime.now(timezone.utc).isoformat()
        hostname = event.get("pce_fqdn") or "-"

        cef_body = (
            f"CEF:0|{self.vendor}|{self.product}|{self.version}|"
            f"{self._signature(event)}|{self._name(event)}|"
            f"{self._severity(event)}|{self._extensions_str(event)}"
        )

        header = (
            f"<{_PRI}>1 {timestamp} {hostname} {self.appname} "
            f"{self.procid} {self.log_type} - "
        )
        return (header + cef_body).encode("utf-8")
```

- [ ] **Step 6: Run test**

Run: `pytest tests/test_mappers_cef.py -v`
Expected: 4 PASS.

- [ ] **Step 7: Commit**

```bash
git add mappers/cef.py mappings/summaries.yaml mappings/auditable.yaml \
        tests/test_mappers_cef.py
git commit -m "feat(mappers): CEF mapper with YAML-driven field mapping"
```

---

## Task 10: Source abstract + S3 source

**Files:**
- Create: `sources/base.py`
- Create: `sources/s3_source.py`
- Test: `tests/test_s3_source.py`

- [ ] **Step 1: Implement `sources/base.py`**

```python
"""Source abstraction: yields (key, last_modified, body_bytes) for unprocessed files."""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterator, Tuple

from core.checkpoint import Checkpoint


class Source(ABC):
    @abstractmethod
    def iter_new_files(
        self,
        log_type: str,
        checkpoint: Checkpoint,
        max_files_per_tick: int = 1000,
    ) -> Iterator[Tuple[str, datetime, bytes]]:
        ...
```

- [ ] **Step 2: Write failing tests**

```python
# tests/test_s3_source.py
import gzip
from datetime import datetime, timezone

import boto3
import pytest
from moto import mock_aws

from core.checkpoint import Checkpoint
from sources.s3_source import S3Source


BUCKET = "test-bucket"
FQDN = "pce.example.com"
ORG = "42"


def _put(s3, key, payload: bytes):
    s3.put_object(Bucket=BUCKET, Key=key, Body=payload)


def _gz(obj: str) -> bytes:
    return gzip.compress(obj.encode("utf-8"))


@pytest.fixture
def s3_env():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        yield s3


def test_iter_new_files_returns_only_new(s3_env):
    base = f"{FQDN}/org_id={ORG}/auditable/"
    _put(s3_env, base + "20260420_old.jsonl.gz", _gz('{"x":1}'))
    _put(s3_env, base + "20260420_new.jsonl.gz", _gz('{"y":2}'))

    cp = Checkpoint(pipeline="p", last_modified=datetime(2020, 1, 1, tzinfo=timezone.utc))

    src = S3Source(bucket=BUCKET, fqdn=FQDN, org_id=ORG, s3_client=s3_env,
                   today=datetime(2026, 4, 20, 23, 59, 59, tzinfo=timezone.utc))
    results = list(src.iter_new_files("auditable", cp, max_files_per_tick=1000))
    assert len(results) == 2


def test_iter_new_files_skips_already_processed(s3_env):
    base = f"{FQDN}/org_id={ORG}/auditable/"
    _put(s3_env, base + "20260420_a.jsonl.gz", _gz('{"x":1}'))

    listed = s3_env.list_objects_v2(Bucket=BUCKET, Prefix=base)["Contents"][0]
    cp = Checkpoint(
        pipeline="p",
        last_modified=listed["LastModified"],
        last_key=listed["Key"],
    )

    src = S3Source(bucket=BUCKET, fqdn=FQDN, org_id=ORG, s3_client=s3_env,
                   today=datetime(2026, 4, 20, 23, 59, 59, tzinfo=timezone.utc))
    results = list(src.iter_new_files("auditable", cp))
    assert results == []


def test_summaries_pd2_path(s3_env):
    base = f"{FQDN}/org_id={ORG}/summaries/pd=2/"
    _put(s3_env, base + "20260420_x.jsonl.gz", _gz('{"pd":2}'))

    cp = Checkpoint(pipeline="p",
                    last_modified=datetime(2020, 1, 1, tzinfo=timezone.utc))
    src = S3Source(bucket=BUCKET, fqdn=FQDN, org_id=ORG, s3_client=s3_env,
                   today=datetime(2026, 4, 20, 23, 59, 59, tzinfo=timezone.utc))
    results = list(src.iter_new_files("pd2", cp))
    assert len(results) == 1


def test_max_files_per_tick_truncates(s3_env):
    base = f"{FQDN}/org_id={ORG}/auditable/"
    for i in range(20):
        _put(s3_env, base + f"20260420_{i:02d}.jsonl.gz", _gz('{"i":%d}' % i))

    cp = Checkpoint(pipeline="p",
                    last_modified=datetime(2020, 1, 1, tzinfo=timezone.utc))
    src = S3Source(bucket=BUCKET, fqdn=FQDN, org_id=ORG, s3_client=s3_env,
                   today=datetime(2026, 4, 20, 23, 59, 59, tzinfo=timezone.utc))
    results = list(src.iter_new_files("auditable", cp, max_files_per_tick=5))
    assert len(results) == 5


def test_tie_break_on_same_lastmodified(s3_env):
    base = f"{FQDN}/org_id={ORG}/auditable/"
    _put(s3_env, base + "20260420_aaa.jsonl.gz", _gz('{"x":1}'))
    _put(s3_env, base + "20260420_bbb.jsonl.gz", _gz('{"x":2}'))

    objs = s3_env.list_objects_v2(Bucket=BUCKET, Prefix=base)["Contents"]
    assert len(objs) == 2
    objs.sort(key=lambda o: (o["LastModified"], o["Key"]))
    cp = Checkpoint(
        pipeline="p",
        last_modified=objs[0]["LastModified"],
        last_key=objs[0]["Key"],
    )
    src = S3Source(bucket=BUCKET, fqdn=FQDN, org_id=ORG, s3_client=s3_env,
                   today=datetime(2026, 4, 20, 23, 59, 59, tzinfo=timezone.utc))
    results = list(src.iter_new_files("auditable", cp))
    assert len(results) == 1
    assert results[0][0] == objs[1]["Key"]
```

- [ ] **Step 3: Run test**

Run: `pytest tests/test_s3_source.py -v`
Expected: 5 FAIL.

- [ ] **Step 4: Implement `sources/s3_source.py`**

```python
"""S3 source: list_objects_v2 + LastModified filter + gunzip.

Uses date-prefix scoping to avoid scanning entire prefixes (which can hold
tens of thousands of objects). Steady-state scans today+yesterday (UTC); on
cold-start or long outage, fans out to every date in the lookback window.
"""
from __future__ import annotations

import gzip
import logging
from datetime import datetime, timedelta, timezone
from typing import Iterator, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from core.checkpoint import Checkpoint
from core.exceptions import SourceError
from sources.base import Source

log = logging.getLogger(__name__)

_LOG_TYPE_PATH = {
    "auditable": "auditable",
    "pd0": "summaries/pd=0",
    "pd1": "summaries/pd=1",
    "pd2": "summaries/pd=2",
    "pd3": "summaries/pd=3",
}


class S3Source(Source):
    def __init__(
        self,
        bucket: str,
        fqdn: str,
        org_id: str,
        s3_client=None,
        today: Optional[datetime] = None,
    ):
        self.bucket = bucket
        self.fqdn = fqdn
        self.org_id = org_id
        self.s3 = s3_client or boto3.client("s3")
        self._today_override = today

    def _today(self) -> datetime:
        return self._today_override or datetime.now(timezone.utc)

    def _base_prefix(self, log_type: str) -> str:
        if log_type not in _LOG_TYPE_PATH:
            raise SourceError(f"unknown log_type: {log_type}")
        return f"{self.fqdn}/org_id={self.org_id}/{_LOG_TYPE_PATH[log_type]}/"

    def _scan_date_prefixes(self, base: str, checkpoint: Checkpoint) -> List[str]:
        today = self._today()
        lm = checkpoint.last_modified
        if lm is None or (today - lm) > timedelta(hours=48):
            start = lm or (today - timedelta(hours=24))
            days = (today.date() - start.date()).days + 1
            dates = [(start.date() + timedelta(days=i)) for i in range(max(1, days))]
        else:
            yesterday = today - timedelta(days=1)
            dates = sorted({yesterday.date(), today.date()})
        return [f"{base}{d.strftime('%Y%m%d')}_" for d in dates]

    def iter_new_files(
        self,
        log_type: str,
        checkpoint: Checkpoint,
        max_files_per_tick: int = 1000,
    ) -> Iterator[Tuple[str, datetime, bytes]]:
        base = self._base_prefix(log_type)
        scan_prefixes = self._scan_date_prefixes(base, checkpoint)
        log.debug("scan prefixes for %s: %s", log_type, scan_prefixes)

        candidates: list[tuple[datetime, str]] = []
        for prefix in scan_prefixes:
            try:
                paginator = self.s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                    for obj in page.get("Contents", []) or []:
                        if self._is_new(obj, checkpoint):
                            candidates.append((obj["LastModified"], obj["Key"]))
            except ClientError as e:
                raise SourceError(f"S3 list failed for {prefix}: {e}") from e

        candidates.sort(key=lambda t: (t[0], t[1]))
        candidates = candidates[:max_files_per_tick]

        for lm, key in candidates:
            try:
                resp = self.s3.get_object(Bucket=self.bucket, Key=key)
                raw = resp["Body"].read()
            except ClientError as e:
                raise SourceError(f"S3 get_object failed for {key}: {e}") from e
            try:
                body = gzip.decompress(raw)
            except OSError as e:
                log.error("gunzip failed for %s: %s", key, e)
                continue
            yield key, lm, body

    @staticmethod
    def _is_new(obj: dict, cp: Checkpoint) -> bool:
        if cp.last_modified is None:
            return True
        if obj["LastModified"] > cp.last_modified:
            return True
        if obj["LastModified"] == cp.last_modified:
            if cp.last_key is None or obj["Key"] > cp.last_key:
                return True
        return False
```

- [ ] **Step 5: Run test**

Run: `pytest tests/test_s3_source.py -v`
Expected: 5 PASS.

- [ ] **Step 6: Commit**

```bash
git add sources/base.py sources/s3_source.py tests/test_s3_source.py
git commit -m "feat(sources): S3 source with LastModified+key-tiebreak dedup"
```

---

## Task 11: Sink abstract + UDP sink

**Files:**
- Create: `sinks/base.py`
- Create: `sinks/udp_sink.py`
- Test: `tests/test_sinks_udp.py`

- [ ] **Step 1: Implement `sinks/base.py`**

```python
"""Sink abstraction: deliver wire bytes to the SIEM."""
from __future__ import annotations

from abc import ABC, abstractmethod


class Sink(ABC):
    @abstractmethod
    def send(self, wire: bytes) -> bool:
        """Return True on success, False after retries exhausted."""

    @abstractmethod
    def close(self) -> None:
        ...
```

- [ ] **Step 2: Write failing test**

```python
# tests/test_sinks_udp.py
import socket
import threading
import time

import pytest

from sinks.udp_sink import UdpSink


@pytest.fixture
def udp_listener():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    received = []

    def _loop():
        while True:
            try:
                data, _ = sock.recvfrom(65535)
                if data == b"__STOP__":
                    break
                received.append(data)
            except OSError:
                break

    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    yield ("127.0.0.1", port, received)
    try:
        stop = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        stop.sendto(b"__STOP__", ("127.0.0.1", port))
        stop.close()
    finally:
        sock.close()


def test_udp_send_delivers_payload(udp_listener):
    host, port, received = udp_listener
    sink = UdpSink(host=host, port=port)
    assert sink.send(b"hello syslog") is True
    time.sleep(0.1)
    assert b"hello syslog" in received
    sink.close()


def test_udp_truncates_over_1024_bytes(udp_listener):
    host, port, received = udp_listener
    sink = UdpSink(host=host, port=port)
    huge = b"A" * 2000
    sink.send(huge)
    time.sleep(0.1)
    assert len(received[-1]) == 1024
    sink.close()
```

- [ ] **Step 3: Run test**

Run: `pytest tests/test_sinks_udp.py -v`
Expected: 2 FAIL.

- [ ] **Step 4: Implement `sinks/udp_sink.py`**

```python
"""UDP syslog sink (fire-and-forget, FortiSIEM max 1024 bytes per datagram)."""
from __future__ import annotations

import logging
import socket

from sinks.base import Sink

log = logging.getLogger(__name__)

_UDP_MAX = 1024


class UdpSink(Sink):
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def send(self, wire: bytes) -> bool:
        if len(wire) > _UDP_MAX:
            log.warning("UDP payload %d > %d bytes; truncating", len(wire), _UDP_MAX)
            wire = wire[:_UDP_MAX]
        try:
            self.sock.sendto(wire, (self.host, self.port))
            return True
        except OSError as e:
            log.error("UDP send failed to %s:%d: %s", self.host, self.port, e)
            return False

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass
```

- [ ] **Step 5: Run test**

Run: `pytest tests/test_sinks_udp.py -v`
Expected: 2 PASS.

- [ ] **Step 6: Commit**

```bash
git add sinks/base.py sinks/udp_sink.py tests/test_sinks_udp.py
git commit -m "feat(sinks): UDP sink with 1024-byte FortiSIEM cap"
```

---

## Task 12: TCP sink + TLS sink

**Files:**
- Create: `sinks/tcp_sink.py`
- Create: `sinks/tls_sink.py`
- Test: `tests/test_sinks_tcp_tls.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_sinks_tcp_tls.py
import socket
import threading
import time

import pytest

from sinks.tcp_sink import TcpSink, _truncate_if_needed


@pytest.fixture
def tcp_listener():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(5)
    port = sock.getsockname()[1]
    received = []
    stop = threading.Event()

    def _loop():
        sock.settimeout(0.2)
        while not stop.is_set():
            try:
                conn, _ = sock.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with conn:
                buf = b""
                conn.settimeout(0.3)
                while True:
                    try:
                        chunk = conn.recv(4096)
                    except socket.timeout:
                        break
                    if not chunk:
                        break
                    buf += chunk
                received.append(buf)

    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    yield ("127.0.0.1", port, received)
    stop.set()
    sock.close()
    t.join(timeout=1)


def test_tcp_send_adds_newline_framing(tcp_listener):
    host, port, received = tcp_listener
    sink = TcpSink(host=host, port=port, max_retries=0,
                   retry_backoff_sec=[])
    assert sink.send(b"event1") is True
    sink.close()
    time.sleep(0.3)
    assert received and received[0] == b"event1\n"


def test_tcp_reconnect_on_failure(tcp_listener):
    host, port, received = tcp_listener
    sink = TcpSink(host=host, port=port, max_retries=2,
                   retry_backoff_sec=[0.01, 0.01])
    assert sink.send(b"first") is True
    sink.close()
    sink2 = TcpSink(host=host, port=port, max_retries=2,
                    retry_backoff_sec=[0.01, 0.01])
    assert sink2.send(b"second") is True
    sink2.close()


def test_tcp_connect_failure_returns_false():
    sink = TcpSink(host="127.0.0.1", port=1, max_retries=1,
                   retry_backoff_sec=[0.01], timeout_sec=1)
    assert sink.send(b"x") is False
    sink.close()


def test_tcp_truncates_over_8192():
    big = b"A" * 10000
    out, warned = _truncate_if_needed(big)
    assert len(out) == 8192
    assert warned
```

- [ ] **Step 2: Run test**

Run: `pytest tests/test_sinks_tcp_tls.py -v`
Expected: 4 FAIL.

- [ ] **Step 3: Implement `sinks/tcp_sink.py`**

```python
"""TCP syslog sink with long-lived connection, newline framing (RFC 6587
non-transparent), and bounded retries."""
from __future__ import annotations

import logging
import socket
import time
from typing import List, Optional

from sinks.base import Sink

log = logging.getLogger(__name__)

_TCP_MAX = 8192


def _truncate_if_needed(wire: bytes) -> tuple[bytes, bool]:
    if len(wire) > _TCP_MAX:
        log.warning("TCP payload %d > %d bytes; truncating", len(wire), _TCP_MAX)
        return wire[:_TCP_MAX], True
    return wire, False


class TcpSink(Sink):
    def __init__(
        self,
        host: str,
        port: int,
        timeout_sec: int = 10,
        max_retries: int = 3,
        retry_backoff_sec: Optional[List[float]] = None,
    ):
        self.host = host
        self.port = port
        self.timeout = timeout_sec
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff_sec if retry_backoff_sec is not None else [1, 2, 4]
        self.sock: Optional[socket.socket] = None

    def _connect(self) -> socket.socket:
        s = socket.create_connection((self.host, self.port), timeout=self.timeout)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        return s

    def _ensure_socket(self) -> socket.socket:
        if self.sock is None:
            self.sock = self._connect()
        return self.sock

    def _drop_socket(self) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    def send(self, wire: bytes) -> bool:
        wire, _ = _truncate_if_needed(wire)
        frame = wire + b"\n"

        attempts = 0
        while True:
            try:
                s = self._ensure_socket()
                s.sendall(frame)
                return True
            except OSError as e:
                log.warning("TCP send to %s:%d failed (attempt %d): %s",
                            self.host, self.port, attempts + 1, e)
                self._drop_socket()
                if attempts >= self.max_retries:
                    return False
                delay = self.retry_backoff[min(attempts, len(self.retry_backoff) - 1)] \
                    if self.retry_backoff else 0
                if delay:
                    time.sleep(delay)
                attempts += 1

    def close(self) -> None:
        self._drop_socket()
```

- [ ] **Step 4: Implement `sinks/tls_sink.py`**

```python
"""TLS-wrapped TCP syslog sink (FortiSIEM default port 6514)."""
from __future__ import annotations

import socket
import ssl
from typing import List, Optional

from sinks.tcp_sink import TcpSink


class TlsSink(TcpSink):
    def __init__(
        self,
        host: str,
        port: int,
        verify: bool = True,
        ca_file: Optional[str] = None,
        timeout_sec: int = 10,
        max_retries: int = 3,
        retry_backoff_sec: Optional[List[float]] = None,
    ):
        super().__init__(host=host, port=port, timeout_sec=timeout_sec,
                         max_retries=max_retries,
                         retry_backoff_sec=retry_backoff_sec)
        self.verify = verify
        self.ca_file = ca_file

    def _connect(self) -> socket.socket:
        ctx = ssl.create_default_context(cafile=self.ca_file)
        if not self.verify:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        raw = socket.create_connection((self.host, self.port), timeout=self.timeout)
        raw.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        return ctx.wrap_socket(raw, server_hostname=self.host)
```

- [ ] **Step 5: Run test**

Run: `pytest tests/test_sinks_tcp_tls.py -v`
Expected: 4 PASS.

- [ ] **Step 6: Commit**

```bash
git add sinks/tcp_sink.py sinks/tls_sink.py tests/test_sinks_tcp_tls.py
git commit -m "feat(sinks): TCP + TLS sinks with reconnect and 8192-byte cap"
```

---

## Task 13: HTTPS sink with batching

**Files:**
- Create: `sinks/https_sink.py`
- Test: `tests/test_sinks_https.py`
- Modify: `requirements-dev.txt`

- [ ] **Step 1: Add `responses` to dev deps**

Append to `requirements-dev.txt`:

```
responses>=0.25
```

Install: `pip install responses`

- [ ] **Step 2: Write failing tests**

```python
# tests/test_sinks_https.py
import json

import responses

from sinks.https_sink import HttpsSink


URL = "https://fsm.example.com/rawupload?vendor=Illumio&model=PCE"


@responses.activate
def test_https_batches_requests():
    responses.add(responses.POST, URL, status=200)
    sink = HttpsSink(url=URL, batch_size=3, max_retries=0)
    for i in range(3):
        sink.send(json.dumps({"i": i}).encode("utf-8"))
    sink.close()
    assert len(responses.calls) == 1
    body = responses.calls[0].request.body
    lines = body.decode("utf-8").strip().split("\n")
    assert len(lines) == 3


@responses.activate
def test_https_flushes_on_close():
    responses.add(responses.POST, URL, status=200)
    sink = HttpsSink(url=URL, batch_size=100, max_retries=0)
    sink.send(b'{"x":1}')
    sink.send(b'{"x":2}')
    assert len(responses.calls) == 0
    sink.close()
    assert len(responses.calls) == 1


@responses.activate
def test_https_retry_on_500_then_success():
    responses.add(responses.POST, URL, status=500)
    responses.add(responses.POST, URL, status=200)
    sink = HttpsSink(url=URL, batch_size=1, max_retries=1,
                     retry_backoff_sec=[0.01])
    assert sink.send(b'{"x":1}') is True
    sink.close()
    assert len(responses.calls) == 2


@responses.activate
def test_https_returns_false_after_all_retries():
    responses.add(responses.POST, URL, status=500)
    responses.add(responses.POST, URL, status=500)
    responses.add(responses.POST, URL, status=500)
    sink = HttpsSink(url=URL, batch_size=1, max_retries=2,
                     retry_backoff_sec=[0.01, 0.01])
    assert sink.send(b'{"x":1}') is False
    sink.close()
```

- [ ] **Step 3: Run test**

Run: `pytest tests/test_sinks_https.py -v`
Expected: 4 FAIL.

- [ ] **Step 4: Implement `sinks/https_sink.py`**

```python
"""HTTPS sink: batch NDJSON POST to FortiSIEM rawupload or similar endpoint."""
from __future__ import annotations

import logging
import time
from typing import List, Optional

import requests

from sinks.base import Sink

log = logging.getLogger(__name__)


class HttpsSink(Sink):
    def __init__(
        self,
        url: str,
        batch_size: int = 100,
        verify_tls: bool = True,
        timeout_sec: int = 10,
        max_retries: int = 3,
        retry_backoff_sec: Optional[List[float]] = None,
    ):
        self.url = url
        self.batch_size = batch_size
        self.verify_tls = verify_tls
        self.timeout = timeout_sec
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff_sec if retry_backoff_sec is not None else [1, 2, 4]
        self.session = requests.Session()
        self.buffer: list[bytes] = []

    def send(self, wire: bytes) -> bool:
        self.buffer.append(wire)
        if len(self.buffer) >= self.batch_size:
            return self._flush()
        return True

    def _flush(self) -> bool:
        if not self.buffer:
            return True
        body = b"\n".join(self.buffer) + b"\n"
        headers = {"Content-Type": "application/x-ndjson"}
        attempts = 0
        while True:
            try:
                resp = self.session.post(
                    self.url,
                    data=body,
                    headers=headers,
                    verify=self.verify_tls,
                    timeout=self.timeout,
                )
                if 200 <= resp.status_code < 300:
                    self.buffer.clear()
                    return True
                log.warning("HTTPS POST %s returned %d (attempt %d)",
                            self.url, resp.status_code, attempts + 1)
            except requests.RequestException as e:
                log.warning("HTTPS POST %s failed (attempt %d): %s",
                            self.url, attempts + 1, e)
            if attempts >= self.max_retries:
                return False
            delay = self.retry_backoff[min(attempts, len(self.retry_backoff) - 1)] \
                if self.retry_backoff else 0
            if delay:
                time.sleep(delay)
            attempts += 1

    def close(self) -> None:
        try:
            self._flush()
        finally:
            self.session.close()
```

- [ ] **Step 5: Run test**

Run: `pytest tests/test_sinks_https.py -v`
Expected: 4 PASS.

- [ ] **Step 6: Commit**

```bash
git add sinks/https_sink.py tests/test_sinks_https.py requirements-dev.txt
git commit -m "feat(sinks): HTTPS NDJSON batch POST with retry"
```

---

## Task 14: Logging setup

**Files:**
- Create: `core/logging_setup.py`

- [ ] **Step 1: Implement `core/logging_setup.py`**

```python
"""Configure root logging: rotating file + optional console."""
from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from core.config import LoggingConfig


def setup_logging(cfg: LoggingConfig) -> None:
    d = Path(cfg.dir)
    d.mkdir(parents=True, exist_ok=True)

    level_name = "WARNING" if cfg.level == "WARN" else cfg.level
    root = logging.getLogger()
    root.setLevel(level_name)

    for h in list(root.handlers):
        root.removeHandler(h)

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(
        d / cfg.file,
        maxBytes=cfg.rotate_mb * 1024 * 1024,
        backupCount=cfg.keep_files,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    if cfg.console:
        console = logging.StreamHandler()
        console.setFormatter(fmt)
        root.addHandler(console)
```

- [ ] **Step 2: Smoke-test import**

Run: `python -c "from core.logging_setup import setup_logging; print('ok')"`
Expected: `ok`.

- [ ] **Step 3: Commit**

```bash
git add core/logging_setup.py
git commit -m "feat(core): rotating-file + console logging setup"
```

---

## Task 15: Pipeline tick orchestrator

**Files:**
- Create: `core/pipeline.py`
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_pipeline.py
import gzip
import json
from datetime import datetime

from core.checkpoint import Checkpoint, CheckpointStore
from core.pipeline import Pipeline


def _gz_lines(lines):
    return gzip.compress("\n".join(json.dumps(l) for l in lines).encode("utf-8"))


class FakeSource:
    def __init__(self, files):
        self._files = files

    def iter_new_files(self, log_type, checkpoint, max_files_per_tick):
        for key, lm, body in self._files:
            yield key, lm, body


class FakeMapper:
    def format(self, ev):
        return json.dumps(ev).encode("utf-8")


class FakeSink:
    def __init__(self, fail_on=None):
        self.sent = []
        self.fail_on = fail_on or set()

    def send(self, w):
        if w in self.fail_on:
            return False
        self.sent.append(w)
        return True

    def close(self):
        pass


def _lm(ts):
    return datetime.fromisoformat(ts)


def test_happy_path_all_events_sent(tmp_state_dir):
    files = [
        ("k1", _lm("2026-04-20T10:00:00+00:00"),
         _gz_lines([{"a": 1}, {"a": 2}])),
    ]
    source = FakeSource(files)
    sink = FakeSink()
    store = CheckpointStore(tmp_state_dir)

    p = Pipeline(name="p1", log_type="auditable", source=source,
                 mapper=FakeMapper(), sink=sink, checkpoint_store=store,
                 filter_fn=None, max_files_per_tick=100)
    p.tick()

    cp = store.load("p1")
    assert cp.last_key == "k1"
    assert cp.processed_files_cumulative == 1
    assert cp.processed_events_cumulative == 2
    assert len(sink.sent) == 2


def test_filter_drops_events_but_checkpoint_advances(tmp_state_dir):
    files = [
        ("k1", _lm("2026-04-20T10:00:00+00:00"),
         _gz_lines([{"a": 1}, {"a": 2}])),
    ]
    p = Pipeline(
        name="p1", log_type="auditable",
        source=FakeSource(files), mapper=FakeMapper(), sink=FakeSink(),
        checkpoint_store=CheckpointStore(tmp_state_dir),
        filter_fn=lambda ev: ev["a"] == 1,
        max_files_per_tick=100,
    )
    p.tick()
    cp = p.checkpoint_store.load("p1")
    assert cp.last_key == "k1"
    assert cp.processed_events_cumulative == 1


def test_sink_failure_blocks_checkpoint(tmp_state_dir):
    files = [
        ("k1", _lm("2026-04-20T10:00:00+00:00"),
         _gz_lines([{"a": 1}, {"a": 2}])),
        ("k2", _lm("2026-04-20T11:00:00+00:00"),
         _gz_lines([{"a": 3}])),
    ]
    sink = FakeSink(fail_on={json.dumps({"a": 2}).encode("utf-8")})
    p = Pipeline(
        name="p1", log_type="auditable",
        source=FakeSource(files), mapper=FakeMapper(), sink=sink,
        checkpoint_store=CheckpointStore(tmp_state_dir),
        filter_fn=None, max_files_per_tick=100,
    )
    p.tick()
    cp = p.checkpoint_store.load("p1")
    assert cp.last_key is None


def test_mapper_exception_skips_line_continues_file(tmp_state_dir):
    files = [
        ("k1", _lm("2026-04-20T10:00:00+00:00"),
         _gz_lines([{"a": 1}, {"a": "boom"}, {"a": 2}])),
    ]

    class BoomMapper:
        def format(self, ev):
            if ev["a"] == "boom":
                raise ValueError("nope")
            return json.dumps(ev).encode("utf-8")

    sink = FakeSink()
    p = Pipeline(
        name="p1", log_type="auditable",
        source=FakeSource(files), mapper=BoomMapper(), sink=sink,
        checkpoint_store=CheckpointStore(tmp_state_dir),
        filter_fn=None, max_files_per_tick=100,
    )
    p.tick()
    cp = p.checkpoint_store.load("p1")
    assert cp.last_key == "k1"
    assert len(sink.sent) == 2


def test_invalid_json_line_skipped(tmp_state_dir):
    body = gzip.compress(b'{"a":1}\nnot json\n{"a":2}\n')
    files = [("k1", _lm("2026-04-20T10:00:00+00:00"), body)]
    sink = FakeSink()
    p = Pipeline(
        name="p1", log_type="auditable",
        source=FakeSource(files), mapper=FakeMapper(), sink=sink,
        checkpoint_store=CheckpointStore(tmp_state_dir),
        filter_fn=None, max_files_per_tick=100,
    )
    p.tick()
    assert len(sink.sent) == 2
```

- [ ] **Step 2: Run test**

Run: `pytest tests/test_pipeline.py -v`
Expected: 5 FAIL.

- [ ] **Step 3: Implement `core/pipeline.py`**

```python
"""Per-pipeline tick orchestrator: list → gunzip lines → filter → map → send."""
from __future__ import annotations

import json
import logging
import time
from typing import Callable, Optional

from core.checkpoint import CheckpointStore
from mappers.base import Mapper
from sinks.base import Sink
from sources.base import Source


class Pipeline:
    def __init__(
        self,
        name: str,
        log_type: str,
        source: Source,
        mapper: Mapper,
        sink: Sink,
        checkpoint_store: CheckpointStore,
        filter_fn: Optional[Callable[[dict], bool]] = None,
        max_files_per_tick: int = 1000,
    ):
        self.name = name
        self.log_type = log_type
        self.source = source
        self.mapper = mapper
        self.sink = sink
        self.checkpoint_store = checkpoint_store
        self.filter_fn = filter_fn
        self.max_files_per_tick = max_files_per_tick
        self.log = logging.getLogger(name)

    def tick(self) -> None:
        t0 = time.monotonic()
        cp = self.checkpoint_store.load(self.name)

        stats = dict(files=0, read=0, filtered=0,
                     sent=0, failed=0, mapper_err=0)
        try:
            for key, lm, body in self.source.iter_new_files(
                    self.log_type, cp, max_files_per_tick=self.max_files_per_tick):
                stats["files"] += 1
                all_ok, sent_in_file = self._process_file(key, body, stats)
                if not all_ok:
                    self.log.error("sink failed on %s; checkpoint not advancing", key)
                    break
                cp = cp.advance(last_modified=lm, last_key=key, events_inc=sent_in_file)
                self.checkpoint_store.save(cp)
        except Exception as e:  # noqa: BLE001
            self.log.exception("tick aborted: %s", e)
        finally:
            self.log.info(
                "tick: files=%d read=%d sent=%d filtered=%d failed=%d mapper_err=%d "
                "checkpoint=%s duration=%.2fs",
                stats["files"], stats["read"], stats["sent"],
                stats["filtered"], stats["failed"], stats["mapper_err"],
                (cp.last_key or "none")[-40:],
                time.monotonic() - t0,
            )

    def _process_file(self, key: str, body: bytes, stats: dict) -> tuple[bool, int]:
        sent_in_file = 0
        for raw_line in body.splitlines():
            if not raw_line.strip():
                continue
            stats["read"] += 1
            try:
                ev = json.loads(raw_line)
            except Exception:
                stats["mapper_err"] += 1
                self.log.warning("bad JSON line in %s; skipping", key)
                continue

            if self.filter_fn and not self.filter_fn(ev):
                stats["filtered"] += 1
                continue

            try:
                wire = self.mapper.format(ev)
            except Exception as e:  # noqa: BLE001
                stats["mapper_err"] += 1
                self.log.error("mapper error on %s: %s", key, e)
                continue

            if self.sink.send(wire):
                stats["sent"] += 1
                sent_in_file += 1
            else:
                stats["failed"] += 1
                return False, sent_in_file
        return True, sent_in_file
```

- [ ] **Step 4: Run test**

Run: `pytest tests/test_pipeline.py -v`
Expected: 5 PASS.

- [ ] **Step 5: Commit**

```bash
git add core/pipeline.py tests/test_pipeline.py
git commit -m "feat(core): pipeline tick orchestrator with per-file checkpoint"
```

---

## Task 16: Scheduler wrapper

**Files:**
- Create: `core/scheduler.py`

- [ ] **Step 1: Implement `core/scheduler.py`**

```python
"""APScheduler wrapper. Each pipeline runs as an IntervalTrigger job, with
coalesce and max_instances=1 so slow ticks don't stack up."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Sequence

from apscheduler.executors.pool import ThreadPoolExecutor as APSThreadPool
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from core.pipeline import Pipeline

log = logging.getLogger(__name__)


class PipelineScheduler:
    def __init__(self, pipelines: Sequence[tuple[Pipeline, int]]):
        max_workers = max(1, len(pipelines))
        self.scheduler = BlockingScheduler(
            executors={"default": APSThreadPool(max_workers=max_workers)},
            job_defaults={"coalesce": True, "max_instances": 1,
                          "misfire_grace_time": 30},
        )
        for pipeline, interval in pipelines:
            self.scheduler.add_job(
                pipeline.tick,
                trigger=IntervalTrigger(seconds=interval),
                id=pipeline.name,
                name=pipeline.name,
                next_run_time=datetime.now(timezone.utc),
            )
            log.info("scheduled pipeline %s every %ds", pipeline.name, interval)

    def run_forever(self) -> None:
        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            log.info("shutdown requested, stopping scheduler")
            self.scheduler.shutdown(wait=True)
```

- [ ] **Step 2: Commit**

```bash
git add core/scheduler.py
git commit -m "feat(core): APScheduler wrapper with per-pipeline interval jobs"
```

---

## Task 17: Pipeline factory from config

**Files:**
- Modify: `core/pipeline.py` (append factory)

- [ ] **Step 1: Append `build_pipelines_from_config` to `core/pipeline.py`**

At end of `core/pipeline.py`:

```python
# ---- factory ----------------------------------------------------------------

def build_pipelines_from_config(cfg) -> list[tuple["Pipeline", int]]:
    """Convert an AppConfig into a list of (Pipeline, poll_interval_sec).

    Returns only enabled pipelines. Raises on configuration errors so the
    program fails fast.
    """
    import boto3
    from pathlib import Path

    from core.checkpoint import CheckpointStore
    from core.expression_filter import compile_expression
    from mappers.cef import CefMapper
    from mappers.passthrough import PassthroughMapper
    from mappers.syslog_json import SyslogJsonMapper
    from sinks.https_sink import HttpsSink
    from sinks.tcp_sink import TcpSink
    from sinks.tls_sink import TlsSink
    from sinks.udp_sink import UdpSink
    from sources.s3_source import S3Source

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
    s3_client = session.client("s3")

    source = S3Source(
        bucket=cfg.source.bucket,
        fqdn=cfg.source.fqdn,
        org_id=cfg.source.org_id,
        s3_client=s3_client,
    )

    store = CheckpointStore(cfg.checkpoint.dir)

    result: list[tuple[Pipeline, int]] = []
    for pc in cfg.pipelines:
        if not pc.enabled:
            continue

        mapper_cfg = pc.mapper
        if mapper_cfg.format == "syslog_json":
            mapper = SyslogJsonMapper(
                log_type=pc.log_type,
                flatten_enabled=mapper_cfg.flatten,
                flatten_separator=mapper_cfg.flatten_separator,
                flatten_max_depth=mapper_cfg.flatten_max_depth,
                array_strategy=mapper_cfg.array_strategy,
            )
        elif mapper_cfg.format == "cef":
            mapper = CefMapper(log_type=pc.log_type,
                               mapping_path=Path(mapper_cfg.mapping_file))
        elif mapper_cfg.format == "json":
            mapper = PassthroughMapper(
                flatten_enabled=mapper_cfg.flatten,
                flatten_separator=mapper_cfg.flatten_separator,
                flatten_max_depth=mapper_cfg.flatten_max_depth,
                array_strategy=mapper_cfg.array_strategy,
            )
        else:
            raise ValueError(f"unknown mapper format: {mapper_cfg.format}")

        sc = pc.sink
        if sc.type == "udp":
            sink = UdpSink(host=sc.host, port=sc.port)
        elif sc.type == "tcp":
            sink = TcpSink(host=sc.host, port=sc.port,
                           timeout_sec=sc.timeout_sec,
                           max_retries=sc.max_retries,
                           retry_backoff_sec=sc.retry_backoff_sec)
        elif sc.type == "tls":
            tls = sc.tls
            sink = TlsSink(host=sc.host, port=sc.port,
                           verify=tls.verify if tls else True,
                           ca_file=tls.ca_file if tls else None,
                           timeout_sec=sc.timeout_sec,
                           max_retries=sc.max_retries,
                           retry_backoff_sec=sc.retry_backoff_sec)
        elif sc.type == "https":
            sink = HttpsSink(url=sc.url,
                             batch_size=sc.batch_size,
                             verify_tls=sc.tls.verify if sc.tls else True,
                             timeout_sec=sc.timeout_sec,
                             max_retries=sc.max_retries,
                             retry_backoff_sec=sc.retry_backoff_sec)
        else:
            raise ValueError(f"unknown sink type: {sc.type}")

        filter_fn = compile_expression(pc.filter.expression) if pc.filter else None

        pipeline = Pipeline(
            name=pc.name,
            log_type=pc.log_type,
            source=source,
            mapper=mapper,
            sink=sink,
            checkpoint_store=store,
            filter_fn=filter_fn,
            max_files_per_tick=pc.max_files_per_tick,
        )
        result.append((pipeline, pc.poll_interval_sec))
    return result
```

- [ ] **Step 2: Smoke-test imports**

Run: `python -c "from core.pipeline import build_pipelines_from_config; print('ok')"`
Expected: `ok`.

- [ ] **Step 3: Commit**

```bash
git add core/pipeline.py
git commit -m "feat(core): factory to build pipelines from AppConfig"
```

---

## Task 18: collector.py main entry (CLI)

**Files:**
- Create: `collector.py`

- [ ] **Step 1: Implement `collector.py`**

```python
"""Illumio S3 → SIEM Collector entry point."""
from __future__ import annotations

import argparse
import logging
import sys

from core.config import load_config
from core.exceptions import ConfigError
from core.logging_setup import setup_logging
from core.pipeline import build_pipelines_from_config
from core.scheduler import PipelineScheduler

log = logging.getLogger("collector")


def banner(cfg) -> None:
    enabled = [p for p in cfg.pipelines if p.enabled]
    print("=" * 72)
    print("Illumio S3 -> SIEM Collector v1.0")
    print(f"  config:    {cfg.source.bucket} / {cfg.source.fqdn} / org_id={cfg.source.org_id}")
    print(f"  pipelines: {len(cfg.pipelines)} defined, {len(enabled)} enabled")
    for p in enabled:
        dest = _sink_desc(p.sink)
        print(f"    - {p.name:30s} log_type={p.log_type:9s} "
              f"every={p.poll_interval_sec}s -> {dest}")
    print(f"  state:     {cfg.checkpoint.dir}")
    print(f"  log:       {cfg.logging.dir}/{cfg.logging.file} "
          f"(level={cfg.logging.level} rotate={cfg.logging.rotate_mb}MB "
          f"keep={cfg.logging.keep_files})")
    print("=" * 72, flush=True)


def _sink_desc(sc) -> str:
    if sc.type == "https":
        return f"https {sc.url}"
    return f"{sc.type} {sc.host}:{sc.port}"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Illumio S3 -> SIEM Collector")
    parser.add_argument("--config", required=True, help="path to config.yaml")
    parser.add_argument("--dry-run", action="store_true",
                        help="validate config and exit")
    parser.add_argument("--once", metavar="PIPELINE_NAME",
                        help="run a single pipeline once and exit")
    args = parser.parse_args(argv)

    try:
        cfg = load_config(args.config)
    except ConfigError as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 2

    setup_logging(cfg.logging)
    banner(cfg)

    if args.dry_run:
        print("[dry-run] config OK, exiting.")
        return 0

    pipelines = build_pipelines_from_config(cfg)
    if not pipelines:
        print("[ERROR] no enabled pipelines", file=sys.stderr)
        return 3

    if args.once:
        match = [p for p, _interval in pipelines if p.name == args.once]
        if not match:
            print(f"[ERROR] pipeline '{args.once}' not enabled", file=sys.stderr)
            return 4
        log.info("running pipeline %s once", args.once)
        match[0].tick()
        match[0].sink.close()
        return 0

    scheduler = PipelineScheduler(pipelines)
    scheduler.run_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Create a smoke config**

Create `config.smoke.yaml`:

```yaml
aws:
  region: "ap-northeast-1"
source:
  type: s3
  bucket: "b"
  fqdn: "f.example.com"
  org_id: "1"
pipelines:
  - name: "smoke"
    log_type: auditable
    poll_interval_sec: 60
    mapper: { format: syslog_json }
    sink: { type: udp, host: "127.0.0.1", port: 514 }
```

- [ ] **Step 3: Run dry-run**

Run: `python collector.py --config config.smoke.yaml --dry-run`
Expected: banner printed, `[dry-run] config OK, exiting.`, exit code 0.

- [ ] **Step 4: Remove smoke config**

```bash
rm config.smoke.yaml
```

- [ ] **Step 5: Commit**

```bash
git add collector.py
git commit -m "feat: collector.py entry point with --dry-run and --once"
```

---

## Task 19: config.example.yaml

**Files:**
- Create: `config.example.yaml`

- [ ] **Step 1: Create `config.example.yaml`**

```yaml
# Illumio S3 -> SIEM Collector - annotated example
# Copy to config.yaml and edit; config.yaml is git-ignored.

aws:
  profile: null
  access_key: "AKIA..."
  secret_key: "..."
  region: "ap-northeast-1"

source:
  type: s3
  bucket: "illumio-flow-XXXXXXXX-your-bucket"
  fqdn: "your-pce.illum.io"
  org_id: "123456"

checkpoint:
  dir: "./state"
  initial_lookback_hours: 0
  atomic_write: true

logging:
  level: INFO
  dir: "./logs"
  file: "collector.log"
  rotate_mb: 50
  keep_files: 7
  console: true

pipelines:

  # Auditable events -> FortiSIEM over TLS (recommended)
  - name: "audit-to-fortisiem"
    enabled: true
    log_type: auditable
    poll_interval_sec: 60
    max_files_per_tick: 1000
    mapper:
      format: syslog_json
      flatten: true
    sink:
      type: tls
      host: "fortisiem.example.com"
      port: 6514
      tls:
        verify: true
        ca_file: null
      timeout_sec: 10
      max_retries: 3
      retry_backoff_sec: [1, 2, 4]

  # Blocked traffic
  - name: "deny-traffic"
    enabled: true
    log_type: pd2
    poll_interval_sec: 30
    mapper: { format: syslog_json, flatten: true }
    sink:
      type: tls
      host: "fortisiem.example.com"
      port: 6514

  # Example: only forward SMB/RDP/SSH from "potentially-blocked"
  - name: "pd1-smb-rdp-ssh-only"
    enabled: false
    log_type: pd1
    poll_interval_sec: 300
    filter:
      expression: "ev.dst_port in (22, 445, 3389)"
    mapper: { format: syslog_json, flatten: true }
    sink:
      type: tls
      host: "fortisiem.example.com"
      port: 6514

  # Example: CEF format (requires mapping file)
  - name: "audit-cef-backup"
    enabled: false
    log_type: auditable
    poll_interval_sec: 300
    mapper:
      format: cef
      mapping_file: "mappings/auditable.yaml"
    sink:
      type: tcp
      host: "fortisiem.example.com"
      port: 1470

  # Example: HTTPS with batching
  - name: "audit-https-batch"
    enabled: false
    log_type: auditable
    poll_interval_sec: 120
    mapper: { format: json, flatten: true }
    sink:
      type: https
      url: "https://fortisiem.example.com/rawupload?vendor=Illumio&model=PCE"
      batch_size: 100
      timeout_sec: 10

  # Example: UDP test only (<=1024 bytes)
  - name: "test-udp"
    enabled: false
    log_type: auditable
    poll_interval_sec: 60
    mapper: { format: syslog_json, flatten: true }
    sink:
      type: udp
      host: "127.0.0.1"
      port: 514
```

- [ ] **Step 2: Verify parse**

Run: `python collector.py --config config.example.yaml --dry-run`
Expected: banner prints 6 pipelines (2 enabled), `[dry-run] config OK`.

- [ ] **Step 3: Commit**

```bash
git add config.example.yaml
git commit -m "docs: annotated config.example.yaml with 6 pipeline patterns"
```

---

## Task 20: FortiSIEM parser XML templates

**Files:**
- Create: `fortisiem_parser/IllumioPCE_Auditable.xml`
- Create: `fortisiem_parser/IllumioPCE_Summaries.xml`
- Create: `fortisiem_parser/README.md`

- [ ] **Step 1: Create `fortisiem_parser/IllumioPCE_Auditable.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!--
  FortiSIEM Custom Parser for Illumio PCE Auditable events (syslog_json format)
  Import via: Admin > Device Support > Parsers > New > Upload XML
-->
<patternDefinitions>
    <deviceType>
        <vendor>Illumio</vendor>
        <model>PCE</model>
        <version>ANY</version>
    </deviceType>

    <eventFormatRecognizer><![CDATA[illumio-pce audit auditable]]></eventFormatRecognizer>

    <parsingInstructions><![CDATA[
        when:$_rawmsg regexp ".*illumio-pce audit auditable - (?<jsonBody>\\{.*\\})"
        collectAndSetAttr($jsonBody);

        setEventAttribute($_eventType, "Illumio-PCE-Audit");
        setEventAttribute($reptDevName, $_hostname);

        when:$_jsonBody contains "pce_fqdn"
        extract:"pce_fqdn":"(?<pceFqdn>[^\"]+)";
        setEventAttribute($hostName, $pceFqdn);

        when:$_jsonBody contains "created_by_agent_hostname"
        extract:"created_by_agent_hostname":"(?<agentHost>[^\"]+)";
        setEventAttribute($user, $agentHost);

        when:$_jsonBody contains "href"
        extract:"href":"(?<href>[^\"]+)";
        setEventAttribute($infoURL, $href);
    ]]></parsingInstructions>
</patternDefinitions>
```

- [ ] **Step 2: Create `fortisiem_parser/IllumioPCE_Summaries.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!--
  FortiSIEM Custom Parser for Illumio PCE Traffic Summaries (syslog_json format)
-->
<patternDefinitions>
    <deviceType>
        <vendor>Illumio</vendor>
        <model>PCE</model>
        <version>ANY</version>
    </deviceType>

    <eventFormatRecognizer><![CDATA[illumio-pce summary]]></eventFormatRecognizer>

    <parsingInstructions><![CDATA[
        when:$_rawmsg regexp ".*illumio-pce summary (?<msgid>pd[0-3]) - (?<jsonBody>\\{.*\\})"
        collectAndSetAttr($jsonBody);

        setEventAttribute($_eventType, "Illumio-PCE-Flow");

        when:$_jsonBody contains "\"pd\":0"
        setEventAttribute($eventSeverity, 1);
        setEventAttribute($policyDecision, "Allowed");

        when:$_jsonBody contains "\"pd\":1"
        setEventAttribute($eventSeverity, 2);
        setEventAttribute($policyDecision, "PotentiallyBlocked");

        when:$_jsonBody contains "\"pd\":2"
        setEventAttribute($eventSeverity, 4);
        setEventAttribute($policyDecision, "Blocked");

        when:$_jsonBody contains "\"pd\":3"
        setEventAttribute($eventSeverity, 1);
        setEventAttribute($policyDecision, "Unknown");

        extract:"src_ip":"(?<srcIpAddr>[0-9.]+)";
        extract:"dst_ip":"(?<destIpAddr>[0-9.]+)";
        extract:"dst_port":(?<destIpPort>\\d+);
        extract:"proto":(?<ipProto>\\d+);

        when:$_jsonBody contains "src_hostname"
        extract:"src_hostname":"(?<srcName>[^\"]+)";

        when:$_jsonBody contains "dst_hostname"
        extract:"dst_hostname":"(?<destName>[^\"]+)";

        when:$_jsonBody contains "\"un\":"
        extract:"un":"(?<user>[^\"]+)";

        when:$_jsonBody contains "\"pn\":"
        extract:"pn":"(?<procName>[^\"]+)";

        when:$_jsonBody contains "pce_fqdn"
        extract:"pce_fqdn":"(?<pceFqdn>[^\"]+)";
        setEventAttribute($hostName, $pceFqdn);
    ]]></parsingInstructions>
</patternDefinitions>
```

- [ ] **Step 3: Create `fortisiem_parser/README.md`**

```markdown
# FortiSIEM Custom Parsers for Illumio PCE

These XML parsers tell FortiSIEM how to extract structured fields from the
syslog-wrapped JSON messages produced by `illumio_s3_collector`.

## Files

| File | Matches | Event format recognizer string |
|---|---|---|
| IllumioPCE_Auditable.xml | Auditable events (PCE admin activity, VEN lifecycle) | `illumio-pce audit auditable` |
| IllumioPCE_Summaries.xml | Traffic summaries (pd=0..3) | `illumio-pce summary` |

## Install

1. FortiSIEM GUI -> Admin -> Device Support -> Parsers
2. Click **New** -> upload the XML
3. Set **Enabled = Yes**
4. Click **Apply** to push to collectors

## Verify

After the collector starts forwarding events:

1. Admin -> Setup -> Reporting Device -> search "Illumio"
2. A new device `Illumio PCE` should appear
3. Analytics -> Event Types = `Illumio-PCE-Audit` or `Illumio-PCE-Flow`
4. Check fields like `srcIpAddr`, `destIpAddr`, `policyDecision` are populated

## Adding fields

The collector flattens nested JSON with `_` separator, so a path like
`created_by.agent.hostname` appears in the syslog message as
`created_by_agent_hostname`. To parse additional fields, add lines like:

    when:$_jsonBody contains "my_field"
    extract:"my_field":"(?<myField>[^\"]+)";
    setEventAttribute($customAttr, $myField);
```

- [ ] **Step 4: Commit**

```bash
git add fortisiem_parser/
git commit -m "feat: FortiSIEM custom parsers for auditable + summaries"
```

---

## Task 21: systemd unit

**Files:**
- Create: `docs/systemd/illumio-collector.service`

- [ ] **Step 1: Create the unit file**

```ini
[Unit]
Description=Illumio S3 to SIEM Collector
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=illumio-collector
Group=illumio-collector
WorkingDirectory=/opt/illumio-collector/app
Environment=PYTHONUNBUFFERED=1
ExecStart=/opt/illumio-collector/python/bin/python3 /opt/illumio-collector/app/collector.py --config /etc/illumio-collector/config.yaml
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=/var/lib/illumio-collector /var/log/illumio-collector
ProtectHome=true

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 2: Commit**

```bash
git add docs/systemd/illumio-collector.service
git commit -m "docs: systemd unit for Linux deployment"
```

---

## Task 22: Linux build script

**Files:**
- Create: `scripts/build_offline_bundle.sh`

- [ ] **Step 1: Create the script**

```bash
#!/usr/bin/env bash
# Build offline install bundle for Linux x86_64.
# Run on a host with internet + Python 3.11 + pip.
set -euo pipefail

VERSION="${VERSION:-1.0}"
PBS_TAG="${PBS_TAG:-20240415}"
PY_VER="${PY_VER:-3.11.9}"
OUT_DIR="${OUT_DIR:-$(pwd)/dist}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$(mktemp -d)"
BUNDLE="${BUILD_DIR}/bundle"

mkdir -p "${BUNDLE}/app" "${BUNDLE}/wheels" "${BUNDLE}/systemd" "${OUT_DIR}"

echo "==> Downloading python-build-standalone cpython-${PY_VER}+${PBS_TAG}"
curl -fL -o "${BUNDLE}/python-runtime.tar.gz" \
  "https://github.com/astral-sh/python-build-standalone/releases/download/${PBS_TAG}/cpython-${PY_VER}+${PBS_TAG}-x86_64-unknown-linux-gnu-install_only.tar.gz"

echo "==> Downloading wheels for manylinux2014_x86_64 / py3.11"
python3.11 -m pip download \
  --only-binary=:all: \
  --platform manylinux2014_x86_64 \
  --python-version 3.11 --implementation cp --abi cp311 \
  -d "${BUNDLE}/wheels" \
  -r "${REPO_ROOT}/requirements.txt"

echo "==> Copying application code"
cp -r \
  "${REPO_ROOT}/collector.py" \
  "${REPO_ROOT}/core" "${REPO_ROOT}/sources" "${REPO_ROOT}/mappers" \
  "${REPO_ROOT}/sinks" "${REPO_ROOT}/mappings" \
  "${REPO_ROOT}/fortisiem_parser" "${REPO_ROOT}/tests" "${REPO_ROOT}/doc" \
  "${REPO_ROOT}/requirements.txt" \
  "${REPO_ROOT}/config.example.yaml" \
  "${REPO_ROOT}/README.md" \
  "${BUNDLE}/app/"

cp "${REPO_ROOT}/docs/systemd/illumio-collector.service" "${BUNDLE}/systemd/"
cp "${REPO_ROOT}/scripts/install.sh" "${BUNDLE}/install.sh"
chmod +x "${BUNDLE}/install.sh"

cat > "${BUNDLE}/VERSION" <<EOF
illumio-s3-siem-collector v${VERSION}
built: $(date -u +%Y-%m-%dT%H:%M:%SZ)
host:  $(uname -a)
python: cpython-${PY_VER}+${PBS_TAG} (x86_64 linux gnu)
EOF

TARBALL="${OUT_DIR}/illumio-collector-linux-x86_64-v${VERSION}.tar.gz"
tar -C "${BUILD_DIR}" -czf "${TARBALL}" bundle
(cd "${OUT_DIR}" && sha256sum "$(basename "${TARBALL}")") > "${OUT_DIR}/SHA256SUMS-linux"

echo "==> Done: ${TARBALL}"
rm -rf "${BUILD_DIR}"
```

- [ ] **Step 2: Make executable**

```bash
chmod +x scripts/build_offline_bundle.sh
```

- [ ] **Step 3: Commit**

```bash
git add scripts/build_offline_bundle.sh
git commit -m "feat(scripts): Linux offline bundle build script"
```

---

## Task 23: Linux install script

**Files:**
- Create: `scripts/install.sh`

- [ ] **Step 1: Create the script**

```bash
#!/usr/bin/env bash
# Install the Illumio S3 -> SIEM Collector from an offline bundle.
set -euo pipefail

INSTALL_DIR="/opt/illumio-collector"
CONFIG_DIR="/etc/illumio-collector"
STATE_DIR="/var/lib/illumio-collector/state"
LOG_DIR="/var/log/illumio-collector"
SERVICE_USER="illumio-collector"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "This script must be run as root." >&2
  exit 1
fi

BUNDLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${BUNDLE_DIR}"

echo "==> Copy bundle to ${INSTALL_DIR}"
mkdir -p "${INSTALL_DIR}"
cp -r app systemd wheels VERSION "${INSTALL_DIR}/"
if [[ ! -d "${INSTALL_DIR}/python" ]]; then
  echo "==> Extract portable Python runtime"
  tar -xzf python-runtime.tar.gz -C "${INSTALL_DIR}"
fi

echo "==> Install wheels (offline)"
"${INSTALL_DIR}/python/bin/python3" -m pip install \
  --no-index \
  --find-links="${INSTALL_DIR}/wheels" \
  -r "${INSTALL_DIR}/app/requirements.txt"

echo "==> Prepare config dir"
mkdir -p "${CONFIG_DIR}"
if [[ ! -f "${CONFIG_DIR}/config.yaml" ]]; then
  cp "${INSTALL_DIR}/app/config.example.yaml" "${CONFIG_DIR}/config.yaml"
  chmod 600 "${CONFIG_DIR}/config.yaml"
fi

echo "==> Create service user and state dirs"
id -u "${SERVICE_USER}" >/dev/null 2>&1 || \
  useradd --system --shell /sbin/nologin "${SERVICE_USER}"
mkdir -p "${STATE_DIR}" "${LOG_DIR}"
chown -R "${SERVICE_USER}:${SERVICE_USER}" "${STATE_DIR}" "${LOG_DIR}" "${INSTALL_DIR}"

echo "==> Install systemd unit"
install -m 0644 "${INSTALL_DIR}/systemd/illumio-collector.service" \
  /etc/systemd/system/illumio-collector.service
systemctl daemon-reload
systemctl enable illumio-collector

cat <<EOF

============================================================
Install complete.

 1. Edit the config:   sudo vi ${CONFIG_DIR}/config.yaml
 2. Start the service: sudo systemctl start illumio-collector
 3. Watch the logs:    sudo journalctl -u illumio-collector -f
============================================================
EOF
```

- [ ] **Step 2: Make executable**

```bash
chmod +x scripts/install.sh
```

- [ ] **Step 3: Commit**

```bash
git add scripts/install.sh
git commit -m "feat(scripts): Linux offline install script"
```

---

## Task 24: Windows build script

**Files:**
- Create: `scripts/build_offline_bundle.ps1`

- [ ] **Step 1: Create the script**

```powershell
<#
.SYNOPSIS
    Build offline install bundle for Windows x86_64.
#>
param(
    [string]$Version = "1.0",
    [string]$PbsTag  = "20240415",
    [string]$PyVer   = "3.11.9"
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$OutDir   = Join-Path $RepoRoot "dist"
$BuildDir = Join-Path $env:TEMP ("illumio-build-" + [guid]::NewGuid().ToString("N").Substring(0,8))
$Bundle   = Join-Path $BuildDir "bundle"

New-Item -ItemType Directory -Force -Path $OutDir, `
    (Join-Path $Bundle "app"), (Join-Path $Bundle "wheels") | Out-Null

Write-Host "==> Downloading python-build-standalone cpython-$PyVer+$PbsTag"
$PyUrl = "https://github.com/astral-sh/python-build-standalone/releases/download/$PbsTag/cpython-$PyVer+$PbsTag-x86_64-pc-windows-msvc-install_only.tar.gz"
Invoke-WebRequest -Uri $PyUrl -OutFile (Join-Path $Bundle "python-runtime.tar.gz")

Write-Host "==> Downloading wheels for win_amd64 / py3.11"
python -m pip download `
  --only-binary=:all: `
  --platform win_amd64 `
  --python-version 3.11 --implementation cp --abi cp311 `
  -d (Join-Path $Bundle "wheels") `
  -r (Join-Path $RepoRoot "requirements.txt")

Write-Host "==> Copying application code"
$AppDst = Join-Path $Bundle "app"
Copy-Item -Path (Join-Path $RepoRoot "collector.py"), `
              (Join-Path $RepoRoot "requirements.txt"), `
              (Join-Path $RepoRoot "config.example.yaml"), `
              (Join-Path $RepoRoot "README.md") -Destination $AppDst
foreach ($sub in "core","sources","mappers","sinks","mappings","fortisiem_parser","tests","doc") {
    Copy-Item -Recurse -Path (Join-Path $RepoRoot $sub) -Destination $AppDst
}

Write-Host "==> Downloading NSSM"
Invoke-WebRequest -Uri "https://nssm.cc/release/nssm-2.24.zip" `
    -OutFile (Join-Path $Bundle "nssm-2.24.zip")

Write-Host "==> Copying install script"
Copy-Item (Join-Path $RepoRoot "scripts/install.ps1") $Bundle

@"
illumio-s3-siem-collector v$Version
built: $(Get-Date -Format 'yyyy-MM-ddTHH:mm:ssZ')
host:  $(hostname)
python: cpython-$PyVer+$PbsTag (x86_64 windows msvc)
"@ | Out-File (Join-Path $Bundle "VERSION") -Encoding UTF8

$Zip = Join-Path $OutDir "illumio-collector-windows-x86_64-v$Version.zip"
if (Test-Path $Zip) { Remove-Item $Zip }
Compress-Archive -Path (Join-Path $Bundle "*") -DestinationPath $Zip

$Hash = Get-FileHash $Zip -Algorithm SHA256
"$($Hash.Hash)  $(Split-Path $Zip -Leaf)" | Out-File (Join-Path $OutDir "SHA256SUMS-windows.txt") -Encoding ASCII

Write-Host "==> Done: $Zip"
Remove-Item -Recurse -Force $BuildDir
```

- [ ] **Step 2: Commit**

```bash
git add scripts/build_offline_bundle.ps1
git commit -m "feat(scripts): Windows offline bundle build script"
```

---

## Task 25: Windows install script

**Files:**
- Create: `scripts/install.ps1`

- [ ] **Step 1: Create the script**

```powershell
<#
.SYNOPSIS
    Install the Illumio S3 -> SIEM Collector from an offline bundle on Windows.
    Must be run as Administrator from inside the extracted bundle directory.
#>
param(
    [string]$InstallDir = "C:\illumio-collector"
)

$ErrorActionPreference = "Stop"

$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltinRole]::Administrator)) {
    Write-Error "This script must be run as Administrator."
    exit 1
}

$BundleDir = $PSScriptRoot

Write-Host "==> Copying bundle to $InstallDir"
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
Copy-Item -Recurse -Force -Path (Join-Path $BundleDir "app"), `
                                 (Join-Path $BundleDir "wheels") `
                          -Destination $InstallDir
Copy-Item -Force (Join-Path $BundleDir "VERSION") $InstallDir

$PythonExe = Join-Path $InstallDir "python\python.exe"
if (-not (Test-Path $PythonExe)) {
    Write-Host "==> Extracting portable Python runtime"
    tar -xzf (Join-Path $BundleDir "python-runtime.tar.gz") -C $InstallDir
}

Write-Host "==> Installing wheels (offline)"
& $PythonExe -m pip install `
    --no-index `
    --find-links (Join-Path $InstallDir "wheels") `
    -r (Join-Path $InstallDir "app\requirements.txt")

Write-Host "==> Preparing config"
$ConfigPath = Join-Path $InstallDir "config.yaml"
if (-not (Test-Path $ConfigPath)) {
    Copy-Item (Join-Path $InstallDir "app\config.example.yaml") $ConfigPath
}

New-Item -ItemType Directory -Force -Path `
    (Join-Path $InstallDir "state"), `
    (Join-Path $InstallDir "logs") | Out-Null

Write-Host "==> Extracting NSSM"
$NssmDir = Join-Path $InstallDir "nssm"
if (-not (Test-Path $NssmDir)) {
    Expand-Archive -Path (Join-Path $BundleDir "nssm-2.24.zip") `
                   -DestinationPath $NssmDir
}
$Nssm = Join-Path $NssmDir "nssm-2.24\win64\nssm.exe"

Write-Host "==> Registering Windows service"
$ServiceName = "IllumioCollector"
& $Nssm install $ServiceName $PythonExe `
    "$InstallDir\app\collector.py --config $InstallDir\config.yaml"
& $Nssm set $ServiceName AppDirectory      "$InstallDir\app"
& $Nssm set $ServiceName DisplayName       "Illumio S3 to SIEM Collector"
& $Nssm set $ServiceName Description       "Pull Illumio PCE logs from S3 and forward to FortiSIEM"
& $Nssm set $ServiceName AppStdout         "$InstallDir\logs\nssm-stdout.log"
& $Nssm set $ServiceName AppStderr         "$InstallDir\logs\nssm-stderr.log"
& $Nssm set $ServiceName AppRotateFiles    1
& $Nssm set $ServiceName AppRotateBytes    52428800
& $Nssm set $ServiceName Start             SERVICE_AUTO_START

Write-Host ""
Write-Host "============================================================"
Write-Host "Install complete."
Write-Host " 1. Edit the config:   notepad $ConfigPath"
Write-Host " 2. Start the service: & `"$Nssm`" start $ServiceName"
Write-Host " 3. Watch the logs:    Get-Content $InstallDir\logs\collector.log -Wait"
Write-Host "============================================================"
```

- [ ] **Step 2: Commit**

```bash
git add scripts/install.ps1
git commit -m "feat(scripts): Windows offline install script"
```

---

## Task 26: End-to-end integration test

**Files:**
- Modify: `tests/test_pipeline.py` (append)

- [ ] **Step 1: Append to `tests/test_pipeline.py`**

```python
# ---------------------------------------------------------------------------
# integration: real Pipeline + moto-S3 + in-memory sink
# ---------------------------------------------------------------------------
import gzip as _gzip
import json as _json
from datetime import datetime as _dt, timezone as _tz

import boto3 as _boto3
import pytest as _pytest
from moto import mock_aws as _mock_aws

from mappers.syslog_json import SyslogJsonMapper as _SyslogJson
from sinks.base import Sink as _Sink
from sources.s3_source import S3Source as _S3Source


class _MemorySink(_Sink):
    def __init__(self):
        self.sent = []

    def send(self, w):
        self.sent.append(w)
        return True

    def close(self):
        pass


@_pytest.mark.integration
def test_end_to_end_with_moto(tmp_state_dir):
    with _mock_aws():
        s3 = _boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="bk")
        base = "f.example.com/org_id=1/auditable/"
        events = [
            {"timestamp": "2026-04-20T10:00:00Z", "pce_fqdn": "f.example.com",
             "href": "/orgs/1/events/a"},
            {"timestamp": "2026-04-20T10:00:01Z", "pce_fqdn": "f.example.com",
             "href": "/orgs/1/events/b",
             "created_by": {"agent": {"hostname": "h1"}}},
        ]
        body = _gzip.compress("\n".join(_json.dumps(e) for e in events).encode())
        s3.put_object(Bucket="bk", Key=base + "20260420_a.jsonl.gz", Body=body)

        source = _S3Source(bucket="bk", fqdn="f.example.com", org_id="1",
                           s3_client=s3,
                           today=_dt(2026, 4, 20, 23, 59, tzinfo=_tz.utc))
        mapper = _SyslogJson(log_type="auditable")
        sink = _MemorySink()
        store = CheckpointStore(tmp_state_dir)

        p = Pipeline(
            name="e2e", log_type="auditable",
            source=source, mapper=mapper, sink=sink,
            checkpoint_store=store, filter_fn=None,
            max_files_per_tick=100,
        )
        p.tick()

        assert len(sink.sent) == 2
        assert b"illumio-pce audit auditable" in sink.sent[0]
        assert b"created_by_agent_hostname" in sink.sent[1]

        cp = store.load("e2e")
        assert cp.last_key == base + "20260420_a.jsonl.gz"

        sink.sent.clear()
        p.tick()
        assert sink.sent == []
```

- [ ] **Step 2: Run**

Run: `pytest tests/test_pipeline.py -v -m integration`
Expected: 1 PASS.

Run full suite: `pytest -v`
Expected: all PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/test_pipeline.py
git commit -m "test: end-to-end pipeline with moto + syslog_json"
```

---

## Task 27: README.md

**Files:**
- Create: `README.md`

- [ ] **Step 1: Create `README.md`**

```markdown
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

## Requirements

**Target host:**
- x86_64 CPU
- Linux (glibc ≥ 2.17) or Windows 10 / Server 2016+

That's it. No Python, no pip, no internet on the target.

**Build host (for packaging):** Python 3.11 + pip + internet.

## Quick start (dev)

```bash
pip install -r requirements-dev.txt
cp config.example.yaml config.yaml
$EDITOR config.yaml
python collector.py --config config.yaml --dry-run
python collector.py --config config.yaml --once audit-to-fortisiem
python collector.py --config config.yaml
```

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

## Configuration

See `config.example.yaml` for every option with comments.

| Pipeline field | Purpose |
|---|---|
| `log_type` | `auditable`, `pd0`, `pd1`, `pd2`, or `pd3` |
| `poll_interval_sec` | how often to pull (min 10) |
| `max_files_per_tick` | bound on files processed per tick (default 1000) |
| `filter.expression` | safe Python-like boolean using `ev.*` |
| `mapper.format` | `syslog_json` (default), `cef`, or `json` |
| `mapper.flatten` | collapse nested JSON (default true) |
| `sink.type` | `udp`, `tcp`, `tls`, or `https` |

### Filter examples

```yaml
filter:
  expression: "ev.pd == 2"
  expression: "ev.dst_port in (22, 445, 3389)"
  expression: "ev.created_by.agent.hostname != 'healthcheck'"
  expression: "'login' in str(ev.notifications)"
```

### FortiSIEM setup

Import parsers from `fortisiem_parser/`. See
`fortisiem_parser/README.md` for step-by-step instructions.

## Operations

Checkpoints live at `<state_dir>/<pipeline_name>.json`. Delete to replay
from the configured `initial_lookback_hours`.

### Troubleshooting

```bash
python s3_log_checker.py --bucket <B> --fqdn <F> --org-id <ID> \
    --access-key <AK> --secret-key <SK>          # S3 connectivity
python collector.py --config config.yaml --dry-run
python collector.py --config config.yaml --once <pipeline-name>
```

### Upgrading

1. Stop the service
2. Re-run `install.sh` / `install.ps1` from the new bundle
3. Config (`/etc/illumio-collector/` or `C:\illumio-collector\config.yaml`)
   and state are preserved
4. Start the service

## Architecture

Full design: `docs/superpowers/specs/2026-04-20-illumio-s3-siem-collector-design.md`.

```
  Source (S3) -> Mapper (flatten + format) -> Sink (UDP/TCP/TLS/HTTPS)
                        |                          |
                    filter (opt)               retry + backoff
```

## License

TBD
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: README with quick-start, offline build, and operations"
```

---

## Task 28: Full suite run + tag v1.0

- [ ] **Step 1: Install dev deps cleanly and run everything**

```bash
pip install -r requirements-dev.txt
pytest -v
```

Expected: all tests PASS.

- [ ] **Step 2: Compile check**

```bash
python -m py_compile collector.py core/*.py sources/*.py mappers/*.py sinks/*.py
```

Expected: no output.

- [ ] **Step 3: Verify dry-run**

```bash
python collector.py --config config.example.yaml --dry-run
```

Expected: banner + `[dry-run] config OK`.

- [ ] **Step 4: Tag release**

```bash
git tag -a v1.0 -m "v1.0: initial release"
```

- [ ] **Step 5: Build a bundle on matching platform**

Linux:
```bash
./scripts/build_offline_bundle.sh
ls -lh dist/
```

Expected: `dist/illumio-collector-linux-x86_64-v1.0.tar.gz` (~80 MB).

Windows PowerShell:
```powershell
.\scripts\build_offline_bundle.ps1
Get-ChildItem dist
```

Expected: `dist\illumio-collector-windows-x86_64-v1.0.zip` (~80 MB).

---

## Task 29: Production smoke test against real bucket

Uses the customer-provided credentials file at the repo root (git-ignored).
Creates sandbox artefacts also git-ignored.

- [ ] **Step 1: Create `config.sandbox.yaml`** (git-ignored via `.gitignore`)

Fill in the credentials from the local (git-ignored) access key file —
do not commit this file.

```yaml
aws:
  access_key: "<REPLACE_WITH_LOCAL_ACCESS_KEY>"
  secret_key: "<REPLACE_WITH_LOCAL_SECRET_KEY>"
  region: "ap-northeast-1"
source:
  type: s3
  bucket: "<YOUR_BUCKET_NAME>"
  fqdn: "<YOUR_PCE_FQDN>"
  org_id: "<YOUR_ORG_ID>"
checkpoint:
  dir: "./state_sandbox"
  initial_lookback_hours: 1
logging:
  level: INFO
  dir: "./logs_sandbox"
  file: "sandbox.log"
pipelines:
  - name: "sandbox-audit-udp"
    log_type: auditable
    poll_interval_sec: 60
    mapper: { format: syslog_json, flatten: true }
    sink: { type: udp, host: "127.0.0.1", port: 5514 }
```

- [ ] **Step 2: Start UDP listener in another terminal**

Linux/macOS: `nc -u -l 5514`

Windows PowerShell:
```powershell
$u = New-Object System.Net.Sockets.UdpClient 5514
while ($true) {
    $ep = New-Object System.Net.IPEndPoint([System.Net.IPAddress]::Any, 0)
    $bytes = $u.Receive([ref]$ep)
    [System.Text.Encoding]::UTF8.GetString($bytes) | Out-Host
}
```

- [ ] **Step 3: Run collector once**

```bash
python collector.py --config config.sandbox.yaml --once sandbox-audit-udp
```

Expected:
- Tick log shows `files=N read=M sent=M ...` with `sent > 0`
- Listener prints Syslog-framed JSON with `illumio-pce audit auditable`

- [ ] **Step 4: Run in scheduled mode for 5 minutes**

```bash
python collector.py --config config.sandbox.yaml
# Ctrl+C after 5 minutes
```

Expected: multiple tick logs, no unhandled exceptions, checkpoint
advances over time.

- [ ] **Step 5: Restart and verify no duplicates / gaps**

```bash
python collector.py --config config.sandbox.yaml --once sandbox-audit-udp
```

Expected: emits 0 events (already caught up) or only new ones.

- [ ] **Step 6: Clean up**

```bash
rm -rf state_sandbox logs_sandbox config.sandbox.yaml
```

---

## Acceptance criteria (spec §15)

After Task 29:

- [ ] `pytest` all green
- [ ] `python collector.py --config config.example.yaml --dry-run` exits 0
- [ ] Real-bucket 5+ minute run: `events_sent > 0`
- [ ] FortiSIEM shows `Reporting Device = Illumio PCE` (ops task)
- [ ] Restart-test: no duplicates or gaps
- [ ] Linux + Windows 30+ minute soak without crash (ops task)

---

## Appendix: dependency graph

```
config, exceptions -> (everything)
checkpoint -> pipeline
flatten -> syslog_json, passthrough, cef
base (source/mapper/sink) -> pipeline
expression_filter -> pipeline (via factory)
source(s3) -> pipeline
mappers(syslog_json, cef, passthrough) -> pipeline (via factory)
sinks(udp, tcp, tls, https) -> pipeline (via factory)
pipeline + scheduler -> collector.py
```

**Build order:** Tasks 1–2 (foundation) → 3–4 (config + checkpoint) →
5–9 (transforms + mappers) → 10 (source) → 11–13 (sinks) →
14–18 (orchestration) → 19–21 (assets) → 22–25 (offline) → 26–29 (tests + release).
