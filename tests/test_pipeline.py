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

    def flush(self):
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


def test_tick_recovers_from_corrupt_checkpoint(tmp_state_dir):
    store = CheckpointStore(tmp_state_dir)
    # Write a corrupt JSON file at the checkpoint path before tick runs.
    cp_path = store._path("p1")
    cp_path.write_text("{not valid json", encoding="utf-8")

    source = FakeSource([])  # yields nothing, so no S3 / file processing
    p = Pipeline(
        name="p1", log_type="auditable",
        source=source, mapper=FakeMapper(), sink=FakeSink(),
        checkpoint_store=store, filter_fn=None,
        max_files_per_tick=100, recovery_lookback_hours=12,
    )
    # Must not raise.
    p.tick()

    # Checkpoint was reset to a fresh state, not None.
    cp = store.load("p1")
    assert cp.last_modified is not None
    assert cp.last_key is None
    assert cp.processed_files_cumulative == 0


def test_flush_failure_blocks_checkpoint(tmp_state_dir):
    files = [
        ("k1", _lm("2026-04-20T10:00:00+00:00"),
         _gz_lines([{"a": 1}, {"a": 2}])),
    ]

    class FlushFailSink(FakeSink):
        def flush(self):
            return False

    sink = FlushFailSink()
    p = Pipeline(
        name="p1", log_type="auditable",
        source=FakeSource(files), mapper=FakeMapper(), sink=sink,
        checkpoint_store=CheckpointStore(tmp_state_dir),
        filter_fn=None, max_files_per_tick=100,
    )
    p.tick()
    cp = p.checkpoint_store.load("p1")
    assert cp.last_key is None


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
        s3.create_bucket(Bucket="test-bucket")
        base = "f.example.com/org_id=1/auditable/"
        events = [
            {"timestamp": "2026-04-20T10:00:00Z", "pce_fqdn": "f.example.com",
             "href": "/orgs/1/events/a"},
            {"timestamp": "2026-04-20T10:00:01Z", "pce_fqdn": "f.example.com",
             "href": "/orgs/1/events/b",
             "created_by": {"agent": {"hostname": "h1"}}},
        ]
        body = _gzip.compress("\n".join(_json.dumps(e) for e in events).encode())
        s3.put_object(Bucket="test-bucket", Key=base + "20260420_a.jsonl.gz", Body=body)

        source = _S3Source(bucket="test-bucket", fqdn="f.example.com", org_id="1",
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
