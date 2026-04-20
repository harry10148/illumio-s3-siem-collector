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
