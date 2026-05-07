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
    assert len(sink.sent) == 2


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
