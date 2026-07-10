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
    return {"Records": [{
        "eventSource": "aws:s3",
        "s3": {"bucket": {"name": bucket}, "object": {"key": key}},
    }]}


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
        {"eventSource": "aws:s3", "s3": {"bucket": {"name": "b"}, "object": {"key": "k1"}}},
        {"eventSource": "aws:s3", "s3": {"bucket": {"name": "b"}, "object": {"key": "k2"}}},
    ]}
    refs = extract_s3_object_refs(s3_event)
    assert len(refs) == 2
    assert {r.key for r in refs} == {"k1", "k2"}


def test_extract_refs_no_records_returns_empty():
    """S3 sometimes sends s3:TestEvent which has no Records array."""
    refs = extract_s3_object_refs({"Service": "Amazon S3", "Event": "s3:TestEvent"})
    assert refs == []


def test_extract_refs_skips_non_s3_event_source():
    """Records without eventSource=aws:s3 (e.g. lifecycle, replication) are dropped."""
    s3_event = {"Records": [
        {"eventSource": "aws:s3", "s3": {"bucket": {"name": "b"}, "object": {"key": "k1"}}},
        {"eventSource": "aws:lambda", "s3": {"bucket": {"name": "b"}, "object": {"key": "k2"}}},
        {"s3": {"bucket": {"name": "b"}, "object": {"key": "k3"}}},  # no eventSource at all
    ]}
    refs = extract_s3_object_refs(s3_event)
    assert len(refs) == 1
    assert refs[0].key == "k1"


def test_extract_refs_url_decodes_keys():
    """AWS URL-encodes object keys in event payloads (spaces -> '+', special -> %XX);
    we must decode so s3.get_object receives the actual key."""
    s3_event = {"Records": [
        {"eventSource": "aws:s3", "s3": {"bucket": {"name": "b"},
         "object": {"key": "pce/org_id=1/auditable/2026-05-08+12%3A00%3A00.json.gz"}}},
    ]}
    refs = extract_s3_object_refs(s3_event)
    assert refs[0].key == "pce/org_id=1/auditable/2026-05-08 12:00:00.json.gz"


import gzip
import logging
import time
from datetime import datetime, timedelta, timezone

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
    s3.create_bucket(Bucket="test-bucket")
    qurl = sqs.create_queue(QueueName="q")["QueueUrl"]
    return s3, sqs, qurl, "test-bucket"


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
        queue_url=qurl, bucket="test-bucket",
        fqdn="pce.example.com", org_id="1",
        pipelines=[pipeline],
        wait_time_sec=0, max_messages_per_receive=10,
    )
    d.consume_one_batch()

    assert len(sink.sent) == 2
    msgs = sqs.receive_message(QueueUrl=qurl, WaitTimeSeconds=0).get("Messages", [])
    assert msgs == []


@mock_aws
def test_dispatcher_unknown_log_type_deletes_message(aws_env):
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    key = "pce.example.com/org_id=1/garbage/x.json.gz"
    _put_object(s3, bucket, key, [{"a": 1}])
    _enqueue_event(sqs, qurl, bucket, key)

    sink = CapturingSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)

    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="test-bucket", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10)
    d.consume_one_batch()

    assert sink.sent == []
    msgs = sqs.receive_message(QueueUrl=qurl, WaitTimeSeconds=0).get("Messages", [])
    assert msgs == []


@mock_aws
def test_dispatcher_no_enabled_pipeline_deletes_message(aws_env):
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    key = "pce.example.com/org_id=1/summaries/pd=2/2026/05/07/x.json.gz"
    _put_object(s3, bucket, key, [{"a": 1}])
    _enqueue_event(sqs, qurl, bucket, key)

    sink = CapturingSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)

    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="test-bucket", fqdn="pce.example.com", org_id="1",
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
                       bucket="test-bucket", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10,
                       visibility_timeout_sec=1)
    d.consume_one_batch()

    time.sleep(1.5)
    msgs = sqs.receive_message(QueueUrl=qurl, WaitTimeSeconds=0,
                               VisibilityTimeout=1).get("Messages", [])
    assert len(msgs) == 1


@mock_aws
def test_dispatcher_s3_get_object_failure_keeps_message(aws_env):
    """If the S3 object referenced doesn't exist, message NOT deleted."""
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    _enqueue_event(sqs, qurl, bucket, "pce.example.com/org_id=1/auditable/missing.json.gz")

    sink = CapturingSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)
    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="test-bucket", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10, visibility_timeout_sec=1)
    d.consume_one_batch()

    assert sink.sent == []
    time.sleep(1.5)
    msgs = sqs.receive_message(QueueUrl=qurl, WaitTimeSeconds=0,
                               VisibilityTimeout=1).get("Messages", [])
    assert len(msgs) == 1


@mock_aws
def test_dispatcher_gunzip_failure_keeps_message(aws_env):
    """If S3 object isn't gzip, message NOT deleted (DLQ catches)."""
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    key = "pce.example.com/org_id=1/auditable/bad.json.gz"
    s3.put_object(Bucket=bucket, Key=key, Body=b"not gzip")
    _enqueue_event(sqs, qurl, bucket, key)

    sink = CapturingSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)
    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="test-bucket", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10, visibility_timeout_sec=1)
    d.consume_one_batch()

    assert sink.sent == []
    time.sleep(1.5)
    msgs = sqs.receive_message(QueueUrl=qurl, WaitTimeSeconds=0,
                               VisibilityTimeout=1).get("Messages", [])
    assert len(msgs) == 1


@mock_aws
def test_dispatcher_malformed_message_keeps_in_queue(aws_env):
    """Malformed (non-JSON) message body left in queue for DLQ."""
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    sqs.send_message(QueueUrl=qurl, MessageBody="this is not json at all")

    sink = CapturingSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)
    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="test-bucket", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10, visibility_timeout_sec=1)
    d.consume_one_batch()

    time.sleep(1.5)
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
                       bucket="test-bucket", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10,
                       visibility_timeout_sec=1)
    d.consume_one_batch()

    time.sleep(1.5)
    msgs = sqs.receive_message(QueueUrl=qurl, WaitTimeSeconds=0,
                               VisibilityTimeout=1).get("Messages", [])
    assert len(msgs) == 1


@mock_aws
def test_dispatcher_extends_visibility_for_slow_processing(aws_env):
    """If processing exceeds visibility_timeout/2, change_message_visibility is called."""
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    key = "pce.example.com/org_id=1/auditable/x.json.gz"
    _put_object(s3, bucket, key, [{"a": 1}])
    _enqueue_event(sqs, qurl, bucket, key)

    real_change = sqs.change_message_visibility
    calls = []
    def spy_change(**kwargs):
        calls.append(kwargs)
        return real_change(**kwargs)
    sqs.change_message_visibility = spy_change

    class SlowSink(CapturingSink):
        def send(self, w):
            time.sleep(0.6)
            return super().send(w)
    sink = SlowSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)
    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="test-bucket", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10,
                       visibility_timeout_sec=1,
                       visibility_extension_sec=10)
    d.consume_one_batch()

    assert len(calls) >= 1
    assert calls[0]["VisibilityTimeout"] == 10


@mock_aws
def test_dispatcher_stops_immediately_when_event_set_first(aws_env):
    """request_stop() before run_forever returns promptly without processing."""
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    sink = CapturingSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)
    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="test-bucket", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10)
    d.request_stop()
    start = time.monotonic()
    d.run_forever()
    elapsed = time.monotonic() - start
    assert elapsed < 2.0, f"run_forever should return promptly, took {elapsed:.2f}s"


@mock_aws
def test_dispatcher_finishes_in_flight_message_before_stop(aws_env):
    """A stop signal arriving mid-process does not abandon the current message."""
    from sources.sqs_s3_source import SqsS3Dispatcher
    s3, sqs, qurl, bucket = _fixture_setup(aws_env)
    key = "pce.example.com/org_id=1/auditable/x.json.gz"
    _put_object(s3, bucket, key, [{"a": 1}])
    _enqueue_event(sqs, qurl, bucket, key)

    captured = []
    holder = {}

    class StopperSink:
        def send(self, w):
            captured.append(w)
            holder["d"].request_stop()
            return True
        def flush(self): return True
        def close(self): pass

    sink = StopperSink()
    pipeline = _make_sqs_pipeline("audit", "auditable", sink)
    d = SqsS3Dispatcher(sqs_client=sqs, s3_client=s3, queue_url=qurl,
                       bucket="test-bucket", fqdn="pce.example.com", org_id="1",
                       pipelines=[pipeline], wait_time_sec=0,
                       max_messages_per_receive=10)
    holder["d"] = d
    d.run_forever()

    # Message was processed (sink saw it) and deleted (in-flight finished)
    assert len(captured) == 1
    msgs = sqs.receive_message(QueueUrl=qurl, WaitTimeSeconds=0).get("Messages", [])
    assert msgs == []


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


@mock_aws
def test_dispatcher_heartbeat_logged(aws_env, caplog):
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
