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


import gzip
import time

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
