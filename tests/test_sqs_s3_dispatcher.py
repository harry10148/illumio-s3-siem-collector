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
