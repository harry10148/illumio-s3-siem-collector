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
