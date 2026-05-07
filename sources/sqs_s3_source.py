"""SQS-based S3 ingestion: long-poll consumer, log-type routing,
visibility extension, delete-on-success.

The Illumio tenant publishes one SQS queue receiving SNS-wrapped
s3:ObjectCreated:* events for the bucket. This module consumes that queue
and dispatches to per-log_type SqsPipeline instances.
"""
from __future__ import annotations

import gzip
import json
import logging
import threading
import time
from dataclasses import dataclass
from typing import Dict, List

from core.sqs_pipeline import SqsPipeline
from sources.s3_source import path_to_log_type

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
        last_extend_at = time.monotonic()
        # First check uses the initial visibility window; later checks use
        # the extension window since each extend_call resets visibility to
        # `visibility_extension_sec`.
        current_window_sec = self.visibility_timeout_sec

        def extend_if_needed():
            nonlocal last_extend_at, current_window_sec
            elapsed_since_extend = time.monotonic() - last_extend_at
            if elapsed_since_extend >= current_window_sec / 2:
                try:
                    self.sqs.change_message_visibility(
                        QueueUrl=self.queue_url,
                        ReceiptHandle=receipt,
                        VisibilityTimeout=self.visibility_extension_sec,
                    )
                    last_extend_at = time.monotonic()
                    current_window_sec = self.visibility_extension_sec
                except Exception as e:  # noqa: BLE001
                    log.warning("change_message_visibility failed: %s", e)

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
        all_ok = True
        for ref in refs:
            extend_if_needed()
            ok = self._handle_object(ref, extend_if_needed)
            extend_if_needed()
            all_ok = all_ok and ok
        if all_ok:
            self._delete(receipt)

    def _handle_object(self, ref: S3ObjectRef, extend_visibility=lambda: None) -> bool:
        if ref.bucket != self.bucket:
            log.error("bucket mismatch: msg=%s configured=%s; not deleting",
                      ref.bucket, self.bucket)
            return False
        log_type = path_to_log_type(ref.key, self.fqdn, self.org_id)
        if log_type is None:
            log.warning("unknown key path %s; deleting (no Illumio log_type matches)",
                        ref.key)
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
            log.error("s3 get_object failed for %s: %s; not deleting", ref.key, e)
            return False
        try:
            decoded = gzip.decompress(raw)
        except OSError as e:
            log.error("gunzip failed for %s: %s; not deleting (DLQ catches)", ref.key, e)
            return False
        extend_visibility()
        return pipeline.process(decoded)

    def _delete(self, receipt_handle: str) -> None:
        try:
            self.sqs.delete_message(QueueUrl=self.queue_url,
                                    ReceiptHandle=receipt_handle)
        except Exception as e:  # noqa: BLE001
            log.error("delete_message failed: %s", e)

    def run_forever(self) -> None:
        log.info("SQS dispatcher starting, queue=...%s", self.queue_url[-32:])
        while not self._stop_event.is_set():
            try:
                self.consume_one_batch()
            except Exception as e:  # noqa: BLE001
                log.exception("consume batch failed: %s", e)
                self._stop_event.wait(timeout=5)
        log.info("SQS dispatcher stopped")

    def request_stop(self) -> None:
        self._stop_event.set()
