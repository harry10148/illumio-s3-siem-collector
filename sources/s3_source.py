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
