"""HTTPS sink: batch NDJSON POST to SIEM rawupload or similar endpoint."""
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
