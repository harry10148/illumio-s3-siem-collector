"""TCP syslog sink with long-lived connection, newline framing (RFC 6587
non-transparent), and bounded retries."""
from __future__ import annotations

import logging
import socket
import time
from typing import List, Optional

from sinks.base import Sink

log = logging.getLogger(__name__)

_TCP_MAX = 8192


def _truncate_if_needed(wire: bytes) -> tuple[bytes, bool]:
    if len(wire) > _TCP_MAX:
        log.warning("TCP payload %d > %d bytes; truncating", len(wire), _TCP_MAX)
        return wire[:_TCP_MAX], True
    return wire, False


class TcpSink(Sink):
    def __init__(
        self,
        host: str,
        port: int,
        timeout_sec: int = 10,
        max_retries: int = 3,
        retry_backoff_sec: Optional[List[float]] = None,
    ):
        self.host = host
        self.port = port
        self.timeout = timeout_sec
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff_sec if retry_backoff_sec is not None else [1, 2, 4]
        self.sock: Optional[socket.socket] = None

    def _connect(self) -> socket.socket:
        s = socket.create_connection((self.host, self.port), timeout=self.timeout)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        return s

    def _ensure_socket(self) -> socket.socket:
        if self.sock is None:
            self.sock = self._connect()
        return self.sock

    def _drop_socket(self) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    def send(self, wire: bytes) -> bool:
        wire, _ = _truncate_if_needed(wire)
        frame = wire + b"\n"

        attempts = 0
        while True:
            try:
                s = self._ensure_socket()
                s.sendall(frame)
                return True
            except OSError as e:
                log.warning("TCP send to %s:%d failed (attempt %d): %s",
                            self.host, self.port, attempts + 1, e)
                self._drop_socket()
                if attempts >= self.max_retries:
                    return False
                delay = self.retry_backoff[min(attempts, len(self.retry_backoff) - 1)] \
                    if self.retry_backoff else 0
                if delay:
                    time.sleep(delay)
                attempts += 1

    def close(self) -> None:
        self._drop_socket()
