"""UDP syslog sink (fire-and-forget).

Default max_bytes=8192 fits typical syslog messages without truncation.
For strict no-fragmentation on Ethernet LANs use max_bytes=1472.
Set max_bytes=0 to disable the limit entirely (not recommended).
"""
from __future__ import annotations

import logging
import socket

from sinks.base import Sink

log = logging.getLogger(__name__)

_UDP_DEFAULT_MAX = 8192


class UdpSink(Sink):
    def __init__(self, host: str, port: int, max_bytes: int = _UDP_DEFAULT_MAX):
        self.host = host
        self.port = port
        self.max_bytes = max_bytes
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def send(self, wire: bytes) -> bool:
        if self.max_bytes and len(wire) > self.max_bytes:
            log.warning(
                "UDP payload %d > max_bytes=%d; truncating. "
                "Consider switching to tcp/tls for large events.",
                len(wire), self.max_bytes,
            )
            wire = wire[:self.max_bytes]
        try:
            self.sock.sendto(wire, (self.host, self.port))
            return True
        except OSError as e:
            log.error("UDP send failed to %s:%d: %s", self.host, self.port, e)
            return False

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass
