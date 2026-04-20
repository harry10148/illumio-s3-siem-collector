"""UDP syslog sink (fire-and-forget, FortiSIEM max 1024 bytes per datagram)."""
from __future__ import annotations

import logging
import socket

from sinks.base import Sink

log = logging.getLogger(__name__)

_UDP_MAX = 1024


class UdpSink(Sink):
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def send(self, wire: bytes) -> bool:
        if len(wire) > _UDP_MAX:
            log.warning("UDP payload %d > %d bytes; truncating", len(wire), _UDP_MAX)
            wire = wire[:_UDP_MAX]
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
