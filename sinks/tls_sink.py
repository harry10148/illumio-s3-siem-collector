"""TLS-wrapped TCP syslog sink (SIEM default port 6514)."""
from __future__ import annotations

import socket
import ssl
from typing import List, Optional

from sinks.tcp_sink import TcpSink


class TlsSink(TcpSink):
    def __init__(
        self,
        host: str,
        port: int,
        verify: bool = True,
        ca_file: Optional[str] = None,
        timeout_sec: int = 10,
        max_retries: int = 3,
        retry_backoff_sec: Optional[List[float]] = None,
    ):
        super().__init__(host=host, port=port, timeout_sec=timeout_sec,
                         max_retries=max_retries,
                         retry_backoff_sec=retry_backoff_sec)
        self.verify = verify
        self.ca_file = ca_file

    def _connect(self) -> socket.socket:
        ctx = ssl.create_default_context(cafile=self.ca_file)
        if not self.verify:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        raw = socket.create_connection((self.host, self.port), timeout=self.timeout)
        raw.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        return ctx.wrap_socket(raw, server_hostname=self.host)
