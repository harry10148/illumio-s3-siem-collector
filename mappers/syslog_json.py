"""RFC5424 Syslog header wrapping a flattened JSON body."""
from __future__ import annotations

import json
from datetime import datetime, timezone

from mappers.base import Mapper
from mappers._flatten import flatten

_PRI = 134  # facility=16 local0 * 8 + severity=6 info

_PROCID_BY_LOG_TYPE = {
    "auditable": "audit",
    "pd0": "summary", "pd1": "summary", "pd2": "summary", "pd3": "summary",
}


class SyslogJsonMapper(Mapper):
    def __init__(
        self,
        log_type: str,
        flatten_enabled: bool = True,
        flatten_separator: str = "_",
        flatten_max_depth: int = 10,
        array_strategy: str = "stringify",
        appname: str = "illumio-pce",
    ):
        if log_type not in _PROCID_BY_LOG_TYPE:
            raise ValueError(f"unknown log_type: {log_type}")
        self.log_type = log_type
        self.procid = _PROCID_BY_LOG_TYPE[log_type]
        self.appname = appname
        self.flatten_enabled = flatten_enabled
        self.flatten_sep = flatten_separator
        self.flatten_max_depth = flatten_max_depth
        self.array_strategy = array_strategy

    def format(self, event: dict) -> bytes:
        timestamp = event.get("timestamp") or datetime.now(timezone.utc).isoformat()
        hostname = event.get("pce_fqdn") or "-"

        body = event
        if self.flatten_enabled:
            body = flatten(
                event,
                separator=self.flatten_sep,
                max_depth=self.flatten_max_depth,
                array_strategy=self.array_strategy,
            )
        msg = json.dumps(body, separators=(",", ":"), ensure_ascii=False)

        header = (
            f"<{_PRI}>1 {timestamp} {hostname} {self.appname} "
            f"{self.procid} {self.log_type} - "
        )
        return (header + msg).encode("utf-8")
