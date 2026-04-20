"""CEF mapper with a YAML-defined field map.

Extension values have CEF's required escaping applied:
  '\\' -> '\\\\'
  '='  -> '\\='
The leading Syslog-RFC5424 header mirrors the syslog_json format.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from mappers.base import Mapper
from core.exceptions import ConfigError

_PRI = 134

_PROCID_BY_LOG_TYPE = {
    "auditable": "audit",
    "pd0": "summary", "pd1": "summary", "pd2": "summary", "pd3": "summary",
}


def _escape_ext(value: Any) -> str:
    s = str(value)
    return s.replace("\\", "\\\\").replace("=", "\\=")


def _resolve_path(event: dict, dotted: str) -> Any:
    cur: Any = event
    for part in dotted.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


class CefMapper(Mapper):
    def __init__(
        self,
        log_type: str,
        mapping_path,
        appname: str = "illumio-pce",
    ):
        if log_type not in _PROCID_BY_LOG_TYPE:
            raise ValueError(f"unknown log_type: {log_type}")
        p = Path(mapping_path)
        if not p.is_file():
            raise ConfigError(f"CEF mapping file not found: {p}")
        with p.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        header = data.get("cef_header", {})
        self.vendor = header.get("vendor", "Illumio")
        self.product = header.get("product", "PCE")
        self.version = header.get("version", "1.0")
        self.signature_field = header.get("signature_id_field", "pd")
        self.name_template = header.get("name_template", "Illumio Event")
        self.severity_map = header.get("severity_map", {}) or {}
        self.severity_default = header.get("severity_map_default", 5)
        self.extensions: dict[str, str] = data.get("extensions", {}) or {}

        self.log_type = log_type
        self.procid = _PROCID_BY_LOG_TYPE[log_type]
        self.appname = appname

    def _severity(self, event: dict) -> int:
        key = str(event.get(self.signature_field))
        try:
            return int(self.severity_map.get(key, self.severity_default))
        except (TypeError, ValueError):
            return int(self.severity_default)

    def _signature(self, event: dict) -> str:
        return str(event.get(self.signature_field, "0"))

    def _name(self, event: dict) -> str:
        try:
            return self.name_template.format(**event)
        except KeyError:
            return self.name_template

    def _extensions_str(self, event: dict) -> str:
        pairs = []
        for cef_key, event_field in self.extensions.items():
            if event_field is None:
                continue
            # Keys ending in "Label" are literal CEF label strings, not event field paths.
            if cef_key.endswith("Label"):
                pairs.append(f"{cef_key}={_escape_ext(event_field)}")
                continue
            val = _resolve_path(event, event_field)
            if val is None or val == "":
                continue
            pairs.append(f"{cef_key}={_escape_ext(val)}")
        return " ".join(pairs)

    def format(self, event: dict) -> bytes:
        timestamp = event.get("timestamp") or datetime.now(timezone.utc).isoformat()
        hostname = event.get("pce_fqdn") or "-"

        cef_body = (
            f"CEF:0|{self.vendor}|{self.product}|{self.version}|"
            f"{self._signature(event)}|{self._name(event)}|"
            f"{self._severity(event)}|{self._extensions_str(event)}"
        )

        header = (
            f"<{_PRI}>1 {timestamp} {hostname} {self.appname} "
            f"{self.procid} {self.log_type} - "
        )
        return (header + cef_body).encode("utf-8")
