"""Fan-out sink: delivers each event to all child sinks.

With require="any" (default): Returns False only when **every** child sink fails,
so a temporary SIEM outage does not block local file writes and vice-versa.
Partial failures are logged as warnings.

With require="all": Returns False if **any** child sink fails, so all sinks must
succeed for checkpoint to advance. Ensures durability across all backends.
"""
from __future__ import annotations

import logging
from typing import List

from sinks.base import Sink

log = logging.getLogger(__name__)


class MultiSink(Sink):
    def __init__(self, sinks: List[Sink], require: str = "any"):
        self._sinks = sinks
        self.require = require

    def _combine(self, results: List[bool]) -> bool:
        return all(results) if self.require == "all" else any(results)

    def send(self, wire: bytes) -> bool:
        results = []
        for s in self._sinks:
            ok = s.send(wire)
            if not ok:
                log.warning("%s.send failed", type(s).__name__)
            results.append(ok)
        return self._combine(results)

    def flush(self) -> bool:
        results = []
        for s in self._sinks:
            ok = s.flush()
            if not ok:
                log.warning("%s.flush failed", type(s).__name__)
            results.append(ok)
        return self._combine(results)

    def close(self) -> None:
        for s in self._sinks:
            try:
                s.close()
            except Exception as exc:  # noqa: BLE001
                log.warning("%s.close error: %s", type(s).__name__, exc)
