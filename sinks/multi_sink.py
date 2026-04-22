"""Fan-out sink: delivers each event to all child sinks.

Returns False only when **every** child sink fails, so a temporary SIEM
outage does not block local file writes and vice-versa.  Partial failures
are logged as warnings.
"""
from __future__ import annotations

import logging
from typing import List

from sinks.base import Sink

log = logging.getLogger(__name__)


class MultiSink(Sink):
    def __init__(self, sinks: List[Sink]):
        self._sinks = sinks

    def send(self, wire: bytes) -> bool:
        results = []
        for s in self._sinks:
            ok = s.send(wire)
            if not ok:
                log.warning("%s.send failed", type(s).__name__)
            results.append(ok)
        return any(results)

    def flush(self) -> bool:
        results = []
        for s in self._sinks:
            ok = s.flush()
            if not ok:
                log.warning("%s.flush failed", type(s).__name__)
            results.append(ok)
        return any(results)

    def close(self) -> None:
        for s in self._sinks:
            try:
                s.close()
            except Exception as exc:  # noqa: BLE001
                log.warning("%s.close error: %s", type(s).__name__, exc)
