"""SQS-mode per-log_type pipeline.

Pure transformer: given a decoded body (JSON-lines), runs each event through
filter -> mapper -> sink. No checkpoint, no SQS knowledge. The dispatcher
(sources/sqs_s3_source.py) owns SQS lifecycle.
"""
from __future__ import annotations

import json
import logging
from typing import Callable, Optional

from mappers.base import Mapper
from sinks.base import Sink


class SqsPipeline:
    def __init__(
        self,
        name: str,
        log_type: str,
        mapper: Mapper,
        sink: Sink,
        filter_fn: Optional[Callable[[dict], bool]] = None,
    ):
        self.name = name
        self.log_type = log_type
        self.mapper = mapper
        self.sink = sink
        self.filter_fn = filter_fn
        self.log = logging.getLogger(name)
        self.last_stats: dict = {"read": 0, "filtered": 0, "sent": 0,
                                  "mapper_err": 0, "failed": 0}

    def process(self, body: bytes) -> bool:
        """Run JSON-lines body through filter/mapper/sink.

        Returns True iff every non-filtered event was sent and the final
        flush succeeded. On False, the dispatcher should NOT delete the
        SQS message.
        """
        stats = {"read": 0, "filtered": 0, "sent": 0, "mapper_err": 0, "failed": 0}
        sent_in_msg = 0
        try:
            for raw_line in body.splitlines():
                if not raw_line.strip():
                    continue
                stats["read"] += 1
                try:
                    ev = json.loads(raw_line)
                except Exception:
                    stats["mapper_err"] += 1
                    self.log.warning("bad JSON line; skipping")
                    continue

                if self.filter_fn and not self.filter_fn(ev):
                    stats["filtered"] += 1
                    continue

                try:
                    wire = self.mapper.format(ev)
                except Exception as e:  # noqa: BLE001
                    stats["mapper_err"] += 1
                    self.log.error("mapper error: %s", e)
                    continue

                if self.sink.send(wire):
                    stats["sent"] += 1
                    sent_in_msg += 1
                else:
                    stats["failed"] += 1
                    return False
            if sent_in_msg and not self.sink.flush():
                stats["failed"] += 1
                self.log.error("sink flush failed")
                return False
            return True
        finally:
            self.last_stats = stats
            self.log.info(
                "process: read=%d sent=%d filtered=%d mapper_err=%d failed=%d",
                stats["read"], stats["sent"], stats["filtered"],
                stats["mapper_err"], stats["failed"],
            )
