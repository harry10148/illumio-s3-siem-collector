"""Per-pipeline tick orchestrator: list → gunzip lines → filter → map → send."""
from __future__ import annotations

import gzip
import json
import logging
import time
from typing import Callable, Optional

from core.checkpoint import CheckpointStore
from mappers.base import Mapper
from sinks.base import Sink
from sources.base import Source


class Pipeline:
    def __init__(
        self,
        name: str,
        log_type: str,
        source: Source,
        mapper: Mapper,
        sink: Sink,
        checkpoint_store: CheckpointStore,
        filter_fn: Optional[Callable[[dict], bool]] = None,
        max_files_per_tick: int = 1000,
    ):
        self.name = name
        self.log_type = log_type
        self.source = source
        self.mapper = mapper
        self.sink = sink
        self.checkpoint_store = checkpoint_store
        self.filter_fn = filter_fn
        self.max_files_per_tick = max_files_per_tick
        self.log = logging.getLogger(name)

    def tick(self) -> None:
        t0 = time.monotonic()
        cp = self.checkpoint_store.load(self.name)

        stats = dict(files=0, read=0, filtered=0,
                     sent=0, failed=0, mapper_err=0)
        try:
            for key, lm, body in self.source.iter_new_files(
                    self.log_type, cp, max_files_per_tick=self.max_files_per_tick):
                stats["files"] += 1
                all_ok, sent_in_file = self._process_file(key, body, stats)
                if not all_ok:
                    self.log.error("sink failed on %s; checkpoint not advancing", key)
                    break
                cp = cp.advance(last_modified=lm, last_key=key, events_inc=sent_in_file)
                self.checkpoint_store.save(cp)
        except Exception as e:  # noqa: BLE001
            self.log.exception("tick aborted: %s", e)
        finally:
            self.log.info(
                "tick: files=%d read=%d sent=%d filtered=%d failed=%d mapper_err=%d "
                "checkpoint=%s duration=%.2fs",
                stats["files"], stats["read"], stats["sent"],
                stats["filtered"], stats["failed"], stats["mapper_err"],
                (cp.last_key or "none")[-40:],
                time.monotonic() - t0,
            )

    def _process_file(self, key: str, body: bytes, stats: dict) -> tuple[bool, int]:
        sent_in_file = 0
        try:
            body = gzip.decompress(body)
        except Exception:
            pass  # not gzipped; use as-is
        for raw_line in body.splitlines():
            if not raw_line.strip():
                continue
            stats["read"] += 1
            try:
                ev = json.loads(raw_line)
            except Exception:
                stats["mapper_err"] += 1
                self.log.warning("bad JSON line in %s; skipping", key)
                continue

            if self.filter_fn and not self.filter_fn(ev):
                stats["filtered"] += 1
                continue

            try:
                wire = self.mapper.format(ev)
            except Exception as e:  # noqa: BLE001
                stats["mapper_err"] += 1
                self.log.error("mapper error on %s: %s", key, e)
                continue

            if self.sink.send(wire):
                stats["sent"] += 1
                sent_in_file += 1
            else:
                stats["failed"] += 1
                return False, sent_in_file
        return True, sent_in_file
