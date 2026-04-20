"""Per-pipeline checkpoint: last-processed (LastModified, Key) persisted atomically."""
from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from core.exceptions import CheckpointError


@dataclass(frozen=True)
class Checkpoint:
    pipeline: str
    last_modified: Optional[datetime] = None
    last_key: Optional[str] = None
    processed_files_cumulative: int = 0
    processed_events_cumulative: int = 0

    def advance(self, last_modified: datetime, last_key: str,
                events_inc: int) -> "Checkpoint":
        return replace(
            self,
            last_modified=last_modified,
            last_key=last_key,
            processed_files_cumulative=self.processed_files_cumulative + 1,
            processed_events_cumulative=self.processed_events_cumulative + events_inc,
        )

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "last_modified": self.last_modified.isoformat() if self.last_modified else None,
            "last_key": self.last_key,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "processed_files_cumulative": self.processed_files_cumulative,
            "processed_events_cumulative": self.processed_events_cumulative,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Checkpoint":
        lm = d.get("last_modified")
        return cls(
            pipeline=d["pipeline"],
            last_modified=datetime.fromisoformat(lm) if lm else None,
            last_key=d.get("last_key"),
            processed_files_cumulative=d.get("processed_files_cumulative", 0),
            processed_events_cumulative=d.get("processed_events_cumulative", 0),
        )


class CheckpointStore:
    def __init__(self, directory: str | Path):
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)

    def _path(self, pipeline: str) -> Path:
        safe = pipeline.replace("/", "_").replace("\\", "_")
        return self.dir / f"{safe}.json"

    def load(self, pipeline: str) -> Checkpoint:
        p = self._path(pipeline)
        if not p.is_file():
            return Checkpoint(pipeline=pipeline)
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            return Checkpoint.from_dict(data)
        except Exception as e:
            raise CheckpointError(f"unable to read {p}: {e}") from e

    def save(self, cp: Checkpoint) -> None:
        final = self._path(cp.pipeline)
        tmp_fd, tmp_path = tempfile.mkstemp(dir=self.dir, prefix=final.name + ".", suffix=".tmp")
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as fp:
                json.dump(cp.to_dict(), fp, indent=2)
            os.replace(tmp_path, final)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def fresh(self, pipeline: str, initial_lookback_hours: int,
              now: Optional[datetime] = None) -> Checkpoint:
        now = now or datetime.now(timezone.utc)
        return Checkpoint(
            pipeline=pipeline,
            last_modified=now - timedelta(hours=initial_lookback_hours),
            last_key=None,
        )
