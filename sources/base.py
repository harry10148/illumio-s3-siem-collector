"""Source abstraction: yields (key, last_modified, body_bytes) for unprocessed files."""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterator, Tuple

from core.checkpoint import Checkpoint


class Source(ABC):
    @abstractmethod
    def iter_new_files(
        self,
        log_type: str,
        checkpoint: Checkpoint,
        max_files_per_tick: int = 1000,
    ) -> Iterator[Tuple[str, datetime, bytes]]:
        ...
