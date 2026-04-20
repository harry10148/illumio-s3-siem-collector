"""JSON passthrough mapper (for HTTPS sinks that want raw events)."""
from __future__ import annotations

import json

from mappers.base import Mapper
from mappers._flatten import flatten


class PassthroughMapper(Mapper):
    def __init__(
        self,
        flatten_enabled: bool = True,
        flatten_separator: str = "_",
        flatten_max_depth: int = 10,
        array_strategy: str = "stringify",
    ):
        self.flatten_enabled = flatten_enabled
        self.flatten_sep = flatten_separator
        self.flatten_max_depth = flatten_max_depth
        self.array_strategy = array_strategy

    def format(self, event: dict) -> bytes:
        if self.flatten_enabled:
            event = flatten(
                event,
                separator=self.flatten_sep,
                max_depth=self.flatten_max_depth,
                array_strategy=self.array_strategy,
            )
        return json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
