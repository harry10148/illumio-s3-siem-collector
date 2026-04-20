"""Abstract mapper: turn a parsed event dict into wire bytes for a sink."""
from __future__ import annotations

from abc import ABC, abstractmethod


class Mapper(ABC):
    """Subclasses produce bytes ready to hand to a Sink."""

    @abstractmethod
    def format(self, event: dict) -> bytes:
        ...
