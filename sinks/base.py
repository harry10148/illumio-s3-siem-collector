"""Sink abstraction: deliver wire bytes to the SIEM."""
from __future__ import annotations

from abc import ABC, abstractmethod


class Sink(ABC):
    @abstractmethod
    def send(self, wire: bytes) -> bool:
        """Return True on success, False after retries exhausted."""

    @abstractmethod
    def close(self) -> None:
        ...
