"""Local rolling-file sink.

Events are appended as single lines (optionally prefixed) to an active log
file.  Rotation triggers when the file exceeds *rotation_mb* MB or has been
open longer than *rotation_hours* hours.  Rotated files are gzip-compressed
and named ``<stem>.<YYYYMMDDThhmmss>.log.gz``.  Files older than
*retention_days* days are deleted automatically.

The active file is always plain text so that FortiSIEM Agent / Filebeat /
other monitoring tools can tail it without decompression.
"""
from __future__ import annotations

import gzip
import logging
import os
import shutil
import time
from datetime import datetime
from pathlib import Path

from sinks.base import Sink

log = logging.getLogger(__name__)


class FileSink(Sink):
    def __init__(
        self,
        path: str,
        rotation_mb: int = 200,
        rotation_hours: int = 24,
        retention_days: int = 30,
        prefix: str = "ILLUMIO_FLOW: ",
    ):
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._max_bytes = rotation_mb * 1024 * 1024
        self._rotation_secs = rotation_hours * 3600
        self._retention_secs = retention_days * 86400
        self._prefix = prefix.encode("utf-8") if prefix else b""

        self._fh = self._path.open("ab")
        if self._path.exists():
            st = self._path.stat()
            self._size = st.st_size
            # Preserve real file age across restarts so rotation_hours still fires.
            self._file_opened_at = st.st_mtime
        else:
            self._size = 0
            self._file_opened_at = time.time()

    # ── public interface ──────────────────────────────────────────────────────

    def send(self, wire: bytes) -> bool:
        line = self._prefix + wire.rstrip(b"\n") + b"\n"
        try:
            self._fh.write(line)
            self._size += len(line)
        except OSError as exc:
            log.error("file write error: %s", exc)
            return False
        if self._needs_rotation():
            self._rotate()
        return True

    def flush(self) -> bool:
        try:
            self._fh.flush()
            return True
        except OSError as exc:
            log.error("file flush error: %s", exc)
            return False

    def close(self) -> None:
        try:
            self._fh.flush()
            self._fh.close()
        except OSError:
            pass

    # ── internals ─────────────────────────────────────────────────────────────

    def _needs_rotation(self) -> bool:
        return (
            self._size >= self._max_bytes
            or (time.time() - self._file_opened_at) >= self._rotation_secs
        )

    def _rotate(self) -> None:
        try:
            self._fh.close()
        except OSError:
            pass

        ts = datetime.now().strftime("%Y%m%dT%H%M%S")
        stem = self._path.stem
        rotated_plain = self._path.parent / f"{stem}.{ts}.log"
        rotated_gz = Path(str(rotated_plain) + ".gz")

        try:
            self._path.rename(rotated_plain)
            with open(rotated_plain, "rb") as f_in, gzip.open(rotated_gz, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            rotated_plain.unlink()
            log.info("rotated log → %s", rotated_gz.name)
        except OSError as exc:
            log.error("rotation error: %s", exc)

        self._cleanup_old()
        self._fh = self._path.open("ab")
        self._size = 0
        self._file_opened_at = time.time()

    def _cleanup_old(self) -> None:
        cutoff = time.time() - self._retention_secs
        stem = self._path.stem
        for f in self._path.parent.glob(f"{stem}.*.log.gz"):
            try:
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    log.info("deleted expired log: %s", f.name)
            except OSError:
                pass
