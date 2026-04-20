"""Configure root logging: rotating file + optional console."""
from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from core.config import LoggingConfig


def setup_logging(cfg: LoggingConfig) -> None:
    d = Path(cfg.dir)
    d.mkdir(parents=True, exist_ok=True)

    level_name = "WARNING" if cfg.level == "WARN" else cfg.level
    root = logging.getLogger()
    root.setLevel(level_name)

    for h in list(root.handlers):
        root.removeHandler(h)

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(
        d / cfg.file,
        maxBytes=cfg.rotate_mb * 1024 * 1024,
        backupCount=cfg.keep_files,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    if cfg.console:
        console = logging.StreamHandler()
        console.setFormatter(fmt)
        root.addHandler(console)
