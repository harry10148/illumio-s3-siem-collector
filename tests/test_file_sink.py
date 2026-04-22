"""Unit tests for FileSink and MultiSink."""
import gzip
import os
import time

import pytest

from sinks.file_sink import FileSink
from sinks.multi_sink import MultiSink


# ── FileSink ──────────────────────────────────────────────────────────────────

def test_creates_file_and_writes_line(tmp_path):
    p = tmp_path / "events.log"
    sink = FileSink(str(p), prefix="TEST: ")
    sink.send(b"hello world")
    sink.flush()
    sink.close()
    assert p.read_bytes() == b"TEST: hello world\n"


def test_strips_trailing_newline_before_prefix(tmp_path):
    p = tmp_path / "events.log"
    sink = FileSink(str(p), prefix="P: ")
    sink.send(b"line\n")
    sink.close()
    assert p.read_bytes() == b"P: line\n"


def test_no_prefix(tmp_path):
    p = tmp_path / "events.log"
    sink = FileSink(str(p), prefix="")
    sink.send(b"bare")
    sink.close()
    assert p.read_bytes() == b"bare\n"


def test_multiple_lines_appended(tmp_path):
    p = tmp_path / "events.log"
    sink = FileSink(str(p), prefix="")
    sink.send(b"a")
    sink.send(b"b")
    sink.close()
    assert p.read_bytes() == b"a\nb\n"


def test_rotation_on_size(tmp_path):
    p = tmp_path / "events.log"
    sink = FileSink(str(p), rotation_mb=1, rotation_hours=24, retention_days=30, prefix="")
    sink._max_bytes = 40  # override to trigger quickly

    sink.send(b"x" * 25)   # 26 bytes with newline — below threshold
    assert not list(tmp_path.glob("events.*.log.gz"))

    sink.send(b"y" * 25)   # 26 bytes → cumulative 52 > 40 → rotate after write
    sink.close()

    gz_files = list(tmp_path.glob("events.*.log.gz"))
    assert len(gz_files) == 1

    # Rotated file must be valid gzip
    with gzip.open(gz_files[0], "rb") as f:
        content = f.read()
    assert b"x" * 25 in content


def test_rotation_on_time(tmp_path):
    p = tmp_path / "events.log"
    sink = FileSink(str(p), rotation_mb=999, rotation_hours=24, retention_days=30, prefix="")
    sink._rotation_secs = 0  # expire immediately

    sink.send(b"trigger")
    sink.close()

    gz_files = list(tmp_path.glob("events.*.log.gz"))
    assert len(gz_files) == 1


def test_cleanup_deletes_old_files(tmp_path):
    p = tmp_path / "events.log"
    sink = FileSink(str(p), retention_days=1, prefix="")

    old_gz = tmp_path / "events.20200101T000000.log.gz"
    old_gz.touch()
    old_time = time.time() - 2 * 86400
    os.utime(old_gz, (old_time, old_time))

    sink._cleanup_old()
    sink.close()

    assert not old_gz.exists()


def test_cleanup_keeps_recent_files(tmp_path):
    p = tmp_path / "events.log"
    sink = FileSink(str(p), retention_days=30, prefix="")

    recent_gz = tmp_path / "events.20260420T000000.log.gz"
    recent_gz.touch()  # mtime = now → younger than 30 days

    sink._cleanup_old()
    sink.close()

    assert recent_gz.exists()


def test_flush_returns_true(tmp_path):
    p = tmp_path / "events.log"
    sink = FileSink(str(p), prefix="")
    sink.send(b"data")
    assert sink.flush() is True
    sink.close()


def test_file_sink_config_parsed_from_pipeline():
    """FileSinkConfig round-trips through PipelineConfig."""
    from core.config import PipelineConfig

    pc = PipelineConfig(**{
        "name": "local",
        "log_type": "auditable",
        "poll_interval_sec": 60,
        "mapper": {"format": "json"},
        "sink": {
            "type": "file",
            "path": "/var/log/illumio/events.log",
            "rotation_mb": 100,
            "rotation_hours": 12,
            "retention_days": 14,
            "prefix": "ILLUMIO_FLOW: ",
        },
    })
    assert pc.sink.type == "file"
    assert pc.sink.rotation_mb == 100
    assert pc.sink.rotation_hours == 12
    assert pc.sink.retention_days == 14
    assert pc.sink.prefix == "ILLUMIO_FLOW: "


def test_multi_sink_config_parsed_from_pipeline():
    """MultiSinkConfig with nested file + tls round-trips through PipelineConfig."""
    from core.config import PipelineConfig

    pc = PipelineConfig(**{
        "name": "dual",
        "log_type": "pd2",
        "poll_interval_sec": 60,
        "mapper": {"format": "syslog_json"},
        "sink": {
            "type": "multi",
            "sinks": [
                {"type": "tls", "host": "siem.example.com", "port": 6514},
                {"type": "file", "path": "/var/log/illumio/events.log"},
            ],
        },
    })
    assert pc.sink.type == "multi"
    assert pc.sink.sinks[0].type == "tls"
    assert pc.sink.sinks[1].type == "file"


# ── MultiSink ─────────────────────────────────────────────────────────────────

class _OkSink:
    def send(self, wire): return True
    def flush(self): return True
    def close(self): pass


class _FailSink:
    def send(self, wire): return False
    def flush(self): return False
    def close(self): pass


def test_multi_all_ok():
    ms = MultiSink([_OkSink(), _OkSink()])
    assert ms.send(b"x") is True
    assert ms.flush() is True


def test_multi_one_ok_one_fail_returns_true():
    ms = MultiSink([_OkSink(), _FailSink()])
    assert ms.send(b"x") is True


def test_multi_all_fail_returns_false():
    ms = MultiSink([_FailSink(), _FailSink()])
    assert ms.send(b"x") is False
    assert ms.flush() is False


def test_multi_close_tolerates_exception():
    class _BrokenSink:
        def close(self): raise RuntimeError("disk gone")

    ms = MultiSink([_BrokenSink()])
    ms.close()  # must not raise
