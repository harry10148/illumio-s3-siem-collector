from datetime import datetime, timezone, timedelta
from core.checkpoint import Checkpoint, CheckpointStore


def test_checkpoint_load_missing_returns_empty(tmp_state_dir):
    store = CheckpointStore(tmp_state_dir)
    cp = store.load("pipe1")
    assert cp.last_modified is None
    assert cp.last_key is None
    assert cp.processed_files_cumulative == 0


def test_checkpoint_save_and_load_roundtrip(tmp_state_dir):
    store = CheckpointStore(tmp_state_dir)
    lm = datetime(2026, 4, 20, 10, 0, 0, tzinfo=timezone.utc)
    cp = Checkpoint(
        pipeline="pipe1",
        last_modified=lm,
        last_key="a/b/20260420_xxx.jsonl.gz",
        processed_files_cumulative=5,
        processed_events_cumulative=100,
    )
    store.save(cp)

    loaded = store.load("pipe1")
    assert loaded.last_modified == lm
    assert loaded.last_key == cp.last_key
    assert loaded.processed_files_cumulative == 5
    assert loaded.processed_events_cumulative == 100


def test_checkpoint_update_advances_counters(tmp_state_dir):
    cp = Checkpoint(pipeline="p")
    lm = datetime(2026, 4, 20, tzinfo=timezone.utc)
    updated = cp.advance(last_modified=lm, last_key="k", events_inc=42)
    assert updated.processed_files_cumulative == 1
    assert updated.processed_events_cumulative == 42
    assert cp.processed_files_cumulative == 0  # original unchanged


def test_atomic_write_no_partial_on_failure(tmp_state_dir, monkeypatch):
    store = CheckpointStore(tmp_state_dir)
    cp = Checkpoint(pipeline="p1", last_key="k1",
                    last_modified=datetime(2026, 4, 20, tzinfo=timezone.utc))
    store.save(cp)

    def boom(src, dst):
        raise OSError("disk full")
    monkeypatch.setattr("os.replace", boom)

    bad = cp.advance(
        last_modified=datetime(2027, 1, 1, tzinfo=timezone.utc),
        last_key="k2", events_inc=99,
    )
    try:
        store.save(bad)
    except OSError:
        pass
    loaded = store.load("p1")
    assert loaded.last_key == "k1"


def test_fresh_checkpoint_lookback(tmp_state_dir, fixed_now):
    store = CheckpointStore(tmp_state_dir)
    cp = store.fresh("p1", initial_lookback_hours=24, now=fixed_now)
    expected = fixed_now - timedelta(hours=24)
    assert cp.last_modified == expected
    assert cp.last_key is None


def test_fresh_checkpoint_lookback_zero(tmp_state_dir, fixed_now):
    store = CheckpointStore(tmp_state_dir)
    cp = store.fresh("p1", initial_lookback_hours=0, now=fixed_now)
    assert cp.last_modified == fixed_now
