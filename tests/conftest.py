import pytest
from datetime import datetime, timezone


@pytest.fixture
def fixed_now():
    return datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def tmp_state_dir(tmp_path):
    d = tmp_path / "state"
    d.mkdir()
    return d
