import pytest
from pydantic import ValidationError
from core.config import AppConfig, PipelineConfig, NetworkSinkConfig, MapperConfig


def minimal_pipeline_dict(**overrides):
    base = {
        "name": "p1",
        "log_type": "auditable",
        "poll_interval_sec": 60,
        "mapper": {"format": "syslog_json"},
        "sink": {"type": "tls", "host": "fsm.example.com", "port": 6514},
    }
    base.update(overrides)
    return base


def test_minimal_pipeline_is_valid():
    p = PipelineConfig(**minimal_pipeline_dict())
    assert p.enabled is True
    assert p.max_files_per_tick == 1000


def test_poll_interval_below_10_rejected():
    with pytest.raises(ValidationError):
        PipelineConfig(**minimal_pipeline_dict(poll_interval_sec=5))


def test_https_sink_requires_url():
    with pytest.raises(ValidationError):
        NetworkSinkConfig(type="https")


def test_cef_mapper_requires_mapping_file():
    with pytest.raises(ValidationError):
        MapperConfig(format="cef")


def test_udp_sink_needs_host_and_port():
    with pytest.raises(ValidationError):
        NetworkSinkConfig(type="udp", host="x")  # missing port
    with pytest.raises(ValidationError):
        NetworkSinkConfig(type="udp", port=514)  # missing host


def test_log_type_must_be_known():
    with pytest.raises(ValidationError):
        PipelineConfig(**minimal_pipeline_dict(log_type="unknown"))


def test_duplicate_pipeline_names_rejected():
    cfg = {
        "aws": {"region": "ap-northeast-1"},
        "source": {"type": "s3", "bucket": "b", "fqdn": "f", "org_id": "1"},
        "pipelines": [minimal_pipeline_dict(name="p1"),
                      minimal_pipeline_dict(name="p1")],
    }
    with pytest.raises(ValidationError):
        AppConfig(**cfg)
