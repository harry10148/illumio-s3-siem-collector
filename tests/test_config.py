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


def test_load_sqs_source_config(tmp_path):
    """sqs_s3 source type parses with required fields."""
    cfg_text = """
aws: { region: us-east-1 }
source:
  type: sqs_s3
  queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/q
  bucket: b
  fqdn: pce.example.com
  org_id: "1"
checkpoint: { dir: ./state, initial_lookback_hours: 24 }
logging: { dir: ./logs, level: INFO }
pipelines:
  - name: p1
    log_type: auditable
    poll_interval_sec: 60
    mapper: { format: syslog_json }
    sink: { type: tls, host: fsm.example.com, port: 6514 }
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(cfg_text)
    from core.config import load_config
    cfg = load_config(str(p))
    assert cfg.source.type == "sqs_s3"
    assert cfg.source.queue_url.startswith("https://sqs.")
    assert cfg.source.visibility_timeout_sec == 60
    assert cfg.source.wait_time_sec == 20
    assert cfg.source.max_messages_per_receive == 10
    assert cfg.source.max_workers == 1


def test_load_generic_s3_still_works(tmp_path):
    """Existing s3 source type unaffected by discriminated union."""
    cfg_text = """
aws: { region: us-east-1 }
source: { type: s3, bucket: b, fqdn: pce.example.com, org_id: "1" }
checkpoint: { dir: ./state, initial_lookback_hours: 24 }
logging: { dir: ./logs, level: INFO }
pipelines:
  - name: p1
    log_type: auditable
    poll_interval_sec: 60
    mapper: { format: syslog_json }
    sink: { type: tls, host: fsm.example.com, port: 6514 }
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(cfg_text)
    from core.config import load_config
    cfg = load_config(str(p))
    assert cfg.source.type == "s3"
    assert cfg.source.bucket == "b"


def test_unknown_source_type_rejected(tmp_path):
    """Pydantic discriminator rejects unknown source types."""
    cfg_text = """
aws: { region: us-east-1 }
source: { type: sqs_raw, queue_url: x, bucket: b, fqdn: x, org_id: "1" }
checkpoint: { dir: ./state, initial_lookback_hours: 24 }
logging: { dir: ./logs, level: INFO }
pipelines:
  - name: p1
    log_type: auditable
    poll_interval_sec: 60
    mapper: { format: syslog_json }
    sink: { type: tls, host: fsm.example.com, port: 6514 }
"""
    p = tmp_path / "cfg.yaml"
    p.write_text(cfg_text)
    from core.config import load_config
    import pytest
    with pytest.raises(Exception):
        load_config(str(p))
