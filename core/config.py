"""Pydantic schema for collector configuration."""
from __future__ import annotations

from pathlib import Path
from typing import Annotated, List, Literal, Optional, Union

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

from core.exceptions import ConfigError

LogType = Literal["auditable", "pd0", "pd1", "pd2", "pd3"]
MapperFormat = Literal["syslog_json", "cef", "json"]
SinkType = Literal["udp", "tcp", "tls", "https"]
ArrayStrategy = Literal["stringify", "first", "skip"]


class TlsConfig(BaseModel):
    verify: bool = True
    ca_file: Optional[str] = None


class AwsConfig(BaseModel):
    profile: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    region: Optional[str] = None

    @model_validator(mode="after")
    def _key_pair(self):
        if bool(self.access_key) != bool(self.secret_key):
            raise ValueError("access_key and secret_key must be provided together")
        return self


class S3SourceConfig(BaseModel):
    type: Literal["s3"] = "s3"
    bucket: str
    fqdn: str
    org_id: str


class SqsS3SourceConfig(BaseModel):
    type: Literal["sqs_s3"] = "sqs_s3"
    queue_url: str
    bucket: str
    fqdn: str
    org_id: str
    visibility_timeout_sec: int = 60
    visibility_extension_sec: int = 60
    wait_time_sec: int = 20
    max_messages_per_receive: int = 10
    max_workers: int = 1


# Discriminated union; Pydantic routes on the literal ``type`` field.
SourceConfig = Annotated[
    Union[S3SourceConfig, SqsS3SourceConfig],
    Field(discriminator="type"),
]


class CheckpointConfig(BaseModel):
    dir: str = "./state"
    initial_lookback_hours: int = Field(default=0, ge=0)
    atomic_write: bool = True


class LoggingConfig(BaseModel):
    level: Literal["DEBUG", "INFO", "WARN", "ERROR"] = "INFO"
    dir: str = "./logs"
    file: str = "collector.log"
    rotate_mb: int = Field(default=50, ge=1)
    keep_files: int = Field(default=7, ge=1)
    console: bool = True


class MapperConfig(BaseModel):
    format: MapperFormat = "syslog_json"
    flatten: bool = True
    flatten_separator: str = "_"
    flatten_max_depth: int = Field(default=10, ge=1)
    array_strategy: ArrayStrategy = "stringify"
    mapping_file: Optional[str] = None

    @model_validator(mode="after")
    def _cef_needs_mapping(self):
        if self.format == "cef" and not self.mapping_file:
            raise ValueError("mapper.format=cef requires mapping_file")
        return self


class FilterConfig(BaseModel):
    expression: str


# ── Sink configurations ───────────────────────────────────────────────────────

class NetworkSinkConfig(BaseModel):
    """UDP / TCP / TLS / HTTPS — forward events to a remote SIEM."""

    type: SinkType
    host: Optional[str] = None
    port: Optional[int] = Field(default=None, ge=1, le=65535)
    url: Optional[str] = None
    tls: Optional[TlsConfig] = None
    timeout_sec: int = Field(default=10, ge=1)
    max_retries: int = Field(default=3, ge=0)
    retry_backoff_sec: List[float] = [1, 2, 4]
    batch_size: int = Field(default=100, ge=1)
    max_bytes: int = Field(default=8192, ge=0)

    @model_validator(mode="after")
    def _type_requirements(self):
        if self.type in ("udp", "tcp", "tls") and (not self.host or not self.port):
            raise ValueError(f"sink.type={self.type} requires host and port")
        if self.type == "https" and not self.url:
            raise ValueError("sink.type=https requires url")
        return self


class FileSinkConfig(BaseModel):
    """Write events to a local rolling log file for long-term retention."""

    type: Literal["file"]
    path: str
    rotation_mb: int = Field(
        default=200, ge=1,
        description="Rotate when the active file exceeds this many MB.",
    )
    rotation_hours: int = Field(
        default=24, ge=1,
        description="Rotate when the active file has been open this many hours.",
    )
    retention_days: int = Field(
        default=30, ge=1,
        description="Delete rotated .log.gz files older than this many days.",
    )
    prefix: str = Field(
        default="ILLUMIO_FLOW: ",
        description="Prepend this string to every line (FortiSIEM Agent Log Prefix).",
    )


class MultiSinkConfig(BaseModel):
    """Fan-out: deliver each event to multiple sinks simultaneously.

    Returns False (stops checkpoint) only when **all** child sinks fail.
    """

    type: Literal["multi"]
    sinks: List[SinkConfig]  # forward ref — resolved by model_rebuild() below


# Discriminated union; Pydantic routes on the literal ``type`` field.
# ``SinkConfig`` is a type alias (not a class) — use NetworkSinkConfig /
# FileSinkConfig / MultiSinkConfig directly when constructing in tests.
SinkConfig = Annotated[
    Union[NetworkSinkConfig, FileSinkConfig, MultiSinkConfig],
    Field(discriminator="type"),
]

# Resolve the forward reference in MultiSinkConfig.sinks.
MultiSinkConfig.model_rebuild()


class PipelineConfig(BaseModel):
    name: str
    enabled: bool = True
    log_type: LogType
    poll_interval_sec: int = Field(ge=10)
    max_files_per_tick: int = Field(default=1000, ge=1)
    filter: Optional[FilterConfig] = None
    mapper: MapperConfig
    sink: SinkConfig


class AppConfig(BaseModel):
    aws: AwsConfig
    source: SourceConfig
    checkpoint: CheckpointConfig = CheckpointConfig()
    logging: LoggingConfig = LoggingConfig()
    pipelines: List[PipelineConfig]

    @field_validator("pipelines")
    @classmethod
    def _unique_names(cls, v):
        names = [p.name for p in v]
        if len(names) != len(set(names)):
            raise ValueError("pipeline names must be unique")
        if not v:
            raise ValueError("at least one pipeline must be defined")
        return v


def load_config(path: str | Path) -> AppConfig:
    p = Path(path).resolve()
    if not p.is_file():
        raise ConfigError(f"config file not found: {p}")
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        raise ConfigError(f"YAML parse error in {p}: {e}") from e
    try:
        cfg = AppConfig(**(data or {}))
    except Exception as e:
        raise ConfigError(f"invalid config in {p}:\n{e}") from e

    # Resolve relative dirs against the config file's directory so the service
    # doesn't try to write into the (read-only) install directory.
    base = p.parent
    if not Path(cfg.logging.dir).is_absolute():
        cfg.logging.dir = str((base / cfg.logging.dir).resolve())
    if not Path(cfg.checkpoint.dir).is_absolute():
        cfg.checkpoint.dir = str((base / cfg.checkpoint.dir).resolve())

    return cfg
