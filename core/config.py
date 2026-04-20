"""Pydantic schema for collector configuration."""
from __future__ import annotations

from pathlib import Path
from typing import List, Literal, Optional

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
    region: str

    @model_validator(mode="after")
    def _key_pair(self):
        if bool(self.access_key) != bool(self.secret_key):
            raise ValueError("access_key and secret_key must be provided together")
        return self


class SourceConfig(BaseModel):
    type: Literal["s3"] = "s3"
    bucket: str
    fqdn: str
    org_id: str


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


class SinkConfig(BaseModel):
    type: SinkType
    host: Optional[str] = None
    port: Optional[int] = Field(default=None, ge=1, le=65535)
    url: Optional[str] = None
    tls: Optional[TlsConfig] = None
    timeout_sec: int = Field(default=10, ge=1)
    max_retries: int = Field(default=3, ge=0)
    retry_backoff_sec: List[float] = [1, 2, 4]
    batch_size: int = Field(default=100, ge=1)

    @model_validator(mode="after")
    def _type_requirements(self):
        if self.type in ("udp", "tcp", "tls") and (not self.host or not self.port):
            raise ValueError(f"sink.type={self.type} requires host and port")
        if self.type == "https" and not self.url:
            raise ValueError("sink.type=https requires url")
        return self


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
    p = Path(path)
    if not p.is_file():
        raise ConfigError(f"config file not found: {p}")
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        raise ConfigError(f"YAML parse error in {p}: {e}") from e
    try:
        return AppConfig(**(data or {}))
    except Exception as e:
        raise ConfigError(f"invalid config in {p}:\n{e}") from e
