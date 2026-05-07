"""Per-pipeline tick orchestrator: list → gunzip lines → filter → map → send."""
from __future__ import annotations

import gzip
import json
import logging
import time
from dataclasses import dataclass
from typing import Callable, Optional

from core.checkpoint import CheckpointStore
from core.exceptions import CheckpointError
from mappers.base import Mapper
from sinks.base import Sink
from sources.base import Source


@dataclass
class BuildResult:
    """Output of build_pipelines_from_config — either polling or sqs mode."""
    mode: str  # "polling" | "sqs"
    polling: Optional[list] = None        # list[tuple[Pipeline, int]]
    sqs_dispatcher: Optional[object] = None   # SqsS3Dispatcher
    sqs_pipelines: Optional[list] = None  # list[SqsPipeline]


class Pipeline:
    def __init__(
        self,
        name: str,
        log_type: str,
        source: Source,
        mapper: Mapper,
        sink: Sink,
        checkpoint_store: CheckpointStore,
        filter_fn: Optional[Callable[[dict], bool]] = None,
        max_files_per_tick: int = 1000,
        recovery_lookback_hours: int = 24,
    ):
        self.name = name
        self.log_type = log_type
        self.source = source
        self.mapper = mapper
        self.sink = sink
        self.checkpoint_store = checkpoint_store
        self.filter_fn = filter_fn
        self.max_files_per_tick = max_files_per_tick
        self.recovery_lookback_hours = recovery_lookback_hours
        self.log = logging.getLogger(name)

    def tick(self) -> None:
        t0 = time.monotonic()
        try:
            cp = self.checkpoint_store.load(self.name)
        except CheckpointError as e:
            cp_path = self.checkpoint_store._path(self.name)
            self.log.error(
                "checkpoint corrupted at %s (%s); resetting to fresh "
                "checkpoint with lookback=%dh",
                cp_path, e, self.recovery_lookback_hours,
            )
            cp = self.checkpoint_store.fresh(
                self.name, initial_lookback_hours=self.recovery_lookback_hours)
            self.checkpoint_store.save(cp)

        stats = dict(files=0, read=0, filtered=0,
                     sent=0, failed=0, mapper_err=0)
        try:
            for key, lm, body in self.source.iter_new_files(
                    self.log_type, cp, max_files_per_tick=self.max_files_per_tick):
                stats["files"] += 1
                all_ok, sent_in_file = self._process_file(key, body, stats)
                if not all_ok:
                    self.log.error("sink failed on %s; checkpoint not advancing", key)
                    break
                cp = cp.advance(last_modified=lm, last_key=key, events_inc=sent_in_file)
                self.checkpoint_store.save(cp)
        except Exception as e:  # noqa: BLE001
            self.log.exception("tick aborted: %s", e)
        finally:
            self.log.info(
                "tick: files=%d read=%d sent=%d filtered=%d failed=%d mapper_err=%d "
                "checkpoint=%s duration=%.2fs",
                stats["files"], stats["read"], stats["sent"],
                stats["filtered"], stats["failed"], stats["mapper_err"],
                (cp.last_key or "none")[-40:],
                time.monotonic() - t0,
            )

    def _process_file(self, key: str, body: bytes, stats: dict) -> tuple[bool, int]:
        sent_in_file = 0
        try:
            body = gzip.decompress(body)
        except Exception:
            pass  # not gzipped; use as-is
        for raw_line in body.splitlines():
            if not raw_line.strip():
                continue
            stats["read"] += 1
            try:
                ev = json.loads(raw_line)
            except Exception:
                stats["mapper_err"] += 1
                self.log.warning("bad JSON line in %s; skipping", key)
                continue

            if self.filter_fn and not self.filter_fn(ev):
                stats["filtered"] += 1
                continue

            try:
                wire = self.mapper.format(ev)
            except Exception as e:  # noqa: BLE001
                stats["mapper_err"] += 1
                self.log.error("mapper error on %s: %s", key, e)
                continue

            if self.sink.send(wire):
                stats["sent"] += 1
                sent_in_file += 1
            else:
                stats["failed"] += 1
                return False, sent_in_file
        if sent_in_file and not self.sink.flush():
            stats["sent"] -= sent_in_file
            stats["failed"] += 1
            self.log.error("sink flush failed on %s", key)
            return False, sent_in_file
        return True, sent_in_file


# ---- factory ----------------------------------------------------------------

def _build_sink(sc) -> Sink:
    from sinks.file_sink import FileSink
    from sinks.https_sink import HttpsSink
    from sinks.multi_sink import MultiSink
    from sinks.tcp_sink import TcpSink
    from sinks.tls_sink import TlsSink
    from sinks.udp_sink import UdpSink

    if sc.type == "udp":
        return UdpSink(host=sc.host, port=sc.port, max_bytes=sc.max_bytes)
    if sc.type == "tcp":
        return TcpSink(host=sc.host, port=sc.port,
                       timeout_sec=sc.timeout_sec,
                       max_retries=sc.max_retries,
                       retry_backoff_sec=sc.retry_backoff_sec)
    if sc.type == "tls":
        tls = sc.tls
        return TlsSink(host=sc.host, port=sc.port,
                       verify=tls.verify if tls else True,
                       ca_file=tls.ca_file if tls else None,
                       timeout_sec=sc.timeout_sec,
                       max_retries=sc.max_retries,
                       retry_backoff_sec=sc.retry_backoff_sec)
    if sc.type == "https":
        return HttpsSink(url=sc.url,
                         batch_size=sc.batch_size,
                         verify_tls=sc.tls.verify if sc.tls else True,
                         timeout_sec=sc.timeout_sec,
                         max_retries=sc.max_retries,
                         retry_backoff_sec=sc.retry_backoff_sec)
    if sc.type == "file":
        return FileSink(
            path=sc.path,
            rotation_mb=sc.rotation_mb,
            rotation_hours=sc.rotation_hours,
            retention_days=sc.retention_days,
            prefix=sc.prefix,
        )
    if sc.type == "multi":
        return MultiSink([_build_sink(sub) for sub in sc.sinks])
    raise ValueError(f"unknown sink type: {sc.type}")


def _build_mapper(mapper_cfg, log_type: str) -> Mapper:
    from pathlib import Path

    from mappers.cef import CefMapper
    from mappers.passthrough import PassthroughMapper
    from mappers.syslog_json import SyslogJsonMapper

    if mapper_cfg.format == "syslog_json":
        return SyslogJsonMapper(
            log_type=log_type,
            flatten_enabled=mapper_cfg.flatten,
            flatten_separator=mapper_cfg.flatten_separator,
            flatten_max_depth=mapper_cfg.flatten_max_depth,
            array_strategy=mapper_cfg.array_strategy,
        )
    if mapper_cfg.format == "cef":
        return CefMapper(log_type=log_type,
                         mapping_path=Path(mapper_cfg.mapping_file))
    if mapper_cfg.format == "json":
        return PassthroughMapper(
            flatten_enabled=mapper_cfg.flatten,
            flatten_separator=mapper_cfg.flatten_separator,
            flatten_max_depth=mapper_cfg.flatten_max_depth,
            array_strategy=mapper_cfg.array_strategy,
        )
    raise ValueError(f"unknown mapper format: {mapper_cfg.format}")


def _build_filter(filter_cfg) -> Optional[Callable[[dict], bool]]:
    from core.expression_filter import compile_expression
    if filter_cfg is None:
        return None
    return compile_expression(filter_cfg.expression)


def _build_session(cfg):
    import boto3
    if cfg.aws.profile:
        return boto3.Session(profile_name=cfg.aws.profile, region_name=cfg.aws.region)
    if cfg.aws.access_key:
        return boto3.Session(
            aws_access_key_id=cfg.aws.access_key,
            aws_secret_access_key=cfg.aws.secret_key,
            region_name=cfg.aws.region,
        )
    return boto3.Session(region_name=cfg.aws.region)


def _build_polling(cfg, session) -> "BuildResult":
    from sources.s3_source import S3Source

    s3_client = session.client("s3")
    source = S3Source(
        bucket=cfg.source.bucket,
        fqdn=cfg.source.fqdn,
        org_id=cfg.source.org_id,
        s3_client=s3_client,
    )

    store = CheckpointStore(cfg.checkpoint.dir)
    lookback = cfg.checkpoint.initial_lookback_hours

    result: list[tuple[Pipeline, int]] = []
    for pc in cfg.pipelines:
        if not pc.enabled:
            continue
        if store.load(pc.name).last_modified is None:
            store.save(store.fresh(pc.name, lookback))

        mapper = _build_mapper(pc.mapper, pc.log_type)
        filter_fn = _build_filter(pc.filter)

        pipeline = Pipeline(
            name=pc.name,
            log_type=pc.log_type,
            source=source,
            mapper=mapper,
            sink=_build_sink(pc.sink),
            checkpoint_store=store,
            filter_fn=filter_fn,
            max_files_per_tick=pc.max_files_per_tick,
            recovery_lookback_hours=lookback,
        )
        result.append((pipeline, pc.poll_interval_sec))
    return BuildResult(mode="polling", polling=result)


def _build_sqs(cfg, session) -> "BuildResult":
    from core.sqs_pipeline import SqsPipeline
    from sources.sqs_s3_source import SqsS3Dispatcher

    sqs_client = session.client("sqs")
    s3_client = session.client("s3")
    sqs_pipelines: list = []
    for pc in cfg.pipelines:
        if not pc.enabled:
            continue
        mapper = _build_mapper(pc.mapper, pc.log_type)
        filter_fn = _build_filter(pc.filter)
        sink = _build_sink(pc.sink)
        sqs_pipelines.append(SqsPipeline(
            name=pc.name, log_type=pc.log_type,
            mapper=mapper, sink=sink, filter_fn=filter_fn,
        ))
    dispatcher = SqsS3Dispatcher(
        sqs_client=sqs_client, s3_client=s3_client,
        queue_url=cfg.source.queue_url, bucket=cfg.source.bucket,
        fqdn=cfg.source.fqdn, org_id=cfg.source.org_id,
        pipelines=sqs_pipelines,
        visibility_timeout_sec=cfg.source.visibility_timeout_sec,
        visibility_extension_sec=cfg.source.visibility_extension_sec,
        wait_time_sec=cfg.source.wait_time_sec,
        max_messages_per_receive=cfg.source.max_messages_per_receive,
        max_workers=cfg.source.max_workers,
    )
    return BuildResult(mode="sqs", sqs_dispatcher=dispatcher,
                       sqs_pipelines=sqs_pipelines)


def build_pipelines_from_config(cfg) -> "BuildResult":
    """Convert an AppConfig into a BuildResult (polling or sqs mode).

    Returns only enabled pipelines. Raises on configuration errors so the
    program fails fast.
    """
    session = _build_session(cfg)
    if cfg.source.type == "s3":
        return _build_polling(cfg, session)
    if cfg.source.type == "sqs_s3":
        return _build_sqs(cfg, session)
    raise ValueError(f"unknown source type: {cfg.source.type}")
