"""Illumio S3 → SIEM Collector entry point."""
from __future__ import annotations

import argparse
import logging
import sys

from core.config import load_config
from core.exceptions import ConfigError
from core.logging_setup import setup_logging
from core.pipeline import build_pipelines_from_config
from core.scheduler import PipelineScheduler

log = logging.getLogger("collector")


def banner(cfg) -> None:
    enabled = [p for p in cfg.pipelines if p.enabled]
    print("=" * 72)
    print("Illumio S3 -> SIEM Collector v1.0")
    print(f"  config:    {cfg.source.bucket} / {cfg.source.fqdn} / org_id={cfg.source.org_id}")
    print(f"  pipelines: {len(cfg.pipelines)} defined, {len(enabled)} enabled")
    for p in enabled:
        dest = _sink_desc(p.sink)
        print(f"    - {p.name:30s} log_type={p.log_type:9s} "
              f"every={p.poll_interval_sec}s -> {dest}")
    print(f"  state:     {cfg.checkpoint.dir}")
    print(f"  log:       {cfg.logging.dir}/{cfg.logging.file} "
          f"(level={cfg.logging.level} rotate={cfg.logging.rotate_mb}MB "
          f"keep={cfg.logging.keep_files})")
    print("=" * 72, flush=True)


def _sink_desc(sc) -> str:
    if sc.type == "https":
        return f"https {sc.url}"
    if sc.type == "file":
        return f"file {sc.path}"
    if sc.type == "multi":
        return "multi[" + ", ".join(_sink_desc(s) for s in sc.sinks) + "]"
    return f"{sc.type} {sc.host}:{sc.port}"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Illumio S3 -> SIEM Collector")
    parser.add_argument("--config", required=True, help="path to config.yaml")
    parser.add_argument("--dry-run", action="store_true",
                        help="validate config and exit")
    parser.add_argument("--once", metavar="PIPELINE_NAME",
                        help="run a single pipeline once and exit")
    args = parser.parse_args(argv)

    try:
        cfg = load_config(args.config)
    except ConfigError as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 2

    setup_logging(cfg.logging)
    banner(cfg)

    if args.dry_run:
        print("[dry-run] config OK, exiting.")
        return 0

    result = build_pipelines_from_config(cfg)
    if result.mode != "polling":
        raise NotImplementedError(
            f"source mode {result.mode} not yet wired up in collector")
    pipelines = result.polling
    if not pipelines:
        print("[ERROR] no enabled pipelines", file=sys.stderr)
        return 3

    if args.once:
        match = [p for p, _interval in pipelines if p.name == args.once]
        if not match:
            print(f"[ERROR] pipeline '{args.once}' not enabled", file=sys.stderr)
            return 4
        log.info("running pipeline %s once", args.once)
        match[0].tick()
        match[0].sink.close()
        return 0

    scheduler = PipelineScheduler(pipelines)
    scheduler.run_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
