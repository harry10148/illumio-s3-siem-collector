"""APScheduler wrapper. Each pipeline runs as an IntervalTrigger job, with
coalesce and max_instances=1 so slow ticks don't stack up."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Sequence

from apscheduler.executors.pool import ThreadPoolExecutor as APSThreadPool
from apscheduler.schedulers.base import SchedulerNotRunningError
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from core.pipeline import Pipeline

log = logging.getLogger(__name__)


class PipelineScheduler:
    def __init__(self, pipelines: Sequence[tuple[Pipeline, int]]):
        self.pipelines = [p for p, _ in pipelines]
        max_workers = max(1, len(pipelines))
        self.scheduler = BlockingScheduler(
            executors={"default": APSThreadPool(max_workers=max_workers)},
            job_defaults={"coalesce": True, "max_instances": 1,
                          "misfire_grace_time": 30},
        )
        for pipeline, interval in pipelines:
            self.scheduler.add_job(
                pipeline.tick,
                trigger=IntervalTrigger(seconds=interval),
                id=pipeline.name,
                name=pipeline.name,
                next_run_time=datetime.now(timezone.utc),
            )
            log.info("scheduled pipeline %s every %ds", pipeline.name, interval)

    def run_forever(self) -> None:
        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            log.info("shutdown requested, stopping scheduler")
        finally:
            try:
                self.scheduler.shutdown(wait=True)
            except SchedulerNotRunningError:
                pass
            for pipeline in self.pipelines:
                try:
                    pipeline.sink.close()
                except Exception as e:  # noqa: BLE001
                    log.warning("failed to close sink for %s: %s", pipeline.name, e)
