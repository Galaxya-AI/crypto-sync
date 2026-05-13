"""Cron-style scheduler embedded in a Python process.

Wraps APScheduler's AsyncIOScheduler so call sites do not deal with
the imperative add_job API directly. Cron triggers (instead of
while-true sleep loops) pin executions on calendar boundaries —
for example, every hour at minute 0 second 3, leaving a small buffer
after the bar closes before the gap filler starts.

Lifecycle is owned by the caller (the cron container's runner):
start at boot, shutdown on SIGTERM. Jobs registered with the same
name are deduplicated and capped at one concurrent execution.
"""
from __future__ import annotations

import os
import time
from collections.abc import Awaitable, Callable
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from src.logging_module.logging import get_logger

log = get_logger(__name__)


class CronScheduler:
    """Thin wrapper around :class:`AsyncIOScheduler`."""

    def __init__(self) -> None:
        """Create the underlying APScheduler. No jobs registered yet."""
        self._scheduler: AsyncIOScheduler = AsyncIOScheduler()

    def add_hourly(
        self,
        *,
        name: str,
        coro_func: Callable[[], Awaitable[object]],
        minute: int = 0,
        second: int = 3,
    ) -> None:
        """Register an async callable to run every hour at ``minute:second``.

        Parameters
        ----------
        name : str
            Logical job name (shown in logs and APScheduler's job store).
        coro_func : Callable[[], Awaitable[object]]
            Async function to call. Must accept no positional arguments;
            bind dependencies with ``functools.partial`` if needed.
        minute : int
            Minute mark to fire on (0-59). Default 0 → hh:00.
        second : int
            Second mark to fire on (0-59). Default 3 → hh:00:03 (small
            buffer after the hour rolls over so the candle is fully
            closed before the gap filler queries the DB).
        """
        trigger: CronTrigger = CronTrigger(minute=minute, second=second)
        self._scheduler.add_job(
            coro_func,
            trigger=trigger,
            id=name,
            name=name,
            replace_existing=True,
            misfire_grace_time=60,
            coalesce=True,
            max_instances=1,
        )
        log.info("scheduler_job_added", name=name, minute=minute, second=second)

    def add_heartbeat(self, *, name: str, path: str, every_seconds: int = 30) -> None:
        """Register a heartbeat job that touches ``path`` every ``every_seconds``.

        Used by the cron container to expose liveness to Docker without
        embedding an HTTP server. The HEALTHCHECK in docker-compose then
        checks the file mtime is recent.

        Parameters
        ----------
        name : str
            Logical job name.
        path : str
            File path to touch (will be created on first run).
        every_seconds : int
            Heartbeat period.
        """
        async def _touch() -> None:
            file_path: Path = Path(path)
            file_path.touch(exist_ok=True)
            now: float = time.time()
            os.utime(file_path, (now, now))

        self._scheduler.add_job(
            _touch,
            trigger=IntervalTrigger(seconds=every_seconds),
            id=name,
            name=name,
            replace_existing=True,
            max_instances=1,
            next_run_time=None,
        )
        log.info("scheduler_heartbeat_registered", path=path, every_seconds=every_seconds)

    def start(self) -> None:
        """Start the scheduler. Idempotent."""
        if self._scheduler.running:
            return
        self._scheduler.start()
        log.info("scheduler_started")

    async def shutdown(self) -> None:
        """Stop the scheduler and wait for in-flight jobs to finish.

        APScheduler's ``shutdown`` is synchronous; we expose an ``async``
        wrapper for symmetry with other ``shutdown`` methods used by the
        FastAPI lifespan.
        """
        if not self._scheduler.running:
            return
        self._scheduler.shutdown(wait=True)
        log.info("scheduler_stopped")


__all__ = ["CronScheduler"]
