"""Supervisor that manages the lifecycle of stream actors.

One supervisor per process. It keeps a dict of active actors keyed by
(symbol, interval) and exposes the three commands the API calls:
start, stop and list. Starting an actor registers it in the storage
registry; stopping cancels its task and marks the registry row
stopped.

The supervisor is core code: it talks to MarketDataPort and
StoragePort, never to concrete adapters. The same supervisor works
in tests against in-memory mocks of those ports.
"""

from __future__ import annotations

import asyncio
import contextlib
import time

from src.core.events import StreamInfo, StreamKey, StreamStatus
from src.core.ports import MarketDataPort, StoragePort
from src.core.stream_actor import StreamActor
from src.logging_module.logging import get_logger

log = get_logger(__name__)


class StreamAlreadyActiveError(RuntimeError):
    """Raised when :meth:`Supervisor.start` is called on an already-active key."""


class StreamNotFoundError(LookupError):
    """Raised when :meth:`Supervisor.stop` is called on an unknown key."""


class Supervisor:
    """Start, stop, and list stream actors."""

    def __init__(
        self,
        *,
        market: MarketDataPort,
        storage: StoragePort,
    ) -> None:
        """Wire supervisor dependencies.

        Parameters
        ----------
        market : MarketDataPort
            Shared market data port used by all spawned actors.
        storage : StoragePort
            Shared storage port used by all spawned actors.
        """
        self._market: MarketDataPort = market
        self._storage: StoragePort = storage
        self._tasks: dict[StreamKey, asyncio.Task[None]] = {}

    async def start(self, key: StreamKey) -> StreamInfo:
        """Start an actor for ``key`` and register it in storage.

        Parameters
        ----------
        key : StreamKey
            Stream to activate.

        Returns
        -------
        StreamInfo
            Registry info after activation.

        Raises
        ------
        StreamAlreadyActiveError
            If an actor is already running for ``key``.
        """
        if key in self._tasks and not self._tasks[key].done():
            raise StreamAlreadyActiveError(f"{key.symbol}/{key.interval} already active")

        started_at: int = int(time.time() * 1000)
        info: StreamInfo = await self._storage.register_stream(key, started_at)

        actor: StreamActor = StreamActor(
            key=key,
            market=self._market,
            storage=self._storage,
        )
        task: asyncio.Task[None] = asyncio.create_task(
            actor.run(),
            name=f"actor:{key.symbol}:{key.interval}",
        )
        self._tasks[key] = task
        log.info("stream_started", symbol=key.symbol, interval=key.interval)
        return info

    async def stop(self, key: StreamKey) -> StreamInfo:
        """Stop the actor for ``key`` and mark it stopped in storage.

        Parameters
        ----------
        key : StreamKey
            Stream to deactivate.

        Returns
        -------
        StreamInfo
            Registry info after deactivation.

        Raises
        ------
        StreamNotFoundError
            If no running task exists for ``key``.
        """
        task: asyncio.Task[None] | None = self._tasks.pop(key, None)
        if task is None:
            raise StreamNotFoundError(f"{key.symbol}/{key.interval} is not active")

        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        stopped_at: int = int(time.time() * 1000)
        await self._storage.mark_stream_stopped(key, stopped_at)
        info: StreamInfo | None = await self._storage.get_stream(key)
        assert info is not None, "registry row must exist after stop"
        log.info("stream_stopped", symbol=key.symbol, interval=key.interval)
        return info

    async def list(self, status: StreamStatus | None = None) -> list[StreamInfo]:
        """List streams from storage, optionally filtered by ``status``.

        Parameters
        ----------
        status : StreamStatus | None
            If set, only return streams with this status.

        Returns
        -------
        list[StreamInfo]
            Matching streams ordered by registration id.
        """
        streams: list[StreamInfo] = await self._storage.list_streams(status)
        return streams

    async def shutdown(self) -> None:
        """Cancel every running actor. Intended for app shutdown."""
        tasks: list[asyncio.Task[None]] = list(self._tasks.values())
        self._tasks.clear()
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        log.info("supervisor_shutdown", cancelled=len(tasks))

    async def restore_active_streams(self) -> int:
        """Restart actors for every stream marked ACTIVE in the registry.

        Called once during application startup so a redeploy or process
        restart resumes streams that were running before. Idempotent: streams
        already supervised by this process are skipped.

        Returns
        -------
        int
            Number of actors actually started by this call.
        """
        active: list[StreamInfo] = await self._storage.list_streams(StreamStatus.ACTIVE)
        restarted: int = 0
        for info in active:
            key: StreamKey = StreamKey(symbol=info.symbol, interval=info.interval)
            if key in self._tasks and not self._tasks[key].done():
                continue
            actor: StreamActor = StreamActor(
                key=key,
                market=self._market,
                storage=self._storage,
            )
            task: asyncio.Task[None] = asyncio.create_task(
                actor.run(),
                name=f"actor:{key.symbol}:{key.interval}",
            )
            self._tasks[key] = task
            restarted += 1
            log.info(
                "stream_restored",
                symbol=key.symbol,
                interval=key.interval,
            )
        log.info("supervisor_restore_done", restarted=restarted, candidates=len(active))
        return restarted


__all__ = [
    "StreamAlreadyActiveError",
    "StreamNotFoundError",
    "Supervisor",
]
