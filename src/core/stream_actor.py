"""Stream actor: one coroutine per active (symbol, interval).

Owns the full ingestion pipeline for a single stream: ensure the
candle table exists, bootstrap the missing history via REST, then
stream live candles from the WebSocket and persist each one. On any
error during the live phase, the actor sleeps a few seconds and
restarts from the bootstrap step so any candles missed during the
outage are filled before live ingestion resumes.

The actor holds no module-level state. Its lifetime equals the
lifetime of the asyncio task that runs its run() coroutine.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from typing import Final

from src.core.events import Candle, StreamKey
from src.core.ports import MarketDataPort, StoragePort
from src.logging_module.logging import get_logger

log = get_logger(__name__)

_BOOTSTRAP_BATCH_SIZE: Final[int] = 500
_RECONNECT_SLEEP_SECONDS: Final[float] = 5.0


class StreamActor:
    """Owns one (symbol, interval) ingestion pipeline end-to-end."""

    def __init__(
        self,
        *,
        key: StreamKey,
        market: MarketDataPort,
        storage: StoragePort,
    ) -> None:
        """Wire the actor to its dependencies.

        Parameters
        ----------
        key : StreamKey
            The (symbol, interval) this actor handles.
        market : MarketDataPort
            Source of candles (historical + live).
        storage : StoragePort
            Destination for candles and registry updates.
        """
        self._key: StreamKey = key
        self._market: MarketDataPort = market
        self._storage: StoragePort = storage

    async def run(self) -> None:
        """Run forever until the task is cancelled.

        The loop repeats bootstrap → live. On WebSocket failure, we log, wait
        :data:`_RECONNECT_SLEEP_SECONDS`, and restart; bootstrap will fetch
        the candles missed during the outage.
        """
        await self._storage.ensure_table(self._key)

        while True:
            try:
                await self._bootstrap_gap()
                await self._run_live_loop()
            except asyncio.CancelledError:
                log.info("actor_cancelled", symbol=self._key.symbol, interval=self._key.interval)
                raise
            except Exception as error:
                log.exception(
                    "actor_crashed_reconnecting",
                    symbol=self._key.symbol,
                    interval=self._key.interval,
                    error=str(error),
                )
                await asyncio.sleep(_RECONNECT_SLEEP_SECONDS)

    async def _bootstrap_gap(self) -> None:
        """Fetch via REST every candle missing between last stored and now.

        On cold start (empty table), ``start_ms=0`` means "since the symbol's
        first listing"; Binance transparently returns candles from the earliest
        available timestamp for that symbol.
        """
        last_ts: int | None = await self._storage.get_last_end_ts(self._key)
        now_ms: int = int(time.time() * 1000)

        if last_ts is None:
            start_ms: int = 0
            log.info(
                "bootstrap_cold",
                symbol=self._key.symbol,
                interval=self._key.interval,
                from_first_listing=True,
            )
        else:
            start_ms = last_ts + 1
            log.info(
                "bootstrap_gap",
                symbol=self._key.symbol,
                interval=self._key.interval,
                from_ms=start_ms,
            )

        batch: list[Candle] = []
        total: int = 0
        candles: AsyncIterator[Candle] = self._market.fetch_historical(
            key=self._key,
            start_ms=start_ms,
            end_ms=now_ms,
        )
        async for candle in candles:
            batch.append(candle)
            if len(batch) >= _BOOTSTRAP_BATCH_SIZE:
                total += await self._storage.write_candles(self._key, batch)
                batch.clear()
        if batch:
            total += await self._storage.write_candles(self._key, batch)

        log.info(
            "bootstrap_done",
            symbol=self._key.symbol,
            interval=self._key.interval,
            persisted=total,
        )

    async def _run_live_loop(self) -> None:
        """Subscribe to the live WebSocket and persist candles one by one.

        Returns when the underlying stream ends (should never happen normally).
        Raises whatever the stream raises on error so the outer ``run`` loop
        can decide to reconnect.
        """
        live: AsyncIterator[Candle] = self._market.stream_live(self._key)
        async for candle in live:
            await self._storage.write_candles(self._key, [candle])
            if candle.is_closed:
                log.debug(
                    "candle_closed",
                    symbol=self._key.symbol,
                    interval=self._key.interval,
                    close_time=candle.close_time,
                )


__all__ = ["StreamActor"]
