"""Gap filler that re-fetches candles missed between consecutive rows.

This is the unit of work executed periodically by the cron container.
It does not loop or sleep on its own; the scheduler in src/cron/
runner.py decides the cadence. For each active stream, the filler
asks the SQL gap detector for missing ranges, fetches them from the
historical Binance adapter, and persists them through the same
storage port used by live ingestion.

Acts as a belt-and-suspenders layer on top of the actor: the actor
self-heals on disconnect, but a silent zombie WebSocket would leave
durable holes that this job closes deterministically.
"""
from __future__ import annotations

from typing import Final

from src.core.events import INTERVAL_MS, StreamInfo, StreamKey, StreamStatus
from src.core.ports import MarketDataPort, StoragePort
from src.logging_module.logging import get_logger
from src.mariadb.gap_detector import Gap, GapDetector

log = get_logger(__name__)

# 500 keeps each executemany payload under MariaDB's default max_allowed_packet
# (64 MiB at 12 DECIMAL columns ≈ ample headroom) while still amortizing the
# round-trip cost. Smaller batches throttle the gap filler; larger ones risk
# packet errors on hosts that ship a smaller default.
_BATCH_SIZE: Final[int] = 500


class GapFiller:
    """One-pass gap-filler. The scheduler decides when to run :meth:`fill_now`."""

    def __init__(
        self,
        *,
        market: MarketDataPort,
        storage: StoragePort,
        gap_detector: GapDetector,
    ) -> None:
        """Wire the gap filler to its collaborators.

        Parameters
        ----------
        market : MarketDataPort
            Source of historical candles (used to refetch missing ranges).
        storage : StoragePort
            Destination for refetched candles. Must be the same instance
            used by live ingestion so upserts deduplicate properly.
        gap_detector : GapDetector
            SQL-backed detector that scans tables for gaps.
        """
        self._market: MarketDataPort = market
        self._storage: StoragePort = storage
        self._detector: GapDetector = gap_detector

    async def fill_now(self) -> int:
        """Run one pass over every active stream.

        Returns
        -------
        int
            Total number of bars persisted across all streams during this pass.
        """
        active: list[StreamInfo] = await self._storage.list_streams(StreamStatus.ACTIVE)
        log.info("gap_filler_pass_started", n_active=len(active))
        total_persisted: int = 0
        for info in active:
            key: StreamKey = StreamKey(symbol=info.symbol, interval=info.interval)
            total_persisted += await self._fill_one(key)
        log.info("gap_filler_pass_done", persisted=total_persisted)
        return total_persisted

    async def _fill_one(self, key: StreamKey) -> int:
        """Fetch + persist every gap for a single stream key.

        Returns
        -------
        int
            Number of bars persisted for ``key``.
        """
        expected_ms: int | None = INTERVAL_MS.get(key.interval)
        if expected_ms is None:
            log.warning("unknown_interval", symbol=key.symbol, interval=key.interval)
            return 0

        gaps: list[Gap] = await self._detector.find_gaps(key, expected_ms)
        if not gaps:
            return 0

        persisted: int = 0
        for gap in gaps:
            # +1 / -1 to exclude the two known-good boundary candles from the
            # refetch window. Without this we would re-pull rows already in
            # the table and hit the upsert path for nothing — wasted bandwidth
            # and a misleading "candles_written" metric spike.
            start_ms: int = gap.last_known_close_ms + 1
            end_ms: int = gap.next_close_ms - 1
            log.info(
                "filling_gap",
                symbol=key.symbol,
                interval=key.interval,
                start_ms=start_ms,
                end_ms=end_ms,
                missing_bars=gap.missing_bars,
            )
            persisted += await self._fetch_and_write(key, start_ms, end_ms)
        return persisted

    async def _fetch_and_write(self, key: StreamKey, start_ms: int, end_ms: int) -> int:
        """Stream candles in ``[start_ms, end_ms]`` from market into storage."""
        batch: list = []
        total: int = 0
        async for candle in self._market.fetch_historical(
            key=key,
            start_ms=start_ms,
            end_ms=end_ms,
        ):
            batch.append(candle)
            if len(batch) >= _BATCH_SIZE:
                total += await self._storage.write_candles(key, batch)
                batch.clear()
        if batch:
            total += await self._storage.write_candles(key, batch)
        return total


__all__ = ["GapFiller"]
