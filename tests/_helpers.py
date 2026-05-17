"""In-memory fakes shared across tests.

Provides `FakeStorage` (implements `StoragePort`) and `FakeMarket` (implements
`MarketDataPort`), plus a `make_candle` factory. Kept out of conftest.py so
test modules can import these classes directly without pytest tooling tricks.
Conftest.py re-exposes them as fixtures (`fake_storage`, `fake_market`).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterable
from decimal import Decimal

from src.core.events import (
    Candle,
    StreamInfo,
    StreamKey,
    StreamStatus,
)
from src.core.ports import MarketDataPort, StoragePort


def make_candle(
    *,
    symbol: str = "BTCUSDT",
    interval: str = "1m",
    open_time: int,
    is_closed: bool = True,
) -> Candle:
    """Build a deterministic candle for tests."""
    return Candle(
        symbol=symbol,
        interval=interval,
        open_time=open_time,
        close_time=open_time + 60_000 - 1,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=Decimal("10"),
        quote_volume=Decimal("1005"),
        num_trades=5,
        taker_buy_base_volume=Decimal("4"),
        taker_buy_quote_volume=Decimal("402"),
        is_closed=is_closed,
    )


class FakeStorage(StoragePort):
    """In-memory storage used to test core code without MariaDB."""

    def __init__(self) -> None:
        self.candles: dict[StreamKey, list[Candle]] = {}
        self.registry: dict[StreamKey, StreamInfo] = {}
        self.tables_ensured: set[StreamKey] = set()

    async def ensure_table(self, key: StreamKey) -> str:
        self.tables_ensured.add(key)
        return key.table_name()

    async def write_candles(self, key: StreamKey, candles: Iterable[Candle]) -> int:
        bucket: list[Candle] = self.candles.setdefault(key, [])
        added: int = 0
        for candle in candles:
            bucket.append(candle)
            added += 1
        return added

    async def register_stream(self, key: StreamKey, started_at: int) -> StreamInfo:
        info: StreamInfo = StreamInfo(
            symbol=key.symbol,
            interval=key.interval,
            status=StreamStatus.ACTIVE,
            table_name=key.table_name(),
            started_at=started_at,
            stopped_at=None,
            last_end_ts=None,
        )
        self.registry[key] = info
        return info

    async def mark_stream_stopped(self, key: StreamKey, stopped_at: int) -> None:
        existing: StreamInfo | None = self.registry.get(key)
        if existing is None:
            return
        self.registry[key] = StreamInfo(
            symbol=existing.symbol,
            interval=existing.interval,
            status=StreamStatus.STOPPED,
            table_name=existing.table_name,
            started_at=existing.started_at,
            stopped_at=stopped_at,
            last_end_ts=existing.last_end_ts,
        )

    async def get_last_end_ts(self, key: StreamKey) -> int | None:
        bucket: list[Candle] = self.candles.get(key, [])
        if not bucket:
            return None
        return max(c.close_time for c in bucket)

    async def list_streams(self, status: StreamStatus | None = None) -> list[StreamInfo]:
        rows: list[StreamInfo] = list(self.registry.values())
        if status is not None:
            rows = [r for r in rows if r.status is status]
        return rows

    async def get_stream(self, key: StreamKey) -> StreamInfo | None:
        return self.registry.get(key)

    async def read_candles(
        self,
        key: StreamKey,
        *,
        start_ms: int | None = None,
        end_ms: int | None = None,
        limit: int = 500,
    ) -> list[Candle]:
        bucket: list[Candle] = self.candles.get(key, [])
        filtered: list[Candle] = []
        for candle in sorted(bucket, key=lambda c: c.close_time):
            if start_ms is not None and candle.close_time < start_ms:
                continue
            if end_ms is not None and candle.close_time > end_ms:
                continue
            filtered.append(candle)
            if len(filtered) >= limit:
                break
        return filtered


class FakeMarket(MarketDataPort):
    """In-memory market data port. Yields preset candles."""

    def __init__(self, historical: list[Candle] | None = None) -> None:
        self.historical: list[Candle] = historical or []
        self.live_queue: asyncio.Queue[Candle] = asyncio.Queue()
        self.fetch_calls: list[tuple[StreamKey, int, int | None]] = []

    async def fetch_historical(
        self,
        key: StreamKey,
        start_ms: int,
        end_ms: int | None = None,
    ) -> AsyncIterator[Candle]:
        self.fetch_calls.append((key, start_ms, end_ms))
        for candle in self.historical:
            if candle.close_time < start_ms:
                continue
            if end_ms is not None and candle.close_time > end_ms:
                continue
            yield candle

    async def stream_live(self, key: StreamKey) -> AsyncIterator[Candle]:
        while True:
            candle: Candle = await self.live_queue.get()
            yield candle
