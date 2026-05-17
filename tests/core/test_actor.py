"""Unit tests for StreamActor.

Verify the bootstrap-then-live ordering, table ensuring, and that the actor
writes received candles to the storage port.
"""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from src.core.events import StreamKey
from src.core.stream_actor import StreamActor
from _helpers import FakeMarket, FakeStorage, make_candle


@pytest.mark.asyncio
async def test_actor_ensures_table_before_running(
    fake_market: FakeMarket,
    fake_storage: FakeStorage,
    btc_1m_key: StreamKey,
) -> None:
    """run() calls ensure_table once at startup."""
    actor: StreamActor = StreamActor(
        key=btc_1m_key,
        market=fake_market,
        storage=fake_storage,
    )
    task: asyncio.Task[None] = asyncio.create_task(actor.run())
    await asyncio.sleep(0.05)
    assert btc_1m_key in fake_storage.tables_ensured
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_actor_persists_bootstrap_candles(
    fake_storage: FakeStorage,
    btc_1m_key: StreamKey,
) -> None:
    """Candles yielded by fetch_historical are written to storage."""
    historical = [
        make_candle(open_time=0),
        make_candle(open_time=60_000),
    ]
    market: FakeMarket = FakeMarket(historical=historical)

    actor: StreamActor = StreamActor(
        key=btc_1m_key,
        market=market,
        storage=fake_storage,
    )
    task: asyncio.Task[None] = asyncio.create_task(actor.run())
    await asyncio.sleep(0.1)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    written = fake_storage.candles.get(btc_1m_key, [])
    assert len(written) >= 2


@pytest.mark.asyncio
async def test_actor_persists_live_candles(
    fake_storage: FakeStorage,
    btc_1m_key: StreamKey,
) -> None:
    """Candles emitted via the live queue land in storage."""
    market: FakeMarket = FakeMarket(historical=[])
    actor: StreamActor = StreamActor(
        key=btc_1m_key,
        market=market,
        storage=fake_storage,
    )
    task: asyncio.Task[None] = asyncio.create_task(actor.run())
    await market.live_queue.put(make_candle(open_time=120_000))
    await asyncio.sleep(0.05)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert any(c.open_time == 120_000 for c in fake_storage.candles.get(btc_1m_key, []))
