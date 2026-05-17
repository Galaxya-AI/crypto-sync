"""Unit tests for Supervisor.

Verify that start/stop/list interact with the storage port correctly and that
duplicate starts and unknown stops raise the right errors. The actor runs as
a background task during these tests; we cancel it via stop() before each
test ends so no warnings leak into other tests.
"""

from __future__ import annotations

import asyncio

import pytest

from src.core.events import StreamKey, StreamStatus
from src.core.stream_supervisor import (
    StreamAlreadyActiveError,
    StreamNotFoundError,
    Supervisor,
)
from _helpers import FakeMarket, FakeStorage


@pytest.mark.asyncio
async def test_start_creates_active_registry_entry(
    fake_market: FakeMarket,
    fake_storage: FakeStorage,
    btc_1m_key: StreamKey,
) -> None:
    """Calling start() registers the stream as active in storage."""
    supervisor: Supervisor = Supervisor(market=fake_market, storage=fake_storage)
    info = await supervisor.start(btc_1m_key)
    assert info.status is StreamStatus.ACTIVE
    assert info.symbol == "BTCUSDT"
    assert info.interval == "1m"
    assert btc_1m_key in fake_storage.registry
    await supervisor.shutdown()


@pytest.mark.asyncio
async def test_start_twice_raises_already_active(
    fake_market: FakeMarket,
    fake_storage: FakeStorage,
    btc_1m_key: StreamKey,
) -> None:
    """A second start() on the same key raises StreamAlreadyActiveError."""
    supervisor: Supervisor = Supervisor(market=fake_market, storage=fake_storage)
    await supervisor.start(btc_1m_key)
    with pytest.raises(StreamAlreadyActiveError):
        await supervisor.start(btc_1m_key)
    await supervisor.shutdown()


@pytest.mark.asyncio
async def test_stop_unknown_raises_not_found(
    fake_market: FakeMarket,
    fake_storage: FakeStorage,
    btc_1m_key: StreamKey,
) -> None:
    """Stopping a stream that was never started raises StreamNotFoundError."""
    supervisor: Supervisor = Supervisor(market=fake_market, storage=fake_storage)
    with pytest.raises(StreamNotFoundError):
        await supervisor.stop(btc_1m_key)


@pytest.mark.asyncio
async def test_stop_marks_registry_stopped(
    fake_market: FakeMarket,
    fake_storage: FakeStorage,
    btc_1m_key: StreamKey,
) -> None:
    """stop() flips the registry status to STOPPED."""
    supervisor: Supervisor = Supervisor(market=fake_market, storage=fake_storage)
    await supervisor.start(btc_1m_key)
    await asyncio.sleep(0)  # let the actor task start
    info = await supervisor.stop(btc_1m_key)
    assert info.status is StreamStatus.STOPPED
    assert info.stopped_at is not None
    assert btc_1m_key not in supervisor._tasks


@pytest.mark.asyncio
async def test_list_filters_by_status(
    fake_market: FakeMarket,
    fake_storage: FakeStorage,
) -> None:
    """list(ACTIVE) excludes streams already stopped."""
    supervisor: Supervisor = Supervisor(market=fake_market, storage=fake_storage)
    btc: StreamKey = StreamKey(symbol="BTCUSDT", interval="1m")
    eth: StreamKey = StreamKey(symbol="ETHUSDT", interval="1h")
    await supervisor.start(btc)
    await supervisor.start(eth)
    await supervisor.stop(eth)
    active = await supervisor.list(StreamStatus.ACTIVE)
    assert {info.symbol for info in active} == {"BTCUSDT"}
    await supervisor.shutdown()
