"""Shared pytest fixtures.

The fake implementations of MarketDataPort and StoragePort live in
``tests/_helpers.py`` so test modules can import the classes directly
(`from _helpers import FakeMarket, FakeStorage`). The fixtures defined
here just wrap them for tests that prefer dependency injection.
"""

from __future__ import annotations

import time

import pytest

from src.core.events import StreamKey

from _helpers import FakeMarket, FakeStorage


@pytest.fixture
def fake_storage() -> FakeStorage:
    """Fresh FakeStorage per test."""
    return FakeStorage()


@pytest.fixture
def fake_market() -> FakeMarket:
    """Fresh FakeMarket per test, no preset history."""
    return FakeMarket()


@pytest.fixture
def now_ms() -> int:
    """Current time in ms (frozen at fixture instantiation)."""
    return int(time.time() * 1000)


@pytest.fixture
def btc_1m_key() -> StreamKey:
    """Convenience BTCUSDT/1m key."""
    return StreamKey(symbol="BTCUSDT", interval="1m")
