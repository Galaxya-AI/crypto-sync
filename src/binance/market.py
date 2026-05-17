"""Composite Binance adapter implementing MarketDataPort.

Combines the historical (REST) and realtime (WebSocket) sub-adapters
behind the single MarketDataPort interface used by the core actor and
the gap filler. Composition keeps each sub-adapter focused while the
rest of the codebase depends on a single port.

Swapping providers later (Coinbase, Kraken) means writing equivalent
adapters and a new composite; nothing in core/ has to change.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from src.binance.historical import BinanceHistoricalAdapter
from src.binance.realtime import BinanceRealtimeAdapter
from src.core.events import Candle, StreamKey
from src.core.ports import MarketDataPort


class BinanceMarketDataAdapter(MarketDataPort):
    """Binance-backed :class:`MarketDataPort` (REST + WebSocket)."""

    def __init__(
        self,
        *,
        rest: BinanceHistoricalAdapter,
        websocket: BinanceRealtimeAdapter,
    ) -> None:
        """Wire the sub-adapters.

        Parameters
        ----------
        rest : BinanceHistoricalAdapter
            Historical kline source.
        websocket : BinanceRealtimeAdapter
            Live kline source.
        """
        self._rest: BinanceHistoricalAdapter = rest
        self._websocket: BinanceRealtimeAdapter = websocket

    async def fetch_historical(
        self,
        key: StreamKey,
        start_ms: int,
        end_ms: int | None = None,
    ) -> AsyncIterator[Candle]:
        """Delegate to :meth:`BinanceHistoricalAdapter.fetch_historical`."""
        async for candle in self._rest.fetch_historical(key, start_ms, end_ms):
            yield candle

    async def stream_live(self, key: StreamKey) -> AsyncIterator[Candle]:
        """Delegate to :meth:`BinanceRealtimeAdapter.stream_live`."""
        async for candle in self._websocket.stream_live(key):
            yield candle


__all__ = ["BinanceMarketDataAdapter"]
