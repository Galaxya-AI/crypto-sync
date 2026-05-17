"""Binance WebSocket adapter for live kline data.

Provides the live half of MarketDataPort. Yields both in-progress
candles (is_closed=False) and finalized candles (is_closed=True) as
they arrive from Binance. The caller decides what to persist; the
MariaDB writer upserts on close_time so duplicates are harmless.

Reconnection is delegated to the caller (the stream actor) so the full
lifecycle of a stream stays in one place: bootstrap, live, crash,
reconnect, repeat.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any

from binance import AsyncClient, BinanceSocketManager
from src.core.events import Candle, StreamKey
from src.logging_module.logging import get_logger

log = get_logger(__name__)

# Binance pings the WS roughly every minute; missing three pings in a row
# means the socket is silently dead (NAT timeout, mid-route drop). Raising
# TimeoutError lets the supervising actor reconnect rather than block forever.
_RECV_TIMEOUT_SECONDS: float = 180.0


class BinanceRealtimeAdapter:
    """Thin async wrapper over ``python-binance`` WebSocket kline stream."""

    def __init__(self, client: AsyncClient) -> None:
        """Store the already-created Binance async client.

        Parameters
        ----------
        client : AsyncClient
            A ready-to-use ``binance.AsyncClient`` instance. Lifetime is
            managed by the caller (e.g. created in FastAPI's lifespan).
        """
        self._client: AsyncClient = client
        self._socket_manager: BinanceSocketManager = BinanceSocketManager(client)

    async def stream_live(self, key: StreamKey) -> AsyncIterator[Candle]:
        """Yield candles from the kline WebSocket until cancelled.

        Parameters
        ----------
        key : StreamKey
            (symbol, interval) to subscribe to.

        Yields
        ------
        Candle
            Candles emitted by Binance. In-progress candles carry the latest
            snapshot of the still-open bar; finalized candles (``is_closed=True``)
            appear once when the bar closes.

        Raises
        ------
        asyncio.TimeoutError
            If no message arrives within :data:`_RECV_TIMEOUT_SECONDS`.
            The caller is expected to treat this as a reconnect trigger.
        """
        log.info("ws_connect", symbol=key.symbol, interval=key.interval)
        socket: Any = self._socket_manager.kline_socket(
            symbol=key.symbol,
            interval=key.interval,
        )
        async with socket as stream:
            while True:
                message: dict[str, Any] = await asyncio.wait_for(
                    stream.recv(),
                    timeout=_RECV_TIMEOUT_SECONDS,
                )
                candle: Candle | None = self._parse_ws_message(message, key)
                if candle is None:
                    continue
                yield candle

    @staticmethod
    def _parse_ws_message(message: dict[str, Any], key: StreamKey) -> Candle | None:
        """Parse a Binance WebSocket kline message into a :class:`Candle`.

        Parameters
        ----------
        message : dict[str, Any]
            Raw Binance payload. Expected shape::

                {
                    "e": "kline",
                    "E": <event_time_ms>,
                    "s": "BTCUSDT",
                    "k": {
                        "t": <open_time_ms>, "T": <close_time_ms>,
                        "i": "1m",
                        "o": "...", "h": "...", "l": "...", "c": "...",
                        "v": "...", "q": "...",
                        "V": "...", "Q": "...",
                        "n": <num_trades>, "x": <is_closed>,
                        ...
                    }
                }

        key : StreamKey
            Stream identification to attach to the candle.

        Returns
        -------
        Candle | None
            Parsed candle, or ``None`` if the message is an error/heartbeat
            that should be skipped by the caller.
        """
        event_type: str | None = message.get("e")
        if event_type == "error":
            log.error("ws_error_message", payload=message)
            return None
        if event_type != "kline":
            log.debug("ws_skip_non_kline", event_type=event_type)
            return None

        kline: dict[str, Any] = message["k"]
        # Decimal(str(x)) — never Decimal(x) directly. Binance sends prices as
        # strings precisely to avoid IEEE-754 rounding; building Decimal from
        # the string preserves that exactness, while Decimal(float) would
        # introduce sub-cent drift that compounds across millions of rows.
        candle: Candle = Candle(
            symbol=key.symbol,
            interval=key.interval,
            open_time=int(kline["t"]),
            close_time=int(kline["T"]),
            open=Decimal(str(kline["o"])),
            high=Decimal(str(kline["h"])),
            low=Decimal(str(kline["l"])),
            close=Decimal(str(kline["c"])),
            volume=Decimal(str(kline["v"])),
            quote_volume=Decimal(str(kline["q"])),
            num_trades=int(kline["n"]),
            taker_buy_base_volume=Decimal(str(kline["V"])),
            taker_buy_quote_volume=Decimal(str(kline["Q"])),
            is_closed=bool(kline["x"]),
        )
        return candle


__all__ = ["BinanceRealtimeAdapter"]
