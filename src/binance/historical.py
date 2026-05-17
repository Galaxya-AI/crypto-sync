"""Binance REST adapter for historical kline data.

Provides the historical half of MarketDataPort. Used to bootstrap the
database when a stream is first activated and to fill gaps detected by
the gap filler when the live WebSocket missed candles.

Relies on binance.AsyncClient.get_historical_klines, which already
paginates over Binance's 1000-candle response limit. Failures are
wrapped in the shared tenacity retry policy from src/utils/retry.py.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any

from binance import AsyncClient
from src.core.events import Candle, StreamKey
from src.logging_module.logging import get_logger
from src.utils.retry import with_exponential_backoff

log = get_logger(__name__)


class BinanceHistoricalAdapter:
    """Thin async wrapper over ``python-binance`` historical endpoint."""

    def __init__(self, client: AsyncClient) -> None:
        """Store the already-created Binance async client.

        Parameters
        ----------
        client : AsyncClient
            A ready-to-use ``binance.AsyncClient`` instance. Lifetime is
            managed by the caller (e.g. created in FastAPI's lifespan).
        """
        self._client: AsyncClient = client

    async def fetch_historical(
        self,
        key: StreamKey,
        start_ms: int,
        end_ms: int | None = None,
    ) -> AsyncIterator[Candle]:
        """Yield historical finalized candles for ``key`` between two timestamps.

        Parameters
        ----------
        key : StreamKey
            (symbol, interval) to fetch.
        start_ms : int
            Inclusive start time in ms since epoch.
        end_ms : int | None
            Inclusive end time in ms since epoch, or ``None`` for "now".

        Yields
        ------
        Candle
            One :class:`Candle` per finalized kline, in chronological order.
        """
        log.info(
            "historical_fetch_start",
            symbol=key.symbol,
            interval=key.interval,
            start_ms=start_ms,
            end_ms=end_ms,
        )

        # Wrapped in retry because Binance returns transient 429 (rate-limited)
        # and 5xx during incident windows; without backoff a single bad minute
        # would surface as a permanent gap-filler failure. Idempotent call: the
        # same (symbol, interval, start, end) tuple always yields the same set
        # of closed candles, so retries can never duplicate or skip rows.
        async for attempt in with_exponential_backoff(max_attempts=5):
            with attempt:
                raw_klines: list[list[Any]] = await self._client.get_historical_klines(
                    symbol=key.symbol,
                    interval=key.interval,
                    start_str=start_ms,
                    end_str=end_ms,
                )

        count: int = 0
        for raw in raw_klines:
            candle: Candle = self._parse_rest_kline(raw, key)
            count += 1
            yield candle

        log.info(
            "historical_fetch_done",
            symbol=key.symbol,
            interval=key.interval,
            count=count,
        )

    @staticmethod
    def _parse_rest_kline(raw: list[Any], key: StreamKey) -> Candle:
        """Turn a raw REST kline array into a :class:`Candle`.

        Parameters
        ----------
        raw : list[Any]
            Binance REST format::

                [ open_time, open, high, low, close, volume,
                  close_time, quote_volume, num_trades,
                  taker_buy_base_volume, taker_buy_quote_volume, ignore ]

        key : StreamKey
            Stream identification for the candle.

        Returns
        -------
        Candle
            Parsed, typed candle with ``is_closed=True`` (REST always
            returns closed candles).
        """
        # Decimal(str(...)) keeps the exact decimal representation Binance
        # serializes; Decimal(float) would silently introduce IEEE-754 noise.
        # is_closed is hard-coded True: REST never returns the in-progress bar.
        candle: Candle = Candle(
            symbol=key.symbol,
            interval=key.interval,
            open_time=int(raw[0]),
            close_time=int(raw[6]),
            open=Decimal(str(raw[1])),
            high=Decimal(str(raw[2])),
            low=Decimal(str(raw[3])),
            close=Decimal(str(raw[4])),
            volume=Decimal(str(raw[5])),
            quote_volume=Decimal(str(raw[7])),
            num_trades=int(raw[8]),
            taker_buy_base_volume=Decimal(str(raw[9])),
            taker_buy_quote_volume=Decimal(str(raw[10])),
            is_closed=True,
        )
        return candle


__all__ = ["BinanceHistoricalAdapter"]
