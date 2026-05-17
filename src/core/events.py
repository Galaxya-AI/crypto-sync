"""Domain events and value objects.

These dataclasses flow across ports: they are produced by the Binance
adapters and consumed by the storage adapter. Keeping them in core/
means adapters depend on the core, never the other way around. The
core itself imports nothing technology-specific.

Also exposes INTERVAL_MS, the lookup table that maps Binance kline
intervals to their duration in milliseconds. Used by the gap filler
to compute expected spacing between consecutive candles.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Final

_MIN_MS: Final[int] = 60_000
_HOUR_MS: Final[int] = 60 * _MIN_MS
_DAY_MS: Final[int] = 24 * _HOUR_MS

INTERVAL_MS: Final[dict[str, int]] = {
    "1s": 1_000,
    "1m": 1 * _MIN_MS,
    "3m": 3 * _MIN_MS,
    "5m": 5 * _MIN_MS,
    "15m": 15 * _MIN_MS,
    "30m": 30 * _MIN_MS,
    "1h": 1 * _HOUR_MS,
    "2h": 2 * _HOUR_MS,
    "4h": 4 * _HOUR_MS,
    "6h": 6 * _HOUR_MS,
    "8h": 8 * _HOUR_MS,
    "12h": 12 * _HOUR_MS,
    "1d": 1 * _DAY_MS,
    "3d": 3 * _DAY_MS,
    "1w": 7 * _DAY_MS,
    "1M": 30 * _DAY_MS,  # approximation; Binance counts calendar months
}
"""Milliseconds per Binance kline interval. Used by gap detection to compute
the expected spacing between two consecutive ``close_time`` values."""


class StreamStatus(StrEnum):
    """Lifecycle status of a (symbol, interval) stream."""

    ACTIVE = "active"
    STOPPED = "stopped"


@dataclass(frozen=True, slots=True)
class StreamKey:
    """Unique identifier for a stream, i.e. a (symbol, interval) pair.

    Attributes
    ----------
    symbol : str
        Binance symbol, uppercase, e.g. ``"BTCUSDT"``.
    interval : str
        Candle interval, e.g. ``"1m"``, ``"1h"``, ``"1d"``.
    """

    symbol: str
    interval: str

    def table_name(self) -> str:
        """Return the MariaDB table name for this stream.

        Returns
        -------
        str
            Lowercase table name, e.g. ``"ohlcv_btcusdt_1m"``.
        """
        name: str = f"ohlcv_{self.symbol}_{self.interval}".lower()
        return name


@dataclass(frozen=True, slots=True)
class Candle:
    """A single OHLCV candle (a.k.a. kline) as emitted by Binance.

    All monetary values are stored as :class:`~decimal.Decimal` to avoid
    floating-point drift. Timestamps are milliseconds since Unix epoch (UTC),
    matching Binance's native format.

    Attributes
    ----------
    symbol : str
        Binance symbol, e.g. ``"BTCUSDT"``.
    interval : str
        Candle interval, e.g. ``"1m"``.
    open_time : int
        Candle open time (ms since epoch).
    close_time : int
        Candle close time (ms since epoch).
    open : Decimal
        Price at open_time.
    high : Decimal
        Highest price during the candle.
    low : Decimal
        Lowest price during the candle.
    close : Decimal
        Price at close_time.
    volume : Decimal
        Base asset volume traded during the candle.
    quote_volume : Decimal
        Quote asset volume traded during the candle.
    num_trades : int
        Number of trades that occurred during the candle.
    taker_buy_base_volume : Decimal
        Base asset volume bought by takers.
    taker_buy_quote_volume : Decimal
        Quote asset volume bought by takers.
    is_closed : bool
        True when the candle is finalized. Live in-progress candles set
        this to False; bootstrap REST candles are always True.
    """

    symbol: str
    interval: str
    open_time: int
    close_time: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal
    num_trades: int
    taker_buy_base_volume: Decimal
    taker_buy_quote_volume: Decimal
    is_closed: bool

    def key(self) -> StreamKey:
        """Return the :class:`StreamKey` this candle belongs to.

        Returns
        -------
        StreamKey
            The stream key built from ``symbol`` and ``interval``.
        """
        return StreamKey(symbol=self.symbol, interval=self.interval)


@dataclass(frozen=True, slots=True)
class StreamInfo:
    """Public view of a registered stream returned by the API.

    Attributes
    ----------
    symbol : str
        Binance symbol, e.g. ``"BTCUSDT"``.
    interval : str
        Candle interval, e.g. ``"1m"``.
    status : StreamStatus
        Current lifecycle status.
    table_name : str
        MariaDB table where candles are stored.
    started_at : int
        Millisecond timestamp of the last start.
    stopped_at : int | None
        Millisecond timestamp of the last stop, or None if currently active.
    last_end_ts : int | None
        Close time of the last candle persisted, or None if none yet.
    """

    symbol: str
    interval: str
    status: StreamStatus
    table_name: str
    started_at: int
    stopped_at: int | None
    last_end_ts: int | None
