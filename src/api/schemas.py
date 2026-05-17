"""Pydantic request and response models for the HTTP API.

These DTOs sit at the boundary of the application: they validate user
input on incoming requests and shape the JSON returned to clients.
They never leak to the core domain, which uses its own value objects
from src/core/events.py.

Symbol normalization and interval whitelisting happen here so invalid
values are rejected before reaching the supervisor.
"""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator

from src.core.events import StreamInfo, StreamStatus

_ALLOWED_INTERVALS: frozenset[str] = frozenset(
    {
        "1s",
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
        "6h",
        "8h",
        "12h",
        "1d",
        "3d",
        "1w",
        "1M",
    }
)


class StreamCreateRequest(BaseModel):
    """Body of ``POST /streams``."""

    symbol: str = Field(..., examples=["BTCUSDT"], description="Binance symbol, uppercase.")
    interval: str = Field(..., examples=["1m"], description="Candle interval.")

    @field_validator("symbol")
    @classmethod
    def _normalize_symbol(cls, value: str) -> str:
        """Uppercase and strip the symbol, reject empties and weird chars.

        Parameters
        ----------
        value : str
            Raw user input.

        Returns
        -------
        str
            Normalized symbol.

        Raises
        ------
        ValueError
            If the symbol is empty or contains non-alphanumeric chars.
        """
        normalized: str = value.strip().upper()
        if not normalized or not normalized.isalnum():
            raise ValueError("symbol must be non-empty and alphanumeric")
        return normalized

    @field_validator("interval")
    @classmethod
    def _check_interval(cls, value: str) -> str:
        """Validate against the Binance kline interval list.

        Parameters
        ----------
        value : str
            Raw user input.

        Returns
        -------
        str
            The same value, verified.

        Raises
        ------
        ValueError
            If ``value`` is not a recognized Binance kline interval.
        """
        if value not in _ALLOWED_INTERVALS:
            raise ValueError(f"interval must be one of {sorted(_ALLOWED_INTERVALS)}")
        return value


class StreamResponse(BaseModel):
    """Response body representing a stream's state."""

    symbol: str
    interval: str
    status: StreamStatus
    table_name: str
    started_at: int
    stopped_at: int | None
    last_end_ts: int | None

    @classmethod
    def from_info(cls, info: StreamInfo) -> StreamResponse:
        """Build a response DTO from a core :class:`StreamInfo`.

        Parameters
        ----------
        info : StreamInfo
            Core value object.

        Returns
        -------
        StreamResponse
            Wire-compatible DTO.
        """
        response: StreamResponse = cls(
            symbol=info.symbol,
            interval=info.interval,
            status=info.status,
            table_name=info.table_name,
            started_at=info.started_at,
            stopped_at=info.stopped_at,
            last_end_ts=info.last_end_ts,
        )
        return response


class HealthResponse(BaseModel):
    """Response body for ``GET /health``."""

    status: str = "ok"


class CandleResponse(BaseModel):
    """Wire shape of a single OHLCV candle."""

    open_time: int
    close_time: int
    open: str
    high: str
    low: str
    close: str
    volume: str
    quote_volume: str
    num_trades: int
    taker_buy_base_volume: str
    taker_buy_quote_volume: str
    is_closed: bool

    @classmethod
    def from_candle(cls, candle: object) -> CandleResponse:
        """Build a CandleResponse from a core Candle.

        Decimals are serialized as strings to preserve precision over JSON.

        Parameters
        ----------
        candle : object
            A core Candle. Typed loosely to avoid an import cycle.

        Returns
        -------
        CandleResponse
            Wire DTO ready to be returned by FastAPI.
        """
        response: CandleResponse = cls(
            open_time=candle.open_time,  # type: ignore[attr-defined]
            close_time=candle.close_time,  # type: ignore[attr-defined]
            open=str(candle.open),  # type: ignore[attr-defined]
            high=str(candle.high),  # type: ignore[attr-defined]
            low=str(candle.low),  # type: ignore[attr-defined]
            close=str(candle.close),  # type: ignore[attr-defined]
            volume=str(candle.volume),  # type: ignore[attr-defined]
            quote_volume=str(candle.quote_volume),  # type: ignore[attr-defined]
            num_trades=candle.num_trades,  # type: ignore[attr-defined]
            taker_buy_base_volume=str(candle.taker_buy_base_volume),  # type: ignore[attr-defined]
            taker_buy_quote_volume=str(candle.taker_buy_quote_volume),  # type: ignore[attr-defined]
            is_closed=candle.is_closed,  # type: ignore[attr-defined]
        )
        return response
