"""HTTP route to read persisted OHLCV candles from MariaDB.

Read-only counterpart of ``/streams``. Internal consumers (ML pipeline,
dashboards, batch jobs) hit this endpoint to fetch historical data without
talking to MariaDB directly. Rate-limiting is inherited from the global
slowapi limiter.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request, status

from src.api.schemas import CandleResponse
from src.core.events import INTERVAL_MS, Candle, StreamKey
from src.core.ports import StoragePort

router: APIRouter = APIRouter(
    prefix="/ohlcv",
    tags=["ohlcv"],
)


def _storage(request: Request) -> StoragePort:
    """Return the storage port stored on app.state."""
    storage: StoragePort = request.app.state.storage
    return storage


@router.get(
    "/{symbol}/{interval}",
    response_model=list[CandleResponse],
    summary="Read persisted OHLCV candles for a (symbol, interval)",
)
async def read_ohlcv(
    symbol: str,
    interval: str,
    request: Request,
    start: int | None = Query(default=None, description="Inclusive lower bound on close_time (ms)"),
    end: int | None = Query(default=None, description="Inclusive upper bound on close_time (ms)"),
    limit: int = Query(default=500, ge=1, le=10_000),
) -> list[CandleResponse]:
    """Return up to ``limit`` candles for the given stream, ordered by close_time ASC.

    Parameters
    ----------
    symbol : str
        Binance symbol path segment, normalized to uppercase.
    interval : str
        Candle interval. Must be a known Binance kline interval.
    request : Request
        Used to reach the shared storage port.
    start : int | None
        Inclusive lower bound on ``close_time`` (ms since epoch).
    end : int | None
        Inclusive upper bound on ``close_time`` (ms since epoch).
    limit : int
        Maximum number of rows to return (1..10000, default 500).

    Returns
    -------
    list[CandleResponse]
        Candles in chronological order. Empty list if the table is empty
        or if filters exclude every row.

    Raises
    ------
    HTTPException
        400 Bad Request if ``interval`` is not a known kline interval.
    """
    if interval not in INTERVAL_MS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"unknown interval {interval!r}; expected one of {sorted(INTERVAL_MS)}",
        )

    key: StreamKey = StreamKey(symbol=symbol.upper(), interval=interval)
    storage: StoragePort = _storage(request)
    candles: list[Candle] = await storage.read_candles(
        key,
        start_ms=start,
        end_ms=end,
        limit=limit,
    )
    response: list[CandleResponse] = [CandleResponse.from_candle(c) for c in candles]
    return response
