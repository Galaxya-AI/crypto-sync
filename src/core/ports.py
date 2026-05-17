"""Abstract ports consumed by the core domain.

A port is an interface the core needs to talk to the outside world.
Adapters under src/binance/ and src/mariadb/ implement these ports
with concrete technology. The core never imports adapters directly,
which makes it easy to swap a real adapter for a mock in tests.

Two ports are defined: MarketDataPort for the candle source and
StoragePort for persistence. New ports go here as the domain grows.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterable

from src.core.events import Candle, StreamInfo, StreamKey, StreamStatus


class MarketDataPort(ABC):
    """Source of OHLCV candles (Binance, Coinbase, a fake, …)."""

    @abstractmethod
    def fetch_historical(
        self,
        key: StreamKey,
        start_ms: int,
        end_ms: int | None = None,
    ) -> AsyncIterator[Candle]:
        """Return an async iterator of historical candles for a stream key.

        Concrete implementations are typically ``async def`` with ``yield``,
        which produces an async generator that satisfies AsyncIterator.

        Parameters
        ----------
        key : StreamKey
            The (symbol, interval) to fetch.
        start_ms : int
            Start of the range, inclusive, in ms since epoch.
        end_ms : int | None
            End of the range, inclusive, in ms since epoch. ``None`` means
            "up to now".

        Returns
        -------
        AsyncIterator[Candle]
            Finalized candles in chronological order.
        """
        raise NotImplementedError

    @abstractmethod
    def stream_live(self, key: StreamKey) -> AsyncIterator[Candle]:
        """Return an async iterator of live candles from a WebSocket.

        The iterator yields until the consumer stops iterating or the
        underlying connection raises. Reconnection is the caller's job.

        Parameters
        ----------
        key : StreamKey
            The (symbol, interval) to subscribe to.

        Returns
        -------
        AsyncIterator[Candle]
            Candles as they arrive. In-progress candles have ``is_closed=False``;
            finalized candles have ``is_closed=True``.
        """
        raise NotImplementedError


class StoragePort(ABC):
    """Persistence layer for candles and stream metadata."""

    @abstractmethod
    async def ensure_table(self, key: StreamKey) -> str:
        """Create the candle table for ``key`` if it does not exist.

        Parameters
        ----------
        key : StreamKey
            Stream key whose table should be ensured.

        Returns
        -------
        str
            The final table name that was created/verified.
        """

    @abstractmethod
    async def write_candles(self, key: StreamKey, candles: Iterable[Candle]) -> int:
        """Upsert a batch of candles for ``key``.

        Parameters
        ----------
        key : StreamKey
            Target stream.
        candles : Iterable[Candle]
            Candles to persist. Order does not matter; upsert handles duplicates.

        Returns
        -------
        int
            Number of rows written (inserted + updated).
        """

    @abstractmethod
    async def register_stream(self, key: StreamKey, started_at: int) -> StreamInfo:
        """Register or re-activate a stream in the registry table.

        Parameters
        ----------
        key : StreamKey
            Stream to register.
        started_at : int
            Millisecond timestamp marking this activation.

        Returns
        -------
        StreamInfo
            The stream's public info after registration.
        """

    @abstractmethod
    async def mark_stream_stopped(self, key: StreamKey, stopped_at: int) -> None:
        """Mark a stream as stopped in the registry.

        Parameters
        ----------
        key : StreamKey
            Stream to update.
        stopped_at : int
            Millisecond timestamp marking the stop.
        """

    @abstractmethod
    async def get_last_end_ts(self, key: StreamKey) -> int | None:
        """Return the close time of the last persisted candle for ``key``.

        Parameters
        ----------
        key : StreamKey
            Stream to inspect.

        Returns
        -------
        int | None
            Close time in ms, or ``None`` if the table is empty.
        """

    @abstractmethod
    async def list_streams(self, status: StreamStatus | None = None) -> list[StreamInfo]:
        """List registered streams.

        Parameters
        ----------
        status : StreamStatus | None
            If provided, only return streams with this status.

        Returns
        -------
        list[StreamInfo]
            Matching streams, ordered by registration id.
        """

    @abstractmethod
    async def get_stream(self, key: StreamKey) -> StreamInfo | None:
        """Return the StreamInfo for ``key`` if registered.

        Parameters
        ----------
        key : StreamKey
            Stream to look up.

        Returns
        -------
        StreamInfo | None
            The registration info, or ``None`` if never registered.
        """

    @abstractmethod
    async def read_candles(
        self,
        key: StreamKey,
        *,
        start_ms: int | None = None,
        end_ms: int | None = None,
        limit: int = 500,
    ) -> list[Candle]:
        """Return persisted candles for ``key``, ordered by close_time ASC.

        Parameters
        ----------
        key : StreamKey
            Stream to read.
        start_ms : int | None
            Inclusive lower bound on close_time.
        end_ms : int | None
            Inclusive upper bound on close_time.
        limit : int
            Maximum number of rows to return.

        Returns
        -------
        list[Candle]
            Candles in chronological order. Empty list if the table is empty
            or filters exclude every row.
        """
