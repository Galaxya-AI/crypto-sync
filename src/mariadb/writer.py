"""MariaDB storage adapter using aiomysql.

Implements StoragePort. Each (symbol, interval) gets its own OHLCV
table created on demand by ensure_table. The registry table
stream_registry tracks activation state and survives restarts so
the supervisor can rebuild state at boot time.

Upserts use INSERT ... ON DUPLICATE KEY UPDATE keyed on close_time:
one round-trip instead of SELECT-then-INSERT, and idempotent against
duplicate writes from the gap filler.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from decimal import Decimal
from typing import Any

import aiomysql

from src.core.events import Candle, StreamInfo, StreamKey, StreamStatus
from src.core.ports import StoragePort
from src.logging_module.logging import get_logger

log = get_logger(__name__)

# Table names are interpolated directly into SQL (DDL and DML) because MySQL
# bind parameters cannot identify schema objects. The whitelist regex is the
# only line of defense — any caller supplying an exotic symbol/interval would
# otherwise enable SQL injection. Keep this strict; widen consciously.
_SAFE_TABLE_NAME: re.Pattern[str] = re.compile(r"^ohlcv_[a-z0-9]+_[a-z0-9]+$")


class MariaDBWriter(StoragePort):
    """MariaDB-backed implementation of :class:`StoragePort`.

    The pool is created lazily on :meth:`connect` and closed with
    :meth:`close`. All writes go through :meth:`write_candles` which batches
    via ``executemany`` and upserts via ``ON DUPLICATE KEY UPDATE``.
    """

    def __init__(
        self,
        *,
        host: str,
        port: int,
        user: str,
        password: str,
        db: str,
        pool_minsize: int = 1,
        pool_maxsize: int = 10,
    ) -> None:
        """Store connection parameters. No I/O is performed here.

        Parameters
        ----------
        host : str
            MariaDB host.
        port : int
            MariaDB TCP port.
        user : str
            MariaDB user.
        password : str
            MariaDB user password.
        db : str
            Schema (database) name.
        pool_minsize : int
            Minimum number of pooled connections.
        pool_maxsize : int
            Maximum number of pooled connections.
        """
        self._host: str = host
        self._port: int = port
        self._user: str = user
        self._password: str = password
        self._db: str = db
        self._pool_minsize: int = pool_minsize
        self._pool_maxsize: int = pool_maxsize
        self._pool: aiomysql.Pool | None = None

    async def connect(self) -> None:
        """Create the connection pool. Idempotent."""
        if self._pool is not None:
            return
        # autocommit=True is intentional: every write is an idempotent upsert,
        # we never need multi-statement atomicity, and explicit BEGIN/COMMIT
        # would force an extra round-trip on every batch. utf8mb4 (not utf8)
        # is required so 4-byte symbols and emoji-bearing metadata round-trip
        # losslessly.
        self._pool = await aiomysql.create_pool(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            db=self._db,
            minsize=self._pool_minsize,
            maxsize=self._pool_maxsize,
            autocommit=True,
            charset="utf8mb4",
        )
        log.info("mariadb_pool_opened", host=self._host, db=self._db)

    async def close(self) -> None:
        """Close the connection pool. Idempotent."""
        if self._pool is None:
            return
        self._pool.close()
        await self._pool.wait_closed()
        self._pool = None
        log.info("mariadb_pool_closed")

    @property
    def pool(self) -> aiomysql.Pool:
        """Return the underlying connection pool.

        Exposed so infrastructure tasks that need raw SQL access (e.g.
        :class:`~src.infra.migrator.Migrator`) can share the same pool
        instead of opening their own.

        Returns
        -------
        aiomysql.Pool
            The already-connected pool.

        Raises
        ------
        RuntimeError
            If :meth:`connect` has not been called yet.
        """
        if self._pool is None:
            raise RuntimeError("MariaDBWriter is not connected; call connect() first")
        return self._pool

    async def ensure_table(self, key: StreamKey) -> str:
        """Create the candle table for ``key`` if missing.

        The schema mirrors Binance's kline payload plus a ``extra`` BLOB for
        MariaDB Dynamic Columns (free-form per-row metadata).
        """
        table: str = key.table_name()
        self._check_table_name(table)
        ddl: str = f"""
        CREATE TABLE IF NOT EXISTS `{table}` (
            -- DECIMAL(30,10): 30 total digits with 10 fractional places.
            -- Covers Binance's largest assets (>1e15 quote volume on shitcoins)
            -- with room for the smallest tick sizes (8 decimals on BTC pairs)
            -- without ever falling back to floating-point storage.
            close_time             BIGINT         NOT NULL,
            open_time              BIGINT         NOT NULL,
            `open`                 DECIMAL(30,10) NOT NULL,
            high                   DECIMAL(30,10) NOT NULL,
            low                    DECIMAL(30,10) NOT NULL,
            `close`                DECIMAL(30,10) NOT NULL,
            volume                 DECIMAL(30,10) NOT NULL,
            quote_volume           DECIMAL(30,10) NOT NULL,
            num_trades             BIGINT         NOT NULL,
            taker_buy_base_volume  DECIMAL(30,10) NOT NULL,
            taker_buy_quote_volume DECIMAL(30,10) NOT NULL,
            is_closed              TINYINT(1)     NOT NULL,
            updated_at             BIGINT         NOT NULL,
            extra                  BLOB           NULL,
            PRIMARY KEY (close_time),
            KEY idx_open_time (open_time)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
        async with self._acquire() as (_conn, cur):
            await cur.execute(ddl)
        log.info("table_ensured", table=table)
        return table

    async def write_candles(self, key: StreamKey, candles: Iterable[Candle]) -> int:
        """Batch upsert candles for ``key``.

        Uses ``INSERT ... ON DUPLICATE KEY UPDATE`` keyed on ``close_time``.
        Rows with ``is_closed=True`` never get overwritten by a later
        ``is_closed=False`` message (guarded in the UPDATE clause).

        Returns
        -------
        int
            Number of rows affected. MariaDB counts 1 per insert and 2 per
            update; we divide by 2 for updates to be meaningful, so we just
            return the raw affected count.
        """
        table: str = key.table_name()
        self._check_table_name(table)
        rows: list[tuple[Any, ...]] = [self._candle_to_row(c) for c in candles]
        if not rows:
            return 0

        sql: str = f"""
        INSERT INTO `{table}` (
            close_time, open_time, `open`, high, low, `close`,
            volume, quote_volume, num_trades,
            taker_buy_base_volume, taker_buy_quote_volume,
            is_closed, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open_time              = VALUES(open_time),
            `open`                 = VALUES(`open`),
            high                   = VALUES(high),
            low                    = VALUES(low),
            `close`                = VALUES(`close`),
            volume                 = VALUES(volume),
            quote_volume           = VALUES(quote_volume),
            num_trades             = VALUES(num_trades),
            taker_buy_base_volume  = VALUES(taker_buy_base_volume),
            taker_buy_quote_volume = VALUES(taker_buy_quote_volume),
            -- GREATEST(...) ratchets is_closed: once a candle is finalized
            -- (TRUE=1) a late in-progress message (FALSE=0) cannot demote it.
            -- Important because the gap filler may revisit a bar the WS just
            -- finalized; without the ratchet, ordering races could regress it.
            is_closed              = GREATEST(is_closed, VALUES(is_closed)),
            updated_at             = VALUES(updated_at);
        """
        async with self._acquire() as (_conn, cur):
            await cur.executemany(sql, rows)
            affected: int = cur.rowcount

        log.debug("candles_written", table=table, rows=len(rows), affected=affected)
        return affected

    async def register_stream(self, key: StreamKey, started_at: int) -> StreamInfo:
        """Insert (or reactivate) a row in ``stream_registry`` for ``key``."""
        table: str = key.table_name()
        sql: str = """
        INSERT INTO stream_registry
            (symbol, `interval`, status, table_name, started_at)
        VALUES (%s, %s, 'active', %s, %s)
        ON DUPLICATE KEY UPDATE
            status     = 'active',
            started_at = VALUES(started_at),
            stopped_at = NULL;
        """
        async with self._acquire() as (_conn, cur):
            await cur.execute(sql, (key.symbol, key.interval, table, started_at))

        info: StreamInfo | None = await self.get_stream(key)
        assert info is not None, "stream_registry row was just upserted"
        log.info("stream_registered", symbol=key.symbol, interval=key.interval)
        return info

    async def mark_stream_stopped(self, key: StreamKey, stopped_at: int) -> None:
        """Set the registry row status to 'stopped' for ``key``."""
        sql: str = """
        UPDATE stream_registry
           SET status = 'stopped', stopped_at = %s
         WHERE symbol = %s AND `interval` = %s;
        """
        async with self._acquire() as (_conn, cur):
            await cur.execute(sql, (stopped_at, key.symbol, key.interval))
        log.info("stream_stopped", symbol=key.symbol, interval=key.interval)

    async def read_candles(
        self,
        key: StreamKey,
        *,
        start_ms: int | None = None,
        end_ms: int | None = None,
        limit: int = 500,
    ) -> list[Candle]:
        """Return persisted candles for ``key`` ordered by ``close_time`` ASC."""
        table: str = key.table_name()
        self._check_table_name(table)

        where_clauses: list[str] = []
        params: list[Any] = []
        if start_ms is not None:
            where_clauses.append("close_time >= %s")
            params.append(start_ms)
        if end_ms is not None:
            where_clauses.append("close_time <= %s")
            params.append(end_ms)
        where_sql: str = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        params.append(int(limit))

        sql: str = f"""
        SELECT close_time, open_time, `open`, high, low, `close`,
               volume, quote_volume, num_trades,
               taker_buy_base_volume, taker_buy_quote_volume, is_closed
          FROM `{table}`
          {where_sql}
          ORDER BY close_time ASC
          LIMIT %s;
        """
        async with self._acquire() as (_conn, cur):
            await cur.execute(sql, tuple(params))
            rows: list[tuple[Any, ...]] = await cur.fetchall()

        candles: list[Candle] = [self._row_to_candle(row, key) for row in rows]
        return candles

    @staticmethod
    def _row_to_candle(row: tuple[Any, ...], key: StreamKey) -> Candle:
        """Map a row from ``read_candles`` back to a Candle dataclass."""
        candle: Candle = Candle(
            symbol=key.symbol,
            interval=key.interval,
            close_time=int(row[0]),
            open_time=int(row[1]),
            open=Decimal(str(row[2])),
            high=Decimal(str(row[3])),
            low=Decimal(str(row[4])),
            close=Decimal(str(row[5])),
            volume=Decimal(str(row[6])),
            quote_volume=Decimal(str(row[7])),
            num_trades=int(row[8]),
            taker_buy_base_volume=Decimal(str(row[9])),
            taker_buy_quote_volume=Decimal(str(row[10])),
            is_closed=bool(row[11]),
        )
        return candle

    async def get_last_end_ts(self, key: StreamKey) -> int | None:
        """Return the max ``close_time`` for ``key`` or ``None`` if empty."""
        table: str = key.table_name()
        self._check_table_name(table)
        sql: str = f"SELECT MAX(close_time) FROM `{table}`;"
        async with self._acquire() as (_conn, cur):
            await cur.execute(sql)
            row: tuple[Any, ...] | None = await cur.fetchone()
        last: int | None = int(row[0]) if row is not None and row[0] is not None else None
        return last

    async def list_streams(self, status: StreamStatus | None = None) -> list[StreamInfo]:
        """Return all streams, optionally filtered by ``status``."""
        if status is None:
            sql: str = "SELECT * FROM stream_registry ORDER BY id;"
            params: tuple[Any, ...] = ()
        else:
            sql = "SELECT * FROM stream_registry WHERE status = %s ORDER BY id;"
            params = (status.value,)

        async with self._acquire(dict_cursor=True) as (_conn, cur):
            await cur.execute(sql, params)
            rows: list[dict[str, Any]] = await cur.fetchall()

        streams: list[StreamInfo] = [self._row_to_stream_info(r) for r in rows]
        return streams

    async def get_stream(self, key: StreamKey) -> StreamInfo | None:
        """Return the registry row for ``key`` or ``None`` if not found."""
        sql: str = "SELECT * FROM stream_registry WHERE symbol = %s AND `interval` = %s;"
        async with self._acquire(dict_cursor=True) as (_conn, cur):
            await cur.execute(sql, (key.symbol, key.interval))
            row: dict[str, Any] | None = await cur.fetchone()
        info: StreamInfo | None = self._row_to_stream_info(row) if row else None
        return info

    def _acquire(self, *, dict_cursor: bool = False) -> _ConnectionContext:
        """Return an async context manager yielding (conn, cursor)."""
        if self._pool is None:
            raise RuntimeError("MariaDBWriter is not connected; call connect() first")
        return _ConnectionContext(self._pool, dict_cursor=dict_cursor)

    @staticmethod
    def _check_table_name(name: str) -> None:
        """Validate that ``name`` matches the expected pattern.

        Raises
        ------
        ValueError
            If the name contains anything unexpected (defense against SQL
            injection through crafted symbols).
        """
        if not _SAFE_TABLE_NAME.fullmatch(name):
            raise ValueError(f"unsafe table name: {name!r}")

    @staticmethod
    def _candle_to_row(candle: Candle) -> tuple[Any, ...]:
        """Convert a :class:`Candle` to the tuple expected by ``INSERT``."""
        row: tuple[Any, ...] = (
            candle.close_time,
            candle.open_time,
            candle.open,
            candle.high,
            candle.low,
            candle.close,
            candle.volume,
            candle.quote_volume,
            candle.num_trades,
            candle.taker_buy_base_volume,
            candle.taker_buy_quote_volume,
            1 if candle.is_closed else 0,
            candle.close_time,
        )
        return row

    @staticmethod
    def _row_to_stream_info(row: dict[str, Any]) -> StreamInfo:
        """Convert a ``stream_registry`` row (as dict) to :class:`StreamInfo`."""
        info: StreamInfo = StreamInfo(
            symbol=row["symbol"],
            interval=row["interval"],
            status=StreamStatus(row["status"]),
            table_name=row["table_name"],
            started_at=int(row["started_at"]),
            stopped_at=int(row["stopped_at"]) if row.get("stopped_at") is not None else None,
            last_end_ts=int(row["last_end_ts"]) if row.get("last_end_ts") is not None else None,
        )
        return info


class _ConnectionContext:
    """Internal async context manager yielding (connection, cursor).

    Keeps connection acquisition and cursor creation in one ``async with``,
    while ensuring both are always released/closed.
    """

    def __init__(self, pool: aiomysql.Pool, *, dict_cursor: bool = False) -> None:
        """Store pool and cursor options."""
        self._pool: aiomysql.Pool = pool
        self._dict_cursor: bool = dict_cursor
        self._conn: aiomysql.Connection | None = None
        self._cursor: aiomysql.Cursor | None = None

    async def __aenter__(self) -> tuple[aiomysql.Connection, aiomysql.Cursor]:
        """Acquire a connection and open a cursor."""
        self._conn = await self._pool.acquire()
        cursor_cls: type[aiomysql.Cursor] = (
            aiomysql.DictCursor if self._dict_cursor else aiomysql.Cursor
        )
        self._cursor = await self._conn.cursor(cursor_cls)
        return self._conn, self._cursor

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Close the cursor and release the connection back to the pool."""
        if self._cursor is not None:
            await self._cursor.close()
        if self._conn is not None:
            self._pool.release(self._conn)


__all__ = ["MariaDBWriter"]
