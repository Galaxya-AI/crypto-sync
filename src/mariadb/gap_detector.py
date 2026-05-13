"""SQL-level gap detector for OHLCV tables.

Uses MariaDB's LAG window function to compute the time difference
between every pair of consecutive finalized candles. Pairs whose
spacing exceeds the expected interval indicate missing rows that
the gap filler will refetch.

This adapter is passive: it only reports gaps. Closing them is the
responsibility of GapFiller, which combines this detector with the
historical Binance adapter.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

import aiomysql

from src.core.events import StreamKey
from src.logging_module.logging import get_logger

log = get_logger(__name__)

_SAFE_TABLE_NAME: re.Pattern[str] = re.compile(r"^ohlcv_[a-z0-9]+_[a-z0-9]+$")


@dataclass(frozen=True, slots=True)
class Gap:
    """A single gap in a candle table.

    Attributes
    ----------
    last_known_close_ms : int
        ``close_time`` of the last row before the gap (already in DB).
    next_close_ms : int
        ``close_time`` of the first row after the gap (already in DB).
    duration_ms : int
        ``next_close_ms - last_known_close_ms``. Always strictly greater than
        the expected interval if we got here.
    missing_bars : int
        Number of bars that should sit inside the gap, computed as
        ``duration_ms / expected_interval_ms - 1``.
    """

    last_known_close_ms: int
    next_close_ms: int
    duration_ms: int
    missing_bars: int


class GapDetector:
    """Find gaps in an OHLCV table using a single SQL window-function query."""

    def __init__(self, pool: aiomysql.Pool) -> None:
        """Store the shared connection pool.

        Parameters
        ----------
        pool : aiomysql.Pool
            Pool created by :class:`~src.mariadb.writer.MariaDBWriter`.
            Reused so every component shares a single pool.
        """
        self._pool: aiomysql.Pool = pool

    async def find_gaps(self, key: StreamKey, expected_interval_ms: int) -> list[Gap]:
        """Return every gap larger than ``expected_interval_ms`` for ``key``.

        Parameters
        ----------
        key : StreamKey
            Stream whose table to inspect.
        expected_interval_ms : int
            Spacing (in ms) two consecutive ``close_time`` values should have.
            Pulled from ``src.core.events.INTERVAL_MS`` by the caller.

        Returns
        -------
        list[Gap]
            One entry per missing range, ordered by ``last_known_close_ms``.
            Empty list if the table is up-to-date or has fewer than 2 rows.

        Raises
        ------
        ValueError
            If ``key.table_name()`` does not match the expected pattern
            (defense against SQL injection through crafted symbols).
        """
        table: str = key.table_name()
        if not _SAFE_TABLE_NAME.fullmatch(table):
            raise ValueError(f"unsafe table name: {table!r}")

        sql: str = f"""
        SELECT prev_close_time, close_time, gap_ms
        FROM (
            SELECT
                close_time,
                LAG(close_time) OVER (ORDER BY close_time) AS prev_close_time,
                close_time
                  - LAG(close_time) OVER (ORDER BY close_time) AS gap_ms
            FROM `{table}`
            WHERE is_closed = 1
        ) AS lagged
        WHERE prev_close_time IS NOT NULL
          AND gap_ms > %s
        ORDER BY prev_close_time;
        """
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, (expected_interval_ms,))
                rows: list[tuple[int, int, int]] = await cur.fetchall()

        gaps: list[Gap] = []
        for prev_close, next_close, gap_ms in rows:
            missing: int = max(0, (gap_ms // expected_interval_ms) - 1)
            gaps.append(
                Gap(
                    last_known_close_ms=int(prev_close),
                    next_close_ms=int(next_close),
                    duration_ms=int(gap_ms),
                    missing_bars=int(missing),
                )
            )

        if gaps:
            log.info(
                "gaps_detected",
                symbol=key.symbol,
                interval=key.interval,
                count=len(gaps),
                missing_total=sum(g.missing_bars for g in gaps),
            )
        else:
            log.debug("no_gaps", symbol=key.symbol, interval=key.interval)
        return gaps


__all__ = ["Gap", "GapDetector"]
