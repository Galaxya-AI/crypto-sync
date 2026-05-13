"""Database migration runner.

Applies every .sql file under src/mariadb/migrations/ in lexicographic
order and tracks the filenames already applied in a schema_migrations
table. Runs once at API startup, before the supervisor is created.

Files must be named YYYYMMDDHHMMSS_snake_name.sql so the alphabetical
sort matches the chronological one. Each file runs inside a
transaction so a partial failure rolls the whole file back.
"""
from __future__ import annotations

import re
from pathlib import Path

import aiomysql

from src.logging_module.logging import get_logger

log = get_logger(__name__)

_FILENAME_PATTERN: re.Pattern[str] = re.compile(r"^\d{14}_[a-z0-9_]+\.sql$")
_COMMENT_LINE: re.Pattern[str] = re.compile(r"^\s*--.*$", re.MULTILINE)


class MigrationError(RuntimeError):
    """Raised when a migration file fails to apply."""


class Migrator:
    """Applies pending SQL migrations on startup.

    The migrator is fully stateless except for the connection pool it receives.
    It creates its own tracking table (``schema_migrations``) on first run.
    """

    def __init__(self, pool: aiomysql.Pool, migrations_dir: Path) -> None:
        """Store dependencies.

        Parameters
        ----------
        pool : aiomysql.Pool
            Connection pool used for all migration queries.
        migrations_dir : Path
            Directory containing ``*.sql`` files.
        """
        self._pool: aiomysql.Pool = pool
        self._dir: Path = migrations_dir

    async def run(self) -> list[str]:
        """Apply every pending migration in order.

        Returns
        -------
        list[str]
            Filenames of migrations applied during this run (empty if the DB
            was already up to date).

        Raises
        ------
        MigrationError
            If any statement fails. The offending file is rolled back, but
            previously-applied files in the same run stay committed.
        """
        await self._ensure_tracking_table()
        applied: set[str] = await self._load_applied()
        pending: list[Path] = self._list_pending(applied)

        if not pending:
            log.info("migrations_up_to_date")
            return []

        log.info("migrations_pending", count=len(pending))
        newly_applied: list[str] = []
        for path in pending:
            await self._apply_one(path)
            newly_applied.append(path.name)
            log.info("migration_applied", file=path.name)

        log.info("migrations_done", applied=len(newly_applied))
        return newly_applied

    async def _ensure_tracking_table(self) -> None:
        """Create ``schema_migrations`` if it does not exist."""
        ddl: str = """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            filename   VARCHAR(255) NOT NULL,
            applied_at BIGINT       NOT NULL COMMENT 'ms since epoch',
            PRIMARY KEY (filename)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(ddl)
            await conn.commit()

    async def _load_applied(self) -> set[str]:
        """Return the set of filenames already recorded in the tracking table."""
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT filename FROM schema_migrations;")
                rows: list[tuple[str]] = await cur.fetchall()
        applied: set[str] = {row[0] for row in rows}
        return applied

    def _list_pending(self, applied: set[str]) -> list[Path]:
        """List ``*.sql`` files in the directory that have not been applied.

        Parameters
        ----------
        applied : set[str]
            Filenames already in ``schema_migrations``.

        Returns
        -------
        list[Path]
            Sorted list of pending migration paths.

        Raises
        ------
        MigrationError
            If a filename does not match the expected naming pattern.
        """
        if not self._dir.is_dir():
            log.warning("migrations_dir_missing", path=str(self._dir))
            return []

        files: list[Path] = sorted(self._dir.glob("*.sql"))
        for path in files:
            if not _FILENAME_PATTERN.fullmatch(path.name):
                raise MigrationError(
                    f"Invalid migration filename {path.name!r}: "
                    f"expected '<14-digit-timestamp>_<snake_name>.sql'"
                )
        pending: list[Path] = [path for path in files if path.name not in applied]
        return pending

    async def _apply_one(self, path: Path) -> None:
        """Execute every statement in ``path`` inside a single transaction.

        Parameters
        ----------
        path : Path
            Migration file to apply.

        Raises
        ------
        MigrationError
            If any statement fails. The transaction is rolled back.
        """
        statements: list[str] = self._split_statements(path.read_text(encoding="utf-8"))
        now_ms: int = await self._now_ms()

        async with self._pool.acquire() as conn:
            await conn.begin()
            try:
                async with conn.cursor() as cur:
                    for stmt in statements:
                        await cur.execute(stmt)
                    await cur.execute(
                        "INSERT INTO schema_migrations (filename, applied_at) VALUES (%s, %s);",
                        (path.name, now_ms),
                    )
                await conn.commit()
            except Exception as error:
                await conn.rollback()
                raise MigrationError(f"Migration {path.name} failed: {error}") from error

    async def _now_ms(self) -> int:
        """Return the current time in ms from the database (avoids local clock drift).

        Returns
        -------
        int
            Milliseconds since epoch, as reported by MariaDB's ``UTC_TIMESTAMP``.
        """
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT UNIX_TIMESTAMP(UTC_TIMESTAMP(3)) * 1000;")
                row: tuple[float] | None = await cur.fetchone()
        now_ms: int = int(row[0]) if row is not None else 0
        return now_ms

    @staticmethod
    def _split_statements(sql: str) -> list[str]:
        """Split a SQL file into individual statements.

        Strips ``-- line comments`` then splits on ``;``. Empty statements
        (whitespace-only) are discarded. Does NOT handle triggers or stored
        procedures using ``DELIMITER`` — add that later if needed.

        Parameters
        ----------
        sql : str
            Raw SQL file content.

        Returns
        -------
        list[str]
            Non-empty SQL statements in file order.
        """
        cleaned: str = _COMMENT_LINE.sub("", sql)
        raw_parts: list[str] = cleaned.split(";")
        statements: list[str] = [part.strip() for part in raw_parts if part.strip()]
        return statements


__all__ = ["MigrationError", "Migrator"]
