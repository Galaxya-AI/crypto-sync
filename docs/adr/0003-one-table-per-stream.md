# 0003 — One MariaDB table per (symbol, interval)

- Status: Accepted
- Date: 2026-04-25

## Context

OHLCV data can be stored either as a single wide table partitioned by
`(symbol, interval)`, or as one table per stream. We weighed both for
this project.

A single table with `(symbol, interval, close_time)` as primary key:

- ✅ classical relational design,
- ❌ a single table grows fast (millions of rows × N symbols),
- ❌ `ALTER TABLE` (e.g. adding a feature column) blocks the whole
  ingestion pipeline,
- ❌ partitioning is required at scale and adds operational cost.

A table per stream:

- ✅ `INSERT … ON DUPLICATE KEY UPDATE` keyed on `close_time` only —
  smaller index, faster writes,
- ✅ each stream evolves independently: add a column to one symbol
  without touching others,
- ✅ table name is the natural scope for backups, retention, exports,
- ❌ schema migrations need to iterate over `stream_registry`,
- ❌ table count grows with the number of activated streams.

## Decision

**One table per `(symbol, interval)`**, named
`ohlcv_<symbol_lower>_<interval_lower>` (e.g. `ohlcv_btcusdt_1h`).
Created on demand by `MariaDBWriter.ensure_table`.

A separate `stream_registry` table tracks every activation so the
supervisor can rebuild state at boot (see ADR 0004 if added later for
auto-restart). Schema migrations under `src/mariadb/migrations/` are
versioned with timestamp prefixes.

To keep schema flexibility without per-table `ALTER` ceremony, every
candle row carries a MariaDB `BLOB extra` column intended for use with
**Dynamic Columns** (key/value extensions in the BLOB) when later
features need to attach metadata to specific candles.

## Consequences

**Pros**

- Writes stay fast even with a large symbol catalog (each writer pool
  hits a small table).
- Schema evolutions are scoped: a new column on `ohlcv_btcusdt_1h`
  does not lock `ohlcv_ethusdt_5m`.
- Backups / drops / exports per stream are trivial.

**Cons**

- Cross-stream queries require `UNION` or a view. We expect consumers
  to read one stream at a time via `/ohlcv/{symbol}/{interval}` so
  this is acceptable.
- Schema migrations affecting all streams must iterate the registry.
  The migrator handles the registry table itself; per-stream changes
  go through `ensure_table` (idempotent DDL) at write time.
