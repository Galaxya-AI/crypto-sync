# 0001 — Hexagonal architecture (ports and adapters)

- Status: Accepted
- Date: 2026-04-25

## Context

Earlier prototypes of similar ingestion services tightly coupled domain
logic to Flask routes, SQLAlchemy ORM and Binance helpers in the same
modules. Tests required spinning up MariaDB and a fake HTTP server;
swapping providers or adding a CLI consumer meant editing the same files
that talked to Binance. Refactors were risky.

We needed an architecture that:

1. lets us unit-test the core (supervisor, stream actor) without booting
   any external service,
2. makes provider swaps (Binance → Coinbase) and DB swaps (MariaDB →
   Postgres) a matter of writing a new adapter, never touching the core,
3. survives the next set of features (gap filler, /ohlcv read endpoint,
   metrics) without each adding a new layer of coupling.

## Decision

Adopt **hexagonal architecture** (ports and adapters).

- `src/core/` holds the domain: events, ports (abstract interfaces),
  the stream actor, the supervisor, the gap filler. **Zero I/O imports**.
- `src/binance/`, `src/mariadb/` hold concrete adapters that *implement*
  the ports.
- `src/api/`, `src/cron/` are entry-point adapters: they translate HTTP
  requests / scheduled triggers into domain commands.
- `src/utils/`, `src/logging_module/` hold cross-cutting infrastructure
  used by adapters and entry points only.

Folder layout is **feature-based** (`binance/`, `mariadb/`) rather than
classical "by-layer" (`adapters/binance/`, `adapters/mariadb/`). The
adapter role is conveyed by the folder name, not by a parent directory.

## Consequences

**Pros**

- Core can be tested with in-memory `FakeStorage` / `FakeMarket`
  fixtures (see `tests/conftest.py`). Tests run in milliseconds.
- The same `Supervisor` works against MariaDB, Postgres, or any future
  storage with no changes.
- The /ohlcv endpoint and the gap filler share the storage port: every
  upsert/read path is exercised the same way.

**Cons**

- More files than a "single api.py" project. Mitigated by tooling
  (autocomplete, grep, ruff) which makes navigation cheap.
- Newcomers need to grasp the port/adapter idea before touching the
  codebase. Mitigated by this ADR and the README's architecture
  diagram.
