# crypto-sync

Real-time Binance OHLCV ingestion service for internal consumers (ML pipelines,
dashboards, batch jobs). Built around three goals:

1. **Ingest live candle data** for any `(symbol, interval)` activated via the API,
   with automatic gap recovery if the WebSocket drops.
2. **Persist** every candle to MariaDB so consumers can query it later.
3. **Be operable** in a pro environment: healthchecks, backups, runbook.

---

## Quickstart (local)

```bash
cp .env.example .env       # set BINANCE_API_KEY/SECRET and DB passwords
docker compose up --build  # starts mariadb + api + cron + backup
```

Activate a stream:

```bash
curl -X POST http://localhost:8000/streams \
  -H "Content-Type: application/json" \
  -d '{"symbol":"BTCUSDT","interval":"1h"}'
```

Read the persisted candles:

```bash
curl http://localhost:8000/ohlcv/BTCUSDT/1h?limit=10
```

OpenAPI docs: <http://localhost:8000/docs>.

---

## Architecture

```
                                                  Binance
                                                ┌────────┐
                          REST (bootstrap, gap) │        │
            ┌────────────────────────────────── │        │
            │                                    └────────┘
            │                                       ▲
            │                                       │ WS (live)
            ▼                                       │
   ┌────────────────────┐                  ┌────────┴───────┐
   │   FastAPI (api/)   │                  │ StreamActor    │
   │  POST /streams ────┼──► Supervisor ──►│ (one per       │
   │  GET  /ohlcv       │   start/stop     │  symbol+interval)
   │  GET  /streams     │                  └────────┬───────┘
   │  GET  /health      │                           │
   │                    │                           ▼
   └─────────┬──────────┘                  ┌────────────────┐
             │                              │  MariaDBWriter │
             │                              │ ohlcv_<sym>_<iv>│
             ▼                              └────────────────┘
     slowapi (per-IP)                              ▲
                                                   │
                                              ┌────┴────┐
                                              │  cron/  │
                                              │ container│
                                              └────┬────┘
                                                   │ APScheduler @ xx:00:03
                                                   ▼
                                          GapDetector (LAG SQL)
                                          GapFiller   (REST refetch)
```

Three containers run in production:

| Container | Image | Purpose |
|---|---|---|
| `mariadb` | `mariadb:11.4` | Storage |
| `api` | this repo | HTTP API + WebSocket actors per stream (1 worker, mandatory) |
| `cron` | this repo (different `command`) | Scheduler + gap detector + gap filler |
| `backup` | `deploy/backup/Dockerfile` | Daily `mysqldump` + optional S3 upload |

An optional Grafana container (defined in `docker-compose.yml` behind the
`grafana` profile) can be added on top, wired to MariaDB so the OHLCV
history can be charted. See [Visualization](#visualization-opt-in) below.

---

## Project layout

```
src/
├── api/                FastAPI: routes, schemas, rate limit
├── binance/            historical (REST) + realtime (WS) + market (composite)
├── core/               domain: events, ports, stream actor, supervisor, gap filler
├── cron/               standalone runner for the cron container
├── logging_module/     structlog setup
├── mariadb/            storage adapter, migrator, gap detector, migrations/
└── utils/              retry policy, scheduler wrapper

tests/                  pytest-asyncio, in-memory fakes, FastAPI TestClient
deploy/
├── grafana/            dashboard JSON + datasource provisioning
└── backup/             daily mysqldump cron container
docs/
├── adr/                Architecture Decision Records
└── RUNBOOK.md          operational playbook
```

See [BACKLOG.md](BACKLOG.md) for what's done, in-flight, and queued.

---

## API surface

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/streams` | Activate ingestion for `(symbol, interval)` |
| `DELETE` | `/streams/{symbol}/{interval}` | Stop ingestion |
| `GET` | `/streams` | List registered streams |
| `GET` | `/ohlcv/{symbol}/{interval}` | Read persisted candles (paginated by `start`/`end`/`limit`) |
| `GET` | `/health` | Liveness probe |
| `GET` | `/docs` | OpenAPI / Swagger UI |

The API is open by design (internal-only network); user-level auth (JWT
or session) belongs to the consumer applications, not this ingestion
service.

---

## Running tests

```bash
pip install -e .[dev]
pytest --cov=src --cov-report=term-missing
```

CI runs `ruff`, `mypy`, `pytest` and a Docker build on every push to `main`
(see `.github/workflows/ci.yml`).

---

## Configuration

All configuration is loaded from environment variables (see `.env.example`
for the full list). Notable knobs:

| Variable | Default | Purpose |
|---|---|---|
| `BINANCE_API_KEY` / `BINANCE_SECRET_KEY` | required | Binance credentials |
| `DB_*` | required | MariaDB connection |
| `RATE_LIMIT_PER_MINUTE` | `100` | Per-IP slowapi limit |
| `BACKUP_S3_BUCKET` | empty | If set, the backup container uploads dumps to S3 |
| `BACKUP_RETENTION_DAYS` | `7` | Local retention for `mysqldump` files |

---

## Visualization (opt-in)

```bash
docker compose --profile grafana up
```

Grafana on `:3000` (admin / `${GRAFANA_ADMIN_PASSWORD}`) is provisioned with
the MariaDB datasource and the **Crypto Prices — BTCUSDT 1h** dashboard
(Japanese candlesticks + volume + counters). All persisted candles are
queryable as a normal MySQL/MariaDB datasource.

---

## See also

- [BACKLOG.md](BACKLOG.md) — sprint planning and open items
- [docs/RUNBOOK.md](docs/RUNBOOK.md) — operations playbook
- [docs/adr/](docs/adr/) — design decisions and their rationale
