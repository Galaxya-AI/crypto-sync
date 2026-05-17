"""FastAPI application entry point.

Wires every long-lived dependency once via the lifespan context manager:
the Binance async client, the historical and realtime adapters, the
MariaDB writer, the migration runner and the stream supervisor. On
shutdown, resources are torn down in reverse order so in-flight tasks
can finish cleanly.

The cron scheduler is not started here. It runs in its own container
(see src/cron/runner.py) so it stays a single instance regardless of
how many uvicorn workers serve the API.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from binance import AsyncClient
from src.api.rate_limit import build_limiter, install_rate_limiter
from src.api.routes_ohlcv import router as ohlcv_router
from src.api.routes_streams import router as streams_router
from src.api.schemas import HealthResponse
from src.binance.historical import BinanceHistoricalAdapter
from src.binance.market import BinanceMarketDataAdapter
from src.binance.realtime import BinanceRealtimeAdapter
from src.config import Settings, get_settings
from src.core.stream_supervisor import Supervisor
from src.logging_module.logging import configure_logging, get_logger
from src.mariadb.migrator import Migrator
from src.mariadb.writer import MariaDBWriter

_MIGRATIONS_DIR: Path = Path(__file__).resolve().parents[1] / "mariadb" / "migrations"


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncIterator[None]:
    """Start and stop all long-lived resources.

    Parameters
    ----------
    application : FastAPI
        The FastAPI app. Shared resources are attached to ``application.state``.

    Yields
    ------
    None
        Yields control to the running server; on exit, teardown runs.
    """
    settings: Settings = get_settings()
    configure_logging(settings.log_level)
    log = get_logger(__name__)
    log.info("app_starting", db_host=settings.db_host, db_name=settings.db_name)

    binance_client: AsyncClient = await AsyncClient.create(
        api_key=settings.binance_api_key.get_secret_value(),
        api_secret=settings.binance_secret_key.get_secret_value(),
    )
    rest_adapter: BinanceHistoricalAdapter = BinanceHistoricalAdapter(binance_client)
    ws_adapter: BinanceRealtimeAdapter = BinanceRealtimeAdapter(binance_client)
    market: BinanceMarketDataAdapter = BinanceMarketDataAdapter(
        rest=rest_adapter,
        websocket=ws_adapter,
    )

    storage: MariaDBWriter = MariaDBWriter(
        host=settings.db_host,
        port=settings.db_port,
        user=settings.db_user,
        password=settings.db_password.get_secret_value(),
        db=settings.db_name,
    )
    await storage.connect()

    migrator: Migrator = Migrator(pool=storage.pool, migrations_dir=_MIGRATIONS_DIR)
    applied: list[str] = await migrator.run()
    log.info("migrations_applied_on_startup", files=applied)

    supervisor: Supervisor = Supervisor(
        market=market,
        storage=storage,
    )
    restored: int = await supervisor.restore_active_streams()
    log.info("streams_restored_on_startup", count=restored)

    application.state.binance_client = binance_client
    application.state.storage = storage
    application.state.supervisor = supervisor
    log.info("app_started")

    try:
        yield
    finally:
        log.info("app_stopping")
        await supervisor.shutdown()
        await storage.close()
        await binance_client.close_connection()
        log.info("app_stopped")


app: FastAPI = FastAPI(
    title="crypto-sync",
    version="0.1.0",
    summary="Real-time Binance OHLCV ingestion API.",
    lifespan=lifespan,
)
install_rate_limiter(app, build_limiter())
app.include_router(streams_router)
app.include_router(ohlcv_router)


@app.get("/health", response_model=HealthResponse, tags=["meta"])
async def health() -> HealthResponse:
    """Simple liveness probe used by Docker healthchecks and monitoring.

    Returns
    -------
    HealthResponse
        Always ``{"status": "ok"}`` if the process is up.
    """
    return HealthResponse()
