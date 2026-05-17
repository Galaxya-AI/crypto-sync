"""Standalone cron runner — entry point of the dedicated cron container.

The cron is split out of the FastAPI process to guarantee a single
scheduler instance regardless of how many API workers serve traffic,
and to keep API and cron lifecycles independent. Same Docker image
as the API, just a different command in docker-compose.yml.

Wires only what the cron needs: the Binance client, the MariaDB
writer, the gap detector and the gap filler. Listens for SIGTERM
and SIGINT for a graceful shutdown of every resource on exit.
"""

from __future__ import annotations

import asyncio
import signal

from binance import AsyncClient
from src.binance.historical import BinanceHistoricalAdapter
from src.binance.market import BinanceMarketDataAdapter
from src.binance.realtime import BinanceRealtimeAdapter
from src.config import Settings, get_settings
from src.core.gap_filler import GapFiller
from src.logging_module.logging import configure_logging, get_logger
from src.mariadb.gap_detector import GapDetector
from src.mariadb.writer import MariaDBWriter
from src.utils.scheduler import CronScheduler


async def run() -> None:
    """Boot every dependency, register the gap-filler job, then sleep until killed.

    The async function returns only when SIGTERM/SIGINT is received, after a
    graceful shutdown of the scheduler, the DB pool and the Binance client.
    """
    # Order matters: settings -> logging -> first log line. Logging before
    # configure_logging() would emit with the stdlib default handler and bypass
    # JSON formatting, breaking structured-log ingestion for the very first events.
    settings: Settings = get_settings()
    configure_logging(settings.log_level)
    log = get_logger(__name__)
    log.info("cron_starting", db_host=settings.db_host, db_name=settings.db_name)

    # Single AsyncClient shared by both adapters: python-binance keeps an
    # internal aiohttp session and reuses connection pools, so creating two
    # clients would double the open sockets without gain.
    binance_client: AsyncClient = await AsyncClient.create(
        api_key=settings.binance_api_key.get_secret_value(),
        api_secret=settings.binance_secret_key.get_secret_value(),
    )
    historical: BinanceHistoricalAdapter = BinanceHistoricalAdapter(binance_client)
    realtime: BinanceRealtimeAdapter = BinanceRealtimeAdapter(binance_client)
    market: BinanceMarketDataAdapter = BinanceMarketDataAdapter(
        rest=historical,
        websocket=realtime,
    )

    storage: MariaDBWriter = MariaDBWriter(
        host=settings.db_host,
        port=settings.db_port,
        user=settings.db_user,
        password=settings.db_password.get_secret_value(),
        db=settings.db_name,
    )
    await storage.connect()

    gap_detector: GapDetector = GapDetector(pool=storage.pool)
    gap_filler: GapFiller = GapFiller(
        market=market,
        storage=storage,
        gap_detector=gap_detector,
    )
    scheduler: CronScheduler = CronScheduler()
    scheduler.add_hourly(name="gap_filler", coro_func=gap_filler.fill_now)
    # Heartbeat file touched every 30s — Docker HEALTHCHECK reads its mtime
    # to detect a hung event loop that would otherwise pass a TCP probe.
    scheduler.add_heartbeat(name="heartbeat", path="/tmp/cron-alive", every_seconds=30)
    scheduler.start()

    # asyncio-aware signal handling: a plain signal.signal() handler runs in
    # the main thread but cannot wake the event loop, so coroutines stay
    # blocked. add_signal_handler schedules stop_event.set() on the loop,
    # which lets `await stop_event.wait()` return cleanly.
    stop_event: asyncio.Event = asyncio.Event()
    loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    log.info("cron_started")
    try:
        await stop_event.wait()
    finally:
        # Shutdown order is intentional: scheduler first so no in-flight job
        # touches a closed DB pool or a closed Binance session, then storage,
        # then the Binance client. Reversing this raises noisy "session is
        # closed" errors during shutdown logs.
        log.info("cron_stopping")
        await scheduler.shutdown()
        await storage.close()
        await binance_client.close_connection()
        log.info("cron_stopped")


def main() -> None:
    """Synchronous entry point used by ``python -m src.cron.runner``."""
    asyncio.run(run())


if __name__ == "__main__":
    main()
