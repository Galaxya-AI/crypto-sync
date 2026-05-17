# 0002 — Cron in a separate container

- Status: Accepted
- Date: 2026-04-25

## Context

The gap filler must run on a fixed schedule (every hour at minute 0
second 3, leaving a small buffer after the candle closes). Three
placement options were considered:

1. **Embedded in the FastAPI process** with `AsyncIOScheduler`.
2. **Separate container** running its own scheduler entry point.
3. **System cron** on the host, hitting the API over HTTP.

Option 1 looks attractive but breaks as soon as the API is scaled to
more than one worker: every uvicorn worker would start its own
scheduler, every job would run N times, the gap filler would hammer Binance and MariaDB.

Option 3 introduces extra moving parts (HTTP IPC, host-level state) and
loses the convenience of sharing the same Docker image and config.

## Decision

Run the cron in a **dedicated container** (`cron` service in
`docker-compose.yml`) using the same Docker image as the API but a
different `command:` (`python -m src.cron.runner`). This is the
classical scheduler-in-its-own-container pattern.

The scheduler is `apscheduler.AsyncIOScheduler` with cron triggers —
no `while True` loops anywhere. SIGTERM handling is wired in
`src/cron/runner.py` for graceful shutdown.

A 30-second heartbeat job touches `/tmp/cron-alive`; the docker-compose
healthcheck reads the file mtime so Docker can restart the container if
the scheduler ever silently dies.

## Consequences

**Pros**

- Exactly one scheduler instance regardless of how many API workers
  serve traffic. No coordination, no advisory lock needed.
- API and cron lifecycles are independent: a slow API redeploy does not
  drop scheduled jobs and vice-versa.
- Same image, different command — no extra build artifact.

**Cons**

- One more container to monitor (mitigated by the heartbeat healthcheck
  on the cron container).
- Slight resource duplication (each container has its own Binance
  client and MariaDB pool). Acceptable at our scale.
