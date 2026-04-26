"""Structured JSON logging configuration.

Centralizes structlog setup so every module gets the same output
format: ISO timestamps in UTC, log level, logger name, and the
extra key-value pairs passed at the call site. JSON-serialized so
Loki, Datadog or Elasticsearch can parse it without custom rules.

configure_logging() is called once at application startup; after
that, any module obtains a ready-to-use logger via get_logger.
"""
from __future__ import annotations

import logging
import os
import sys
from typing import Any

import structlog
from structlog.stdlib import BoundLogger

LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

def configure_logging(level: str = "INFO") -> None:
    """Configure structlog + stdlib logging to emit JSON to stdout.

    Parameters
    ----------
    level : str
        Root log level, e.g. ``"INFO"``, ``"DEBUG"``. Case-insensitive.
    """
    log_level: int = logging.getLevelName(level.upper())

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )

    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> BoundLogger:
    """Return a structured logger bound to ``name``.

    Parameters
    ----------
    name : str | None
        Logger name, usually ``__name__`` of the caller module.

    Returns
    -------
    BoundLogger
        Ready-to-use structlog logger.
    """
    logger: BoundLogger = structlog.get_logger(name)
    return logger
