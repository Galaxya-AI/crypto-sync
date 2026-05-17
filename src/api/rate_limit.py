"""Per-IP rate limiting backed by slowapi.

Internal traffic is well-behaved by definition, so the limiter is set to a
generous default (configurable via ``rate_limit_per_minute`` in Settings).
The goal is not to slow legitimate consumers but to catch buggy clients
that retry-loop a failing call.

The limiter is registered globally on the FastAPI app in
``src/api/main.py``; each route then declares its own ``@limiter.limit(...)``
decorator if a tighter cap makes sense.
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from src.config import get_settings


def build_limiter() -> Limiter:
    """Create a slowapi limiter using the configured per-minute limit.

    Returns
    -------
    Limiter
        Limiter keyed on the client IP (X-Forwarded-For aware).
    """
    rate: int = get_settings().rate_limit_per_minute
    limiter: Limiter = Limiter(
        key_func=get_remote_address,
        default_limits=[f"{rate}/minute"],
        headers_enabled=True,
    )
    return limiter


def install_rate_limiter(app: FastAPI, limiter: Limiter) -> None:
    """Register the limiter on the FastAPI app and wire its 429 handler.

    Parameters
    ----------
    app : FastAPI
        The FastAPI application.
    limiter : Limiter
        Limiter returned by :func:`build_limiter`.
    """
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore[arg-type]


__all__ = ["build_limiter", "install_rate_limiter"]


# Type re-export so call sites get JSONResponse without re-importing FastAPI.
_ = (Request, JSONResponse)
