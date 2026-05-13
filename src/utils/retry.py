"""Retry policy factory built on tenacity.

Exposes with_exponential_backoff, the single source of truth for how
the project handles transient I/O errors: exponential wait with
jitter, capped at 60 seconds, with structured logs before each
sleep so retries are observable.

Concentrating the policy here means tuning it later (more attempts,
different jitter, finer exception filtering) only requires editing
this file. Currently used by the historical Binance adapter.
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from src.logging_module.logging import get_logger

F = TypeVar("F", bound=Callable[..., Any])


def with_exponential_backoff(
    *,
    max_attempts: int = 5,
    max_wait_seconds: float = 60.0,
    retry_on: type[BaseException] | tuple[type[BaseException], ...] = Exception,
) -> AsyncRetrying:
    """Return a tenacity :class:`AsyncRetrying` policy with sane defaults.

    Parameters
    ----------
    max_attempts : int
        Maximum number of attempts before giving up (including the first call).
    max_wait_seconds : float
        Upper bound of the exponential backoff wait time.
    retry_on : type[BaseException] | tuple[type[BaseException], ...]
        Exception type(s) that trigger a retry. Others propagate immediately.

    Returns
    -------
    AsyncRetrying
        A ready-to-use async retrying controller. Iterate with
        ``async for attempt in policy: with attempt: await ...``.
    """
    log = get_logger(__name__)

    async def _log_before_sleep(retry_state: Any) -> None:
        """Log a warning before sleeping between attempts."""
        wait_seconds: float = getattr(retry_state.next_action, "sleep", 0.0)
        log.warning(
            "retrying_after_error",
            attempt=retry_state.attempt_number,
            wait_seconds=wait_seconds,
            error=str(retry_state.outcome.exception()) if retry_state.outcome else None,
        )

    policy: AsyncRetrying = AsyncRetrying(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential_jitter(initial=1.0, max=max_wait_seconds, jitter=1.0),
        retry=retry_if_exception_type(retry_on),
        before_sleep=_log_before_sleep,
        reraise=True,
    )
    return policy
