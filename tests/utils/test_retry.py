"""Unit tests for the tenacity retry policy."""

from __future__ import annotations

import pytest

from src.utils.retry import with_exponential_backoff


@pytest.mark.asyncio
async def test_retries_until_success() -> None:
    """The policy retries on exception and succeeds on the third attempt."""
    attempts: list[int] = []

    async for attempt in with_exponential_backoff(
        max_attempts=5,
        max_wait_seconds=0.01,
    ):
        with attempt:
            attempts.append(len(attempts) + 1)
            if len(attempts) < 3:
                raise RuntimeError("transient")

    assert len(attempts) == 3


@pytest.mark.asyncio
async def test_gives_up_after_max_attempts() -> None:
    """The policy reraises the underlying exception after max_attempts."""
    with pytest.raises(RuntimeError, match="permanent"):
        async for attempt in with_exponential_backoff(
            max_attempts=3,
            max_wait_seconds=0.01,
        ):
            with attempt:
                raise RuntimeError("permanent")
