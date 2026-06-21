"""Retry logic with exponential backoff for HTTP requests."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass

from datasluice.exceptions import RateLimitError
from datasluice.logging import get_logger

logger = get_logger("transport.retry")


@dataclass(frozen=True)
class RetryPolicy:
    """Configuration for retrying transient failures.

    Attributes:
        max_attempts: Maximum number of attempts (including the first).
        base_delay: Base delay in seconds before the first retry.
        max_delay: Maximum delay between retries.
        backoff_factor: Multiplier applied to the delay after each attempt.
        retry_on: Tuple of exception types that should trigger a retry.
    """

    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    backoff_factor: float = 2.0
    retry_on: tuple[type[Exception], ...] = (RateLimitError, OSError)


def with_retry[T](
    func: Callable[[], T],
    policy: RetryPolicy | None = None,
) -> T:
    """Execute *func*, retrying on transient failures per *policy*.

    Args:
        func: Zero-argument callable to execute.
        policy: Retry configuration (defaults to a new :class:`RetryPolicy`).

    Returns:
        The return value of *func*.

    Raises:
        The last exception if all attempts are exhausted.
    """
    policy = policy or RetryPolicy()
    delay = policy.base_delay
    last_exc: Exception = RuntimeError("No retries attempted")

    for attempt in range(1, policy.max_attempts + 1):
        try:
            return func()
        except policy.retry_on as exc:
            last_exc = exc
            if isinstance(exc, RateLimitError) and exc.retry_after is not None:
                sleep = min(exc.retry_after, policy.max_delay)
            elif attempt < policy.max_attempts:
                sleep = min(delay, policy.max_delay)
                delay *= policy.backoff_factor
            else:
                break
            logger.warning("Attempt %d/%d failed: %s — retrying in %.1fs", attempt, policy.max_attempts, exc, sleep)
            time.sleep(sleep)

    raise last_exc  # type: ignore[misc]
