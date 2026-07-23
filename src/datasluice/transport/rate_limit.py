"""Rate-limiting to stay within portal request quotas."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass


@dataclass
class RateLimiter:
    """Thread-safe token-bucket rate limiter.

    Args:
        requests_per_second: Maximum number of requests allowed per second.
    """

    requests_per_second: float

    def __post_init__(self) -> None:
        if self.requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        self._min_interval = 1.0 / self.requests_per_second
        self._last_request: float = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Block until the next request is permitted."""
        with self._lock:
            now = time.monotonic()
            wait = self._last_request + self._min_interval - now
            self._last_request = max(now, self._last_request + self._min_interval)
        if wait > 0:
            time.sleep(wait)
