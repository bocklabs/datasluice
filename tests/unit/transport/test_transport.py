"""Unit tests for transport utilities."""

from __future__ import annotations

import pytest

from datasluice.transport import RateLimiter, RetryPolicy, build_user_agent, paginate
from datasluice.transport.retry import with_retry


def test_build_user_agent() -> None:
    ua = build_user_agent()
    assert "datasluice/" in ua
    assert "Python" in ua


def test_retry_policy_defaults() -> None:
    policy = RetryPolicy()
    assert policy.max_attempts == 3
    assert policy.base_delay == 1.0


def test_with_retry_succeeds() -> None:
    calls = [0]

    def func() -> str:
        calls[0] += 1
        return "ok"

    result = with_retry(func, RetryPolicy(max_attempts=3))
    assert result == "ok"
    assert calls[0] == 1


def test_with_retry_exhausted() -> None:
    attempts = [0]

    def func() -> str:
        attempts[0] += 1
        raise OSError("boom")

    with pytest.raises(OSError):
        with_retry(func, RetryPolicy(max_attempts=3, base_delay=0.01))
    assert attempts[0] == 3


def test_rate_limiter_throttle() -> None:
    limiter = RateLimiter(requests_per_second=100)
    limiter.acquire()
    limiter.acquire()


def test_paginate() -> None:
    def fetch_page(page: int, size: int) -> tuple[list[int], bool]:
        if page > 3:
            return [], False
        return list(range((page - 1) * size, page * size)), page < 3

    pages = list(paginate(fetch_page, page_size=10, max_pages=5))
    assert len(pages) == 3
    assert pages[0] == list(range(0, 10))
