"""Unit tests for retry classification, full-jitter backoff, and Retry-After parsing."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from unittest.mock import patch

import pytest

from datasluice.exceptions import PortalError, RateLimitError, RetryableHTTPError
from datasluice.transport.http_client import _parse_retry_after
from datasluice.transport.retry import RetryPolicy, _full_jitter_delay, with_retry


def test_retry_policy_default_includes_retryable_http_error() -> None:
    policy = RetryPolicy()
    assert RetryableHTTPError in policy.retry_on


def test_retryable_http_error_triggers_retry() -> None:
    calls = [0]

    def func() -> str:
        calls[0] += 1
        if calls[0] < 3:
            raise RetryableHTTPError("svc down", 503)
        return "ok"

    result = with_retry(func, RetryPolicy(max_attempts=3, base_delay=0.01))
    assert result == "ok"
    assert calls[0] == 3


def test_portal_error_does_not_retry() -> None:
    calls = [0]

    def func() -> str:
        calls[0] += 1
        raise PortalError("not found")

    policy = RetryPolicy(max_attempts=3, base_delay=0.01, retry_on=(RetryableHTTPError, OSError))
    with pytest.raises(PortalError):
        with_retry(func, policy)
    assert calls[0] == 1


def test_full_jitter_delay_within_range() -> None:
    for attempt in range(0, 5):
        delay = _full_jitter_delay(1.0, 60.0, attempt)
        assert 0.0 <= delay <= min(60.0, 1.0 * (2**attempt))


def test_full_jitter_delay_caps_at_max_delay() -> None:
    delay = _full_jitter_delay(1.0, 4.0, 10)
    assert 0.0 <= delay <= 4.0


def test_rate_limit_retry_after_capped_at_max_delay() -> None:
    calls = [0]

    def func() -> str:
        calls[0] += 1
        if calls[0] < 2:
            raise RateLimitError("slow down", retry_after=9999.0)
        return "ok"

    sleeps: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    with patch("datasluice.transport.retry.time.sleep", side_effect=fake_sleep):
        result = with_retry(func, RetryPolicy(max_attempts=2, base_delay=0.01, max_delay=5.0))
    assert result == "ok"
    assert sleeps == [5.0]


def test_parse_retry_after_delta_seconds() -> None:
    assert _parse_retry_after("30") == 30.0
    assert _parse_retry_after(" 12 ") == 12.0


def test_parse_retry_after_http_date() -> None:
    future = datetime.now(UTC) + timedelta(seconds=45)
    raw = format_datetime(future, usegmt=True)
    result = _parse_retry_after(raw)
    assert result is not None
    assert 30.0 <= result <= 60.0


def test_parse_retry_after_none_and_garbage() -> None:
    assert _parse_retry_after(None) is None
    assert _parse_retry_after("not-a-date-or-number") is None
