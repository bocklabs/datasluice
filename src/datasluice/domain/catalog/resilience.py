"""Bounded retry, timeout, and circuit-breaker contracts."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from urllib.parse import urlsplit, urlunsplit

from datasluice.domain.catalog.safety import IdempotencyPolicy


def _positive_budget(value: float, name: str) -> float:
    if not isinstance(value, int | float) or not isfinite(value) or value <= 0:
        raise ValueError(f"{name} budget must be a finite positive number.")
    return float(value)


@dataclass(frozen=True, slots=True)
class TimeBudget:
    """Independent finite connect, read, write, and total operation budgets."""

    connect: float = 5.0
    read: float = 30.0
    write: float = 30.0
    total: float = 90.0

    def __post_init__(self) -> None:
        values = (
            (self.connect, "Connect"),
            (self.read, "Read"),
            (self.write, "Write"),
            (self.total, "Total"),
        )
        for value, name in values:
            object.__setattr__(self, name.lower(), _positive_budget(value, name))
        if self.total < max(self.connect, self.read, self.write):
            raise ValueError("Total budget cannot be shorter than an individual operation budget.")


@dataclass(frozen=True, slots=True)
class RetryDecision:
    """A typed decision explaining whether and when one attempt may repeat."""

    retry: bool
    delay: float | None
    reason: str

    def __post_init__(self) -> None:
        if type(self.retry) is not bool or not isinstance(self.reason, str) or not self.reason:
            raise ValueError("Retry decisions require a boolean and non-empty reason.")
        invalid_delay = self.delay is not None and (
            not isinstance(self.delay, int | float) or self.delay < 0 or not isfinite(self.delay)
        )
        if invalid_delay:
            raise ValueError("Retry delays must be finite non-negative numbers.")
        if not self.retry and self.delay is not None:
            raise ValueError("Declined retry decisions cannot include a delay.")
        if self.delay is not None:
            object.__setattr__(self, "delay", float(self.delay))

    @classmethod
    def for_response(
        cls,
        *,
        attempt: int,
        max_attempts: int,
        status_code: int | None,
        retry_after: float | None,
        idempotency: IdempotencyPolicy,
        budget: TimeBudget,
    ) -> RetryDecision:
        """Derive a safe retry decision from response state, idempotency, and budgets."""
        if type(attempt) is not int or type(max_attempts) is not int or attempt < 1 or max_attempts < attempt:
            raise ValueError("Retry attempt counts must be positive and ordered.")
        if status_code is not None and (type(status_code) is not int or not 100 <= status_code <= 599):
            raise ValueError("Retry status codes must be valid HTTP status codes.")
        if retry_after is not None and (not isinstance(retry_after, int | float) or retry_after < 0):
            raise ValueError("Retry-After must be a non-negative number.")
        if not isinstance(idempotency, IdempotencyPolicy) or not isinstance(budget, TimeBudget):
            raise ValueError("Retry decisions require typed idempotency and time budgets.")
        if not idempotency.allows_retry():
            return cls(retry=False, delay=None, reason="The operation is not safe to repeat.")
        if attempt >= max_attempts:
            return cls(retry=False, delay=None, reason="The retry attempt limit is exhausted.")
        if status_code not in {None, 408, 425, 429, 500, 502, 503, 504}:
            return cls(retry=False, delay=None, reason="The response is not retryable.")
        delay = float(retry_after) if retry_after is not None else min(2 ** (attempt - 1), budget.total)
        if delay > budget.total:
            return cls(retry=False, delay=None, reason="Retry-After exceeds the total time budget.")
        return cls(retry=True, delay=delay, reason="The response is retryable within the configured budget.")


@dataclass(frozen=True, slots=True)
class CircuitKey:
    """A per-origin, credential-scope circuit-breaker identity."""

    origin: str
    credential_scope: str

    def __post_init__(self) -> None:
        parsed = urlsplit(self.origin)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password:
            raise ValueError("Circuit origins must be sanitized HTTP(S) origins.")
        if not isinstance(self.credential_scope, str) or not self.credential_scope:
            raise ValueError("Circuit credential scopes must be non-empty identifiers.")
        object.__setattr__(self, "origin", urlunsplit((parsed.scheme, parsed.netloc, "", "", "")))


@dataclass(frozen=True, slots=True)
class CircuitState:
    """Inspectable circuit state with finite failure and cooldown bounds."""

    failure_count: int = 0
    failure_threshold: int = 3
    cooldown: float = 30.0
    open: bool = False

    def __post_init__(self) -> None:
        if type(self.failure_count) is not int or self.failure_count < 0:
            raise ValueError("Circuit failure counts must be non-negative integers.")
        if type(self.failure_threshold) is not int or self.failure_threshold < 1:
            raise ValueError("Circuit failure thresholds must be positive integers.")
        object.__setattr__(self, "cooldown", _positive_budget(self.cooldown, "Circuit cooldown"))
        if type(self.open) is not bool:
            raise ValueError("Circuit open state must be a boolean.")

    @property
    def is_available(self) -> bool:
        """Return whether dispatch is currently allowed through the circuit."""
        return not self.open

    def record_failure(self) -> CircuitState:
        """Return a new state after one failed request."""
        failures = self.failure_count + 1
        return CircuitState(
            failure_count=failures,
            failure_threshold=self.failure_threshold,
            cooldown=self.cooldown,
            open=failures >= self.failure_threshold,
        )

    def reset(self) -> CircuitState:
        """Return a closed state after explicit inspection and reset."""
        return CircuitState(failure_threshold=self.failure_threshold, cooldown=self.cooldown)
