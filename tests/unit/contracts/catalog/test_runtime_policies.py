from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict
from typing import cast

import pytest

from datasluice.domain.catalog.observability import DiagnosticPolicy, StructuredEvent, TelemetryPolicy, TLSPolicy
from datasluice.domain.catalog.resilience import CircuitKey, CircuitState, RetryDecision, TimeBudget
from datasluice.domain.catalog.safety import (
    BulkExecutionPolicy,
    ConcurrencyPolicy,
    ConfirmationPolicy,
    DryRunPolicy,
    IdempotencyPolicy,
    MutationPolicy,
)


def test_destructive_and_retry_work_require_explicit_safe_policies() -> None:
    destructive = MutationPolicy(destructive=True)
    confirmed = MutationPolicy(destructive=True, confirmation=ConfirmationPolicy(confirmed=True))

    assert not destructive.allows_execution()
    assert confirmed.allows_execution()
    assert MutationPolicy().allows_execution()
    assert not IdempotencyPolicy().allows_retry()
    assert IdempotencyPolicy(key="request-123").allows_retry()
    assert IdempotencyPolicy(explicit_retry_opt_in=True).allows_retry()


def test_concurrency_dry_run_and_native_bulk_atomicity_are_capability_guarded() -> None:
    assert not ConcurrencyPolicy().allows_execution()
    assert ConcurrencyPolicy(token="etag-1").allows_execution()
    assert ConcurrencyPolicy(overwrite=True).allows_execution()
    assert not DryRunPolicy(requested=True).allows_execution(capability_supported=False)
    assert DryRunPolicy(requested=True).allows_execution(capability_supported=True)
    with pytest.raises(ValueError):
        BulkExecutionPolicy(atomicity="atomic", native_atomic_available=False)
    assert BulkExecutionPolicy(atomicity="atomic", native_atomic_available=True, max_parallelism=1).is_atomic


def test_time_budgets_retry_after_and_credential_aware_circuits_are_typed() -> None:
    budget = TimeBudget(connect=1, read=2, write=3, total=8)
    decision = RetryDecision.for_response(
        attempt=1,
        max_attempts=3,
        status_code=429,
        retry_after=4,
        idempotency=IdempotencyPolicy(safe=True),
        budget=budget,
    )

    assert decision.retry
    assert decision.delay == 4
    assert CircuitKey(origin="https://catalog.example", credential_scope="team-a") != CircuitKey(
        origin="https://catalog.example", credential_scope="team-b"
    )
    assert CircuitState().is_available


@pytest.mark.parametrize(
    ("attempt", "max_attempts", "status_code", "retry_after", "idempotency", "reason"),
    [
        (3, 3, 503, None, IdempotencyPolicy(safe=True), "The retry attempt limit is exhausted."),
        (1, 3, 400, None, IdempotencyPolicy(safe=True), "The response is not retryable."),
        (1, 3, 503, None, IdempotencyPolicy(), "The operation is not safe to repeat."),
        (1, 3, 429, 9, IdempotencyPolicy(safe=True), "Retry-After exceeds the total time budget."),
    ],
)
def test_retry_decision_declines_unsafe_or_unavailable_retries(
    attempt: int,
    max_attempts: int,
    status_code: int,
    retry_after: float | None,
    idempotency: IdempotencyPolicy,
    reason: str,
) -> None:
    """Retry decisions explain every negative safety and budget guard."""
    decision = RetryDecision.for_response(
        attempt=attempt,
        max_attempts=max_attempts,
        status_code=status_code,
        retry_after=retry_after,
        idempotency=idempotency,
        budget=TimeBudget(connect=1, read=2, write=3, total=8),
    )

    assert not decision.retry
    assert decision.delay is None
    assert decision.reason == reason


def test_tls_diagnostics_events_and_telemetry_have_secure_defaults() -> None:
    assert TLSPolicy().verify
    with pytest.raises(ValueError):
        TLSPolicy(verify=False)
    assert not TLSPolicy(verify=False, override_scope="development").verify
    with pytest.raises(ValueError):
        DiagnosticPolicy(include_raw_body=True)
    diagnostic = DiagnosticPolicy(include_raw_body=True, raw_body_max_bytes=128)
    assert diagnostic.bound_raw_body(b"x" * 256) == b"x" * 128
    event = StructuredEvent(name="catalog.request", metadata={"token": "secret", "request_id": "abc"})
    assert event.metadata["token"] == "***"
    assert "secret" not in str(asdict(event))
    assert not TelemetryPolicy().enabled


def test_structured_event_sequences_are_truncated_and_redacted_recursively() -> None:
    event = StructuredEvent(
        name="catalog.request",
        metadata={"notes": ["n" * 512, {"api_key": "secret", "depth": ["d" * 512]}]},
    )

    notes = cast(tuple[object, ...], event.metadata["notes"])
    assert notes[0] == "n" * 256
    nested = cast(Mapping[str, object], notes[1])
    assert nested["api_key"] == "***"
    assert cast(tuple[object, ...], nested["depth"])[0] == "d" * 256

    with pytest.raises(ValueError, match="sequence exceeds the entry limit"):
        StructuredEvent(name="catalog.request", metadata={"notes": ["x"] * 33})
