from __future__ import annotations

from dataclasses import asdict

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
