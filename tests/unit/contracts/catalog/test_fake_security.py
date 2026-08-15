"""Security boundary tests for deterministic reference catalog fakes."""

from __future__ import annotations

import json

import pytest

from datasluice.contracts.catalog.fakes import SyncReferenceConnector
from datasluice.contracts.catalog.fixtures import load_reference_fixture_set
from datasluice.domain.catalog.observability import DiagnosticPolicy
from datasluice.domain.catalog.safety import IdempotencyPolicy
from datasluice.errors.catalog import CatalogRateLimitError, ForbiddenError, UnauthenticatedError

_SECRET = "token=really-secret&signature=also-secret"


def test_secret_shapes_are_redacted_from_fake_public_boundaries() -> None:
    """Errors, events, receipts, metadata, and representations never expose secret-shaped input."""
    fixture_set = load_reference_fixture_set("ckan")
    fake = SyncReferenceConnector(fixture_set)
    denied = next(case for case in fixture_set.cases if case.outcome == "invalid-credentials")

    with pytest.raises(UnauthenticatedError) as error:
        fake.execute_case(denied)
    event = fake.record_event(
        "catalog.reference.test", {"authorization": _SECRET, "url": f"https://example.test/?{_SECRET}"}
    )
    public_values = [
        str(error.value),
        repr(event),
        json.dumps(fake.platform_metadata()),
        json.dumps(dict(event.metadata)),
    ]

    assert all(_SECRET not in value for value in public_values)
    assert event.metadata["authorization"] == "***"


def test_raw_diagnostics_require_opt_in_and_are_bounded() -> None:
    """Raw diagnostic bodies remain unavailable by default and bounded when enabled."""
    fixture_set = load_reference_fixture_set("udata")
    default = SyncReferenceConnector(fixture_set)
    enabled = SyncReferenceConnector(
        fixture_set, diagnostic_policy=DiagnosticPolicy(include_raw_body=True, raw_body_max_bytes=8)
    )

    assert default.diagnostic(_SECRET.encode()) is None
    assert enabled.diagnostic(_SECRET.encode()) == _SECRET.encode()[:8]


def test_retry_and_circuit_observations_remain_typed_and_safe() -> None:
    """Unsafe mutation retries are refused while rate-limit state remains inspectable."""
    fixture_set = load_reference_fixture_set("socrata")
    fake = SyncReferenceConnector(fixture_set)
    rate_limited = next(case for case in fixture_set.cases if case.outcome == "rate-limited")

    assert not fake.retry_decision(IdempotencyPolicy()).retry
    assert fake.retry_decision(IdempotencyPolicy(key="fixture-key")).retry
    with pytest.raises(CatalogRateLimitError):
        fake.execute_case(rate_limited)
    assert fake.circuit.failure_count == 1
    assert fake.circuit_key.credential_scope == "socrata"
    assert fake.tls_policy.verify
    assert not fake.telemetry_policy.enabled


def test_invalid_credentials_and_forbidden_permissions_do_not_dispatch() -> None:
    """Authentication failure and authorization failure remain distinct pre-dispatch states."""
    fixture_set = load_reference_fixture_set("ckan")
    invalid = next(case for case in fixture_set.cases if case.outcome == "invalid-credentials")
    forbidden = next(case for case in fixture_set.cases if case.outcome == "forbidden")
    fake = SyncReferenceConnector(fixture_set)

    with pytest.raises(UnauthenticatedError):
        fake.execute_case(invalid)
    with pytest.raises(ForbiddenError):
        fake.execute_case(forbidden)

    assert fake.dispatches == []
