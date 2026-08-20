"""Tests for policy-enforced runtime mutations."""

from __future__ import annotations

import pytest

from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.domain.catalog.safety import (
    ConcurrencyPolicy,
    ConfirmationPolicy,
    DryRunPolicy,
    IdempotencyPolicy,
    MutationPolicy,
)
from datasluice.errors.catalog import CatalogConflictError, CatalogUnavailableError, CatalogValidationError
from datasluice.runtime.mutation import MutationEnforcer
from datasluice.runtime.transport.base import RuntimeResponse


def _target() -> CatalogId:
    return CatalogId(CatalogPlatform.CKAN, ResourceKind.DATASET, "weather")


def _policy(
    *,
    dry_run: DryRunPolicy | None = None,
    idempotency: IdempotencyPolicy | None = None,
) -> MutationPolicy:
    return MutationPolicy(
        destructive=True,
        confirmation=ConfirmationPolicy(confirmed=True),
        concurrency=ConcurrencyPolicy(token="v1"),
        dry_run=dry_run or DryRunPolicy(),
        idempotency=idempotency or IdempotencyPolicy(),
    )


def test_mutations_require_confirmed_policy_before_dispatch() -> None:
    sent = []
    enforcer = MutationEnforcer(lambda request: sent.append(request) or RuntimeResponse(200, {}, b""))

    with pytest.raises(CatalogValidationError) as raised:
        enforcer.execute(OperationId("ckan", "datasets", "update"), _target(), None)

    assert raised.value.operation == "ckan/datasets.update"
    assert raised.value.platform == "ckan"
    assert raised.value.safe_action
    receipt = enforcer.execute(OperationId("ckan", "datasets", "update"), _target(), _policy())
    assert receipt.outcome == "succeeded"
    assert sent[0].concurrency_token == "v1"


def test_dry_run_short_circuits_without_transport_send() -> None:
    enforcer = MutationEnforcer(lambda request: pytest.fail("dry run must not dispatch"))

    receipt = enforcer.execute(
        OperationId("ckan", "datasets", "update"),
        _target(),
        _policy(dry_run=DryRunPolicy(requested=True)),
    )

    assert receipt.outcome == "skipped"
    assert receipt.audit_metadata["dry_run"] is True


def test_unsafe_idempotency_does_not_retry_retryable_response() -> None:
    responses = [RuntimeResponse(503, {}, b"")]
    enforcer = MutationEnforcer(
        lambda request: responses.pop(0),
        budget=TimeBudget(connect=1, read=1, write=1, total=10),
        sleep=lambda _: None,
    )

    with pytest.raises(CatalogUnavailableError):
        enforcer.execute(
            OperationId("ckan", "datasets", "update"),
            _target(),
            _policy(idempotency=IdempotencyPolicy()),
        )

    assert responses == []
    assert enforcer.last_receipt is not None
    assert enforcer.last_receipt.outcome == "failed"


def test_conflict_response_maps_to_catalog_conflict() -> None:
    enforcer = MutationEnforcer(lambda request: RuntimeResponse(409, {}, b""))

    with pytest.raises(CatalogConflictError):
        enforcer.execute(OperationId("ckan", "datasets", "update"), _target(), _policy())


def test_receipts_redact_credential_shaped_audit_values() -> None:
    enforcer = MutationEnforcer(lambda request: RuntimeResponse(200, {}, b""))
    receipt = enforcer.execute(
        OperationId("ckan", "datasets", "update"),
        _target(),
        _policy(),
        audit_metadata={"details": {"response": {"value": "Bearer aBcDeFgH1234"}}},
    )

    assert "aBcDeFgH1234" not in repr(receipt.to_dict())
    assert "Bearer ***" in repr(receipt.to_dict())
