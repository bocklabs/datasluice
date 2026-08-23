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
from datasluice.exceptions import DataSluiceError
from datasluice.runtime.mutation import MutationDispatchRequest, MutationEnforcer, build_mutation_receipt
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
    assert raised.value.safe_action == "Provide a confirmed mutation policy with a version token or explicit overwrite."
    receipt = enforcer.execute(OperationId("ckan", "datasets", "update"), _target(), _policy())
    assert receipt.outcome == "succeeded"
    assert sent[0].concurrency_token == "v1"


def test_destructive_mutations_require_explicit_confirmation() -> None:
    sent: list[object] = []
    enforcer = MutationEnforcer(lambda request: sent.append(request) or RuntimeResponse(200, {}, b""))
    unconfirmed = MutationPolicy(
        destructive=True,
        confirmation=ConfirmationPolicy(confirmed=False),
        concurrency=ConcurrencyPolicy(token="v1"),
    )

    with pytest.raises(CatalogValidationError) as raised:
        enforcer.execute(OperationId("ckan", "datasets", "update"), _target(), unconfirmed)

    assert sent == []
    assert raised.value.safe_action == "Provide a confirmed mutation policy with a version token or explicit overwrite."


def test_confirmed_mutations_require_a_concurrency_instruction() -> None:
    sent: list[object] = []
    enforcer = MutationEnforcer(lambda request: sent.append(request) or RuntimeResponse(200, {}, b""))
    confirmed = MutationPolicy(
        destructive=False,
        confirmation=ConfirmationPolicy(confirmed=True),
        concurrency=None,
    )

    with pytest.raises(CatalogValidationError) as raised:
        enforcer.execute(OperationId("ckan", "datasets", "update"), _target(), confirmed)

    assert sent == []
    assert raised.value.safe_action == "Provide a confirmed mutation policy with a version token or explicit overwrite."


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
            _policy(idempotency=IdempotencyPolicy(safe=False)),
        )

    assert responses == []
    assert enforcer.last_receipt is not None
    assert enforcer.last_receipt.outcome == "failed"


def test_safe_idempotency_retries_a_retryable_response_then_succeeds() -> None:
    responses = [RuntimeResponse(503, {}, b""), RuntimeResponse(200, {}, b"ok")]
    enforcer = MutationEnforcer(
        lambda request: responses.pop(0),
        budget=TimeBudget(connect=1, read=1, write=1, total=10),
        sleep=lambda _: None,
    )

    receipt = enforcer.execute(
        OperationId("ckan", "datasets", "update"),
        _target(),
        _policy(idempotency=IdempotencyPolicy(safe=True)),
    )

    assert responses == []
    assert receipt.outcome == "succeeded"
    assert enforcer.last_receipt is receipt


def test_conflict_response_maps_to_catalog_conflict() -> None:
    enforcer = MutationEnforcer(lambda request: RuntimeResponse(409, {}, b""))

    with pytest.raises(CatalogConflictError):
        enforcer.execute(OperationId("ckan", "datasets", "update"), _target(), _policy())


def test_receipts_redact_credential_shaped_audit_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATASLUICE_NO_REDACT", raising=False)
    enforcer = MutationEnforcer(lambda request: RuntimeResponse(200, {}, b""))
    receipt = enforcer.execute(
        OperationId("ckan", "datasets", "update"),
        _target(),
        _policy(),
        audit_metadata={"details": {"response": {"value": "Bearer aBcDeFgH1234"}}},
    )

    assert receipt.audit_metadata == {"details": {"response": {"value": "Bearer ***"}}}


def test_failed_dispatch_receipt_construction_does_not_mask_the_original_error() -> None:
    def explode(request: MutationDispatchRequest) -> RuntimeResponse:
        raise RuntimeError("dispatch exploded")

    enforcer = MutationEnforcer(explode, sleep=lambda _: None)

    with pytest.raises(RuntimeError, match="dispatch exploded"):
        enforcer.execute(
            OperationId("ckan", "datasets", "update"),
            _target(),
            _policy(),
            audit_metadata={"auth_header": "Bearer aBcDeFgH1234"},
        )


def test_succeeded_receipt_construction_failure_surfaces_after_the_dispatch() -> None:
    sent: list[object] = []
    enforcer = MutationEnforcer(lambda request: sent.append(request) or RuntimeResponse(200, {}, b""))

    with pytest.raises(DataSluiceError):
        enforcer.execute(
            OperationId("ckan", "datasets", "update"),
            _target(),
            _policy(),
            audit_metadata={"auth_header": "Bearer aBcDeFgH1234"},
        )

    assert len(sent) == 1


def test_build_mutation_receipt_matches_enforcer_receipt() -> None:
    enforcer = MutationEnforcer(lambda request: RuntimeResponse(200, {}, b""))
    operation = OperationId("ckan", "datasets", "update")
    policy = _policy()
    receipt = enforcer.execute(operation, _target(), policy, audit_metadata={"dataset": "weather", "attempt": 1})
    equivalent = build_mutation_receipt(
        operation,
        _target(),
        policy,
        "succeeded",
        {"dataset": "weather", "attempt": 1},
    )

    assert receipt == equivalent
