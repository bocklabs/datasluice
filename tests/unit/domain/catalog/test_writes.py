"""Tests for catalog create, patch, and mutation receipt values."""

from __future__ import annotations

import dataclasses
from types import MappingProxyType

import pytest

from datasluice.domain.catalog import (
    UNSET,
    BulkCheckpoint,
    BulkItemReceipt,
    BulkPlan,
    CatalogId,
    CatalogPlatform,
    CreateRequest,
    MutationReceipt,
    PatchRequest,
    ResourceKind,
)
from datasluice.exceptions import DataSluiceError


@pytest.mark.parametrize(
    ("value", "expected"),
    [(UNSET, {}), (None, {"title": None}), ("Updated", {"title": "Updated"})],
)
def test_patch_request_preserves_each_tri_state(value: object, expected: dict[str, object]) -> None:
    request = PatchRequest(fields={"title": value})

    assert request.to_wire() == expected


def test_create_rejects_unset_and_patch_retains_tri_state_for_round_trips() -> None:
    with pytest.raises(DataSluiceError):
        CreateRequest(fields={"title": UNSET})

    request = PatchRequest(fields={"title": UNSET, "description": None, "tags": ["open-data"]})
    assert request.to_wire() == {"description": None, "tags": ["open-data"]}
    assert PatchRequest.from_dict(request.to_dict()) == request


def test_mutation_receipt_preserves_safe_tokens_and_rejects_credential_metadata() -> None:
    target = CatalogId(CatalogPlatform.CKAN, ResourceKind.DATASET, "weather")
    receipt = MutationReceipt(
        operation="datasets.update",
        outcome="succeeded",
        target=target,
        version_token='W/"revision-3"',
        request_id="req-123",
        atomicity="independent",
        audit_metadata={"status_code": 200, "attempt": 1},
    )

    assert isinstance(receipt.audit_metadata, MappingProxyType)
    assert MutationReceipt.from_dict(receipt.to_dict()) == receipt
    with pytest.raises(dataclasses.FrozenInstanceError):
        receipt.outcome = "failed"  # ty: ignore[invalid-assignment]: frozen dataclass assertion
    with pytest.raises(DataSluiceError):
        MutationReceipt(
            operation="datasets.update",
            outcome="succeeded",
            target=target,
            audit_metadata={"authorization": "Bearer secret"},
        )
    with pytest.raises(DataSluiceError):
        MutationReceipt(
            operation="datasets.update",
            outcome="succeeded",
            target=target,
            atomicity="atomic",
            operation_atomicity="independent",
        )


def test_bulk_values_preserve_order_outcomes_cancellation_and_resumption() -> None:
    first = CatalogId(CatalogPlatform.CKAN, ResourceKind.DATASET, "first")
    second = CatalogId(CatalogPlatform.CKAN, ResourceKind.DATASET, "second")
    first_receipt = MutationReceipt(operation="datasets.delete", outcome="succeeded", target=first)
    second_receipt = MutationReceipt(operation="datasets.delete", outcome="cancelled", target=second)
    plan = BulkPlan(
        operation="datasets.delete",
        items=(first, second),
        preview=True,
        atomicity="independent",
        cancellation_requested=True,
        resumption_cursor="dataset:second",
    )
    checkpoint = BulkCheckpoint(
        plan=plan,
        item_receipts=(
            BulkItemReceipt(index=0, receipt=first_receipt),
            BulkItemReceipt(index=1, receipt=second_receipt),
        ),
        cancellation_requested=True,
        resumption_cursor="dataset:second",
    )

    assert [item.value for item in plan.items] == ["first", "second"]
    assert BulkPlan.from_dict(plan.to_dict()) == plan
    assert BulkCheckpoint.from_dict(checkpoint.to_dict()) == checkpoint
