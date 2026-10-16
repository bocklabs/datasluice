from __future__ import annotations

import asyncio
import json
from importlib.resources import files
from typing import TYPE_CHECKING

import pytest

from datasluice.contracts.catalog.fakes import AsyncReferenceConnector, SyncReferenceConnector
from datasluice.contracts.catalog.fixtures import load_reference_fixture_set
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, IdempotencyPolicy, MutationPolicy
from datasluice.errors.catalog import ForbiddenError, UnauthenticatedError, UnsupportedCapabilityError

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize("platform", ["ckan", "udata", "socrata"])
def test_reference_fixture_sets_are_strictly_profile_bound(platform: str, tmp_path: Path) -> None:
    fixture_set = load_reference_fixture_set(platform)

    assert fixture_set.platform == platform
    assert fixture_set.cases
    assert all(case.operation_id.platform == platform for case in fixture_set.cases)

    fixture_root = files("datasluice.contracts.catalog.fixtures").joinpath(platform)
    original = json.loads(fixture_root.joinpath("cases.json").read_text(encoding="utf-8"))
    original["cases"][0]["operation"] = f"{platform}/unknown.operation"
    tampered = tmp_path / "cases.json"
    tampered.write_text(json.dumps(original), encoding="utf-8")
    (tmp_path / "evidence.json").write_bytes(fixture_root.joinpath("evidence.json").read_bytes())
    with pytest.raises(ValueError, match="fingerprint"):
        load_reference_fixture_set(platform, cases_path=tampered)


@pytest.mark.parametrize("platform", ["ckan", "udata", "socrata"])
def test_sync_and_async_reference_fakes_return_lossless_native_envelopes(platform: str) -> None:
    fixture_set = load_reference_fixture_set(platform)
    sync = SyncReferenceConnector(fixture_set)
    async_client = AsyncReferenceConnector(fixture_set)

    for case in fixture_set.success_cases:
        sync_result = sync.execute_case(case)
        async_result = asyncio.run(async_client.execute_case(case))
        assert sync_result.to_dict() == async_result.to_dict()
        assert sync_result.items[0].payload["operation"] == str(case.operation_id)


@pytest.mark.parametrize(
    ("outcome", "error"),
    [
        ("missing-credentials", UnauthenticatedError),
        ("invalid-credentials", UnauthenticatedError),
        ("forbidden", ForbiddenError),
        ("deployment-disabled", UnsupportedCapabilityError),
        ("unavailable", UnsupportedCapabilityError),
    ],
)
def test_reference_fake_guards_reject_before_dispatch(outcome: str, error: type[Exception]) -> None:
    fixture_set = load_reference_fixture_set("ckan")
    fake = SyncReferenceConnector(fixture_set)
    case = next(case for case in fixture_set.cases if case.outcome == outcome)

    with pytest.raises(error):
        fake.execute_case(case)

    assert fake.dispatches == []


def test_mutations_require_safe_policy_and_return_deterministic_ordered_receipts() -> None:
    fixture_set = load_reference_fixture_set("udata")
    fake = SyncReferenceConnector(fixture_set)
    case = next(case for case in fixture_set.cases if case.outcome == "authenticated-success")
    policy = MutationPolicy(
        destructive=True,
        confirmation=ConfirmationPolicy(confirmed=True),
        concurrency=ConcurrencyPolicy(token="fixture-v1"),
        idempotency=IdempotencyPolicy(key="fixture-key"),
    )

    result, receipt = fake.execute_mutation(case, policy)
    checkpoint = fake.execute_bulk(case, policy, count=2)

    assert result.items[0].payload["operation"] == str(case.operation_id)
    assert receipt.request_id == "fixture-0001"
    assert [item.index for item in checkpoint.item_receipts] == [0, 1]
    assert checkpoint.resumption_cursor == "fixture-0002"


def test_unknown_operation_is_never_dispatched() -> None:
    fixture_set = load_reference_fixture_set("ckan")
    fake = SyncReferenceConnector(fixture_set)
    unknown = OperationId(platform="ckan", service="unknown", method="operation")

    with pytest.raises(ValueError, match="undeclared"):
        fake.execute_operation(unknown, "core")

    assert fake.dispatches == []
