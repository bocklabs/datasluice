from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError
from operator import methodcaller, setitem
from typing import TYPE_CHECKING, cast
from unittest.mock import Mock

import pytest

from datasluice.connectors.catalog.udata import drift
from datasluice.connectors.catalog.udata.models.dataservices import (
    DataserviceCreateInput,
    DataserviceDatasetLinkInput,
    DataserviceUpdateInput,
)
from datasluice.connectors.catalog.udata.models.topics import (
    TopicCreateInput,
    TopicElementInput,
    TopicElementLink,
    TopicElementsCreateInput,
    TopicUpdateInput,
)
from datasluice.connectors.catalog.udata.models.transfers import TransferRequestInput, TransferResponseInput
from datasluice.connectors.catalog.udata.services.dataservices import _dataset_target
from datasluice.connectors.catalog.udata.services.topics import _element_target
from datasluice.connectors.catalog.udata.wire import reuses, spatial, transfers
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.errors.catalog import CatalogValidationError, ForbiddenError
from tests.helpers.udata_test_support import (
    UDATA_CREDENTIAL,
    UDATA_ORIGIN,
    UDATA_PERMISSIONS,
    async_client,
    async_route_table,
    mutation_policy,
    sync_client,
    sync_route_table,
    with_site_route,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


def _receipt_from(error: BaseException) -> MutationReceipt:
    receipt = getattr(error, "mutation_receipt", None)
    assert isinstance(receipt, MutationReceipt)
    return receipt


@pytest.mark.parametrize("target", [_dataset_target, _element_target])
def test_composite_targets_encode_delimiters_and_escape_characters(target: Callable[[str, str], str]) -> None:
    assert target("a:b", "c") == "a%3Ab:c"
    assert target("a", "b:c") == "a:b%3Ac"
    assert target("a%3Ab", "c") == "a%253Ab:c"
    assert target("a:b", "c") != target("a", "b:c")
    assert target("a:b", "c") != target("a%3Ab", "c")


@pytest.mark.parametrize("builder", [reuses.reuse_add_dataset_request, reuses.reuse_add_dataservice_request])
@pytest.mark.parametrize("identifier", ["", ".", "..", "a/b", "a?b", "a#b", "a\x00b"])
def test_reuse_wire_validates_linked_identifiers(builder: Callable[[str, str], object], identifier: str) -> None:
    with pytest.raises(CatalogValidationError):
        builder("reuse-1", identifier)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("kind", ["dataset", "dataservice"])
def test_reuse_relationship_confirmation_cannot_move_a_colon_between_identifiers(asynchronous: bool, kind: str) -> None:
    operation = getattr(reuses, f"REUSE_ADD_{kind.upper()}_OPERATION")
    policy = mutation_policy(operation, "a%3Ab:c")
    routes = with_site_route({("POST", f"{UDATA_ORIGIN}/api/1/reuses/a%3Ab/{kind}s/"): (200, {})})

    async def run() -> None:
        router = async_route_table(routes)
        async with async_client(router, UDATA_CREDENTIAL) as client:
            method = getattr(client.reuses, f"reuse_add_{kind}")
            with pytest.raises(ForbiddenError):
                await method("a", "b:c", UDATA_PERMISSIONS, policy)
            assert router.requests == []
            result = await method("a:b", "c", UDATA_PERMISSIONS, policy)
            assert result.receipt.target.value == "a%3Ab:c"

    if asynchronous:
        asyncio.run(run())
    else:
        router = sync_route_table(routes)
        with sync_client(router, UDATA_CREDENTIAL) as client:
            method = getattr(client.reuses, f"reuse_add_{kind}")
            with pytest.raises(ForbiddenError):
                method("a", "b:c", UDATA_PERMISSIONS, policy)
            assert router.requests == []
            result = method("a:b", "c", UDATA_PERMISSIONS, policy)
            assert result.receipt.target.value == "a%3Ab:c"


@pytest.mark.parametrize("count", [100, 101])
def test_zone_lists_stop_after_the_count_limit(count: int) -> None:
    consumed = 0

    def ids() -> Iterator[str]:
        nonlocal consumed
        for _ in range(count):
            consumed += 1
            yield "zone-1"
        if count > 100:
            pytest.fail("The zone builder consumed beyond the count limit.")

    if count == 100:
        assert spatial.spatial_zones_request(ids())[1].count("zone-1") == 100
    else:
        identifiers = ids()
        with pytest.raises(CatalogValidationError, match="100"):
            spatial.spatial_zones_request(identifiers)
    assert consumed == count


@pytest.mark.parametrize("count", [100, 101])
def test_dataservice_link_counts_cover_relationship_create_and_update(count: int) -> None:
    identifiers = ("dataset-1",) * count
    builders = (
        lambda: DataserviceDatasetLinkInput(identifiers),
        lambda: DataserviceCreateInput(title="API", base_api_url="https://example.test", datasets=identifiers),
        lambda: DataserviceUpdateInput(datasets=identifiers),
    )
    for build in builders:
        if count == 100:
            build()
        else:
            with pytest.raises(ValueError, match="100"):
                build()


@pytest.mark.parametrize("count", [100, 101])
def test_topic_element_counts_cover_relationship_create_and_update(count: int) -> None:
    elements = (TopicElementInput(title="Element"),) * count
    builders = (
        lambda: TopicElementsCreateInput((TopicElementLink("Dataset", "dataset-1"),) * count),
        lambda: TopicCreateInput(name="Topic", elements=elements),
        lambda: TopicUpdateInput(elements=elements),
    )
    for build in builders:
        if count == 100:
            build()
        else:
            with pytest.raises(ValueError, match="100"):
                build()


def test_transfer_target_binds_encoded_identifiers_and_reference_classes() -> None:
    first = TransferRequestInput("a:b", "Dataset", "c", "Organization")
    second = TransferRequestInput("a", "Dataset", "b:c", "Organization")
    changed_subject_class = TransferRequestInput("a:b", "Reuse", "c", "Organization")
    changed_recipient_class = TransferRequestInput("a:b", "Dataset", "c", "User")
    assert first.mutation_target() == "Dataset:a%3Ab:Organization:c"
    subjects = (first, second, changed_subject_class, changed_recipient_class)
    assert len({item.mutation_target() for item in subjects}) == 4


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    ("family", "method_name", "operation", "path"),
    [
        (
            "dataservices",
            "dataservice_dataset_remove",
            "udata/api-v1.dataservice-dataset-delete",
            "/api/1/dataservices/a%3Ab/datasets/c/",
        ),
        (
            "topics",
            "topic_element_delete",
            "udata/api-v2.topic-element-delete",
            "/api/2/topics/a%3Ab/elements/c/",
        ),
    ],
)
def test_relationship_removals_bind_each_encoded_identifier_before_dispatch(
    asynchronous: bool, family: str, method_name: str, operation: str, path: str
) -> None:
    policy = mutation_policy(operation, "a%3Ab:c", destructive=True)
    routes = with_site_route({("DELETE", f"{UDATA_ORIGIN}{path}"): (204, None)})

    async def run() -> None:
        router = async_route_table(routes)
        async with async_client(router, UDATA_CREDENTIAL) as client:
            method = getattr(getattr(client, family), method_name)
            with pytest.raises(ForbiddenError) as raised:
                await method("a", "b:c", UDATA_PERMISSIONS, policy)
            assert _receipt_from(raised.value).target.value == "a:b%3Ac"
            assert router.requests == []
            result = await method("a:b", "c", UDATA_PERMISSIONS, policy)
            assert result.receipt.target.value == "a%3Ab:c"

    if asynchronous:
        asyncio.run(run())
    else:
        router = sync_route_table(routes)
        with sync_client(router, UDATA_CREDENTIAL) as client:
            method = getattr(getattr(client, family), method_name)
            with pytest.raises(ForbiddenError) as raised:
                method("a", "b:c", UDATA_PERMISSIONS, policy)
            assert _receipt_from(raised.value).target.value == "a:b%3Ac"
            assert router.requests == []
            assert method("a:b", "c", UDATA_PERMISSIONS, policy).receipt.target.value == "a%3Ab:c"


@pytest.mark.parametrize("asynchronous", [False, True])
def test_transfer_confirmation_cannot_change_either_reference_class(asynchronous: bool) -> None:
    approved = TransferRequestInput("dataset-1", "Dataset", "owner-1", "Organization")
    policy = mutation_policy(transfers.REQUEST_TRANSFER_OPERATION, approved.mutation_target())
    changed = (
        TransferRequestInput("dataset-1", "Reuse", "owner-1", "Organization"),
        TransferRequestInput("dataset-1", "Dataset", "owner-1", "User"),
    )

    async def run() -> None:
        router = async_route_table(with_site_route({}))
        async with async_client(router, UDATA_CREDENTIAL) as client:
            for client_input in changed:
                with pytest.raises(ForbiddenError) as raised:
                    await client.transfers.request_transfer(client_input, UDATA_PERMISSIONS, policy)
                assert _receipt_from(raised.value).target.value == client_input.mutation_target()
        assert router.requests == []

    if asynchronous:
        asyncio.run(run())
    else:
        router = sync_route_table(with_site_route({}))
        with sync_client(router, UDATA_CREDENTIAL) as client:
            for client_input in changed:
                with pytest.raises(ForbiddenError) as raised:
                    client.transfers.request_transfer(client_input, UDATA_PERMISSIONS, policy)
                assert _receipt_from(raised.value).target.value == client_input.mutation_target()
        assert router.requests == []


@pytest.mark.parametrize("asynchronous", [False, True])
def test_individual_transfer_receipts_are_immutable_and_target_specific(asynchronous: bool) -> None:
    operation = transfers.RESPOND_TO_TRANSFER_OPERATION
    policy = mutation_policy(operation, "transfer-1")
    routes = with_site_route({("POST", f"{UDATA_ORIGIN}/api/1/transfer/transfer-1/"): (200, {})})

    def check(receipt: MutationReceipt) -> None:
        assert receipt.operation == operation
        assert receipt.target.value == "transfer-1"
        set_outcome = methodcaller("__setattr__", "outcome", "failed")
        with pytest.raises(FrozenInstanceError):
            set_outcome(receipt)
        audit_metadata = cast("dict[str, object]", receipt.audit_metadata)
        with pytest.raises(TypeError):
            setitem(audit_metadata, "mutation", "changed")
        copied = receipt.to_dict()
        cast("dict[str, object]", copied["audit_metadata"])["mutation"] = "changed"
        assert receipt.audit_metadata["mutation"] == "refused"

    async def run() -> None:
        router = async_route_table(routes)
        async with async_client(router, UDATA_CREDENTIAL) as client:
            response_input = TransferResponseInput("refuse")
            with pytest.raises(ForbiddenError) as raised:
                await client.transfers.respond_to_transfer("transfer-2", response_input, UDATA_PERMISSIONS, policy)
            assert _receipt_from(raised.value).target.value == "transfer-2"
            assert router.requests == []
            result = await client.transfers.respond_to_transfer(
                "transfer-1", TransferResponseInput("refuse"), UDATA_PERMISSIONS, policy
            )
            check(result.receipt)

    if asynchronous:
        asyncio.run(run())
    else:
        router = sync_route_table(routes)
        with sync_client(router, UDATA_CREDENTIAL) as client:
            response_input = TransferResponseInput("refuse")
            with pytest.raises(ForbiddenError) as raised:
                client.transfers.respond_to_transfer("transfer-2", response_input, UDATA_PERMISSIONS, policy)
            assert _receipt_from(raised.value).target.value == "transfer-2"
            assert router.requests == []
            check(
                client.transfers.respond_to_transfer(
                    "transfer-1", TransferResponseInput("refuse"), UDATA_PERMISSIONS, policy
                ).receipt
            )


@pytest.mark.parametrize("asynchronous", [False, True])
def test_caller_targets_cannot_override_an_empty_drift_allowlist(asynchronous: bool) -> None:
    target = drift.DriftTarget("unapproved", "https://example.test", (drift.SITE_OPERATION_ID,))
    transport = Mock()
    transport.send.side_effect = AssertionError("An unapproved target must not dispatch.")
    if asynchronous:
        coroutine = drift.run_udata_drift_async([target], transport=transport)
        with pytest.raises(drift.UDataDriftUnavailableError):
            asyncio.run(coroutine)
    else:
        with pytest.raises(drift.UDataDriftUnavailableError):
            drift.run_udata_drift([target], transport=transport)
    transport.send.assert_not_called()


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("change", ["alias", "origin", "reads", "duplicate"])
def test_drift_selection_requires_exact_approved_targets(
    monkeypatch: pytest.MonkeyPatch, asynchronous: bool, change: str
) -> None:
    approved = drift.DriftTarget(
        "approved", "https://approved.test", (drift.SITE_OPERATION_ID, "udata/api-v2.list-topics")
    )
    monkeypatch.setattr(drift, "APPROVED_TARGETS", (approved,))
    selected = {
        "alias": [drift.DriftTarget("different", approved.origin, approved.reads)],
        "origin": [drift.DriftTarget(approved.alias, "https://other.test", approved.reads)],
        "reads": [drift.DriftTarget(approved.alias, approved.origin, (drift.SITE_OPERATION_ID,))],
        "duplicate": [approved, approved],
    }[change]
    transport = Mock()
    if asynchronous:
        coroutine = drift.run_udata_drift_async(selected, transport=transport)
        with pytest.raises(CatalogValidationError):
            asyncio.run(coroutine)
    else:
        with pytest.raises(CatalogValidationError):
            drift.run_udata_drift(selected, transport=transport)
    transport.send.assert_not_called()
