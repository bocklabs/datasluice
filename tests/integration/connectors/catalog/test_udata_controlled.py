"""Controlled-environment tracer proof against the loopback uData 17.6 stack."""

from __future__ import annotations

import os

import pytest

from datasluice.connectors.catalog.udata.clients import (
    create_async_client,
    create_sync_client,
    declared_udata_profile,
)
from datasluice.connectors.catalog.udata.models.datasets import DatasetListQuery, DatasetSuggestQuery
from datasluice.connectors.catalog.udata.settings import UDataClientSettings
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest

ORIGIN = os.environ.get("UDATA_EVIDENCE_ORIGIN", "http://127.0.0.1:5640")

pytestmark = [
    pytest.mark.udata_controlled,
    pytest.mark.skipif(
        os.environ.get("UDATA_EVIDENCE_CONTROLLED") != "1",
        reason="controlled uData evidence runs only against the local digest-pinned stack",
    ),
]

_DATASET_OPERATION_ID = next(op_id for op_id in declared_udata_profile().operations if "dataset" in op_id.method)


def test_controlled_stack_proves_exact_version_then_one_dataset_read() -> None:
    settings = UDataClientSettings(base_url=ORIGIN)
    with create_sync_client(settings) as client:
        assert client.site_version().version == "17.6.0"
        envelope = client.datasets_list(
            CatalogOperationRequest(operation_id=_DATASET_OPERATION_ID, payload={"page": 1, "page_size": 5}),
            CatalogOperationGuard(operation_id=_DATASET_OPERATION_ID),
        )

    assert envelope.page is not None
    assert envelope.page.total_items is not None and envelope.page.total_items >= 0
    for record in envelope.items:
        assert record.id.value


def test_controlled_stack_proves_dataset_family_reads() -> None:
    settings = UDataClientSettings(base_url=ORIGIN)
    with create_sync_client(settings) as client:
        page = client.datasets.list(DatasetListQuery(page=1, page_size=3))
        suggestions = client.datasets.suggest(DatasetSuggestQuery(q="evidence", size=3))
        v2_page = client.datasets.list_v2(DatasetListQuery(page=1, page_size=3))

    assert page.page is not None and page.page.total_items is not None
    assert isinstance(suggestions, tuple)
    assert v2_page.page is not None


def test_controlled_stack_proves_authenticated_dataset_mutation_chain() -> None:
    token = os.environ.get("UDATA_EVIDENCE_ADMIN_TOKEN")
    if not token:
        pytest.skip("controlled mutations require UDATA_EVIDENCE_ADMIN_TOKEN from the seeded admin")
    from datasluice.connectors.catalog.udata.models.datasets import (
        DatasetCreateInput,
        DatasetDeleteOptions,
        DatasetUpdateInput,
    )
    from datasluice.domain.catalog.auth import UDataCredential

    settings = UDataClientSettings(base_url=ORIGIN, credential=UDataCredential(api_key=token))
    with create_sync_client(settings) as client:
        record = client.datasets.create(DatasetCreateInput(title="Evidence dataset", description="d"))
        dataset_id = record.id.value
        updated = client.datasets.update(dataset_id, DatasetUpdateInput(title="Evidence dataset v2"))
        outcome = client.datasets.delete(dataset_id, DatasetDeleteOptions())

    assert updated.payload["title"] == "Evidence dataset v2"
    assert outcome.status_code == 204 and outcome.outcome == "deleted"


def test_controlled_async_stack_proves_exact_version_then_one_dataset_read() -> None:
    import asyncio

    settings = UDataClientSettings(base_url=ORIGIN)

    async def run() -> tuple[str, int | None]:
        async with create_async_client(settings) as client:
            version = (await client.site_version()).version
            envelope = await client.datasets_list(
                CatalogOperationRequest(operation_id=_DATASET_OPERATION_ID, payload={}),
                CatalogOperationGuard(operation_id=_DATASET_OPERATION_ID),
            )
            return version, envelope.page.total_items if envelope.page else None

    version, total = asyncio.run(run())

    assert version == "17.6.0"
    assert total is not None and total >= 0
