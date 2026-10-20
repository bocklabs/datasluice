from __future__ import annotations

import asyncio

import pytest

from datasluice.connectors.catalog.udata.models.extensions import ExtensionEvidence
from datasluice.connectors.catalog.udata.models.harvest import HARVEST_OPERATION, HarvestBulkTargets
from datasluice.connectors.catalog.udata.services.datasets import _rdf_redirect_receipt
from datasluice.connectors.catalog.udata.services.extensions import AsyncExtensionsService, ExtensionEvidenceCache
from datasluice.connectors.catalog.udata.wire import extensions as wire
from datasluice.domain.catalog.auth import credential_scope
from datasluice.domain.catalog.ids import ResourceKind
from datasluice.domain.catalog.receipts import BulkCheckpoint, MutationReceipt
from datasluice.domain.catalog.safety import BulkExecutionPolicy
from datasluice.errors.catalog import CatalogValidationError
from tests.helpers.udata_test_support import (
    UDATA_ADMIN_PERMISSIONS,
    UDATA_CREDENTIAL,
    UDATA_ORIGIN,
    async_client,
    async_route_table,
    declared_routes,
    mutation_policy,
)


def test_async_extension_cache_methods_remain_awaitable_without_dispatch() -> None:
    async def run() -> None:
        router = async_route_table({})
        cache = ExtensionEvidenceCache(ttl_seconds=60)
        operation = wire.REASON_CATEGORIES_OPERATION
        evidence = ExtensionEvidence(operation, "available", "success", credential_scope(UDATA_CREDENTIAL))
        cache.record(evidence)
        async with async_client(router) as client:
            service = AsyncExtensionsService(client, cache)
            assert await service.evidence(operation) is evidence
            assert await service.proven_operations() == (operation,)
            with pytest.raises(CatalogValidationError, match="not a documented stock extension operation"):
                await service.evidence("unknown-operation")
            await service.refresh()
            assert await service.evidence(operation) is None
            assert await service.proven_operations() == ()
        assert router.requests == []

    asyncio.run(run())


@pytest.mark.parametrize(("subject", "collection"), [("dataset", "datasets"), ("dataservice", "dataservices")])
def test_rdf_redirect_replacement_preserves_mutation_receipt_type(subject: str, collection: str) -> None:
    receipt = _rdf_redirect_receipt(
        "udata/api-v1.rdf",
        "record-1",
        {"Location": f"{UDATA_ORIGIN}/api/1/{collection}/record-1/rdf.ttl"},
        302,
        UDATA_ORIGIN,
        rf"/api/1/{collection}/([^/]+)/rdf(?:\.([A-Za-z0-9_-]+))?",
        subject,
    )

    assert type(receipt) is MutationReceipt
    assert receipt.target.resource_kind == ResourceKind(subject)
    assert receipt.target.value == "record-1"
    assert receipt.outcome == "skipped"
    assert receipt.audit_metadata["status_code"] == 302


def test_async_harvest_bulk_consumes_receipts_and_summary_from_async_stream() -> None:
    async def run() -> None:
        router = async_route_table(
            declared_routes(
                ("DELETE", "/api/1/harvest/source/source-1/", 204, None),
                ("DELETE", "/api/1/harvest/source/source-2/", 404, {"message": "not found"}),
            )
        )
        checkpoints: list[BulkCheckpoint] = []
        async with async_client(router) as client:
            service = client.harvest_moderation_admin
            targets = HarvestBulkTargets(("source-1", "source-2"))
            preview = service.preview_bulk("delete-source", targets)
            result = await service.run_bulk(
                "delete-source",
                targets,
                UDATA_ADMIN_PERMISSIONS,
                mutation_policy(HARVEST_OPERATION, preview.confirmation_target, destructive=True),
                execution_policy=BulkExecutionPolicy(max_parallelism=1),
                checkpoint_sink=checkpoints.append,
            )

        assert [item.receipt.target.value for item in result.receipts] == ["source-1", "source-2"]
        assert [item.receipt.outcome for item in result.receipts] == ["succeeded", "failed"]
        assert result.summary.total == 2
        assert result.summary.settled == 2
        assert result.summary.outstanding == 0
        assert result.summary.state == "completed"
        assert len(checkpoints) == 2
        assert result.checkpoints[-1]["completed_indexes"] == [0, 1]

    asyncio.run(run())
