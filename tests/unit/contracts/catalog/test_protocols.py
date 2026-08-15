"""Contract tests for normalized catalog client Protocols."""

from __future__ import annotations

import inspect
from typing import get_type_hints

from datasluice.contracts.catalog.protocols import (
    AsyncCatalogClient,
    AsyncCatalogOperationExecutor,
    CatalogConnectorContext,
    CatalogOperationGuard,
    CatalogOperationRequest,
    SyncCatalogClient,
    SyncCatalogOperationExecutor,
    SyncDatasetService,
)
from datasluice.domain.catalog.auth import CKANCredential
from datasluice.domain.catalog.operations import OperationId


def test_normalized_services_use_operation_requests_guards_and_typed_envelopes() -> None:
    """Portable services carry operation IDs and explicit dispatch guards."""
    request = CatalogOperationRequest(
        operation_id=OperationId(platform="catalog", service="datasets", method="get"), payload={"id": "dataset-1"}
    )
    guard = CatalogOperationGuard(operation_id=request.operation_id)

    assert request.operation_id == guard.operation_id
    assert {"datasets", "resources", "organizations"} <= set(SyncCatalogClient.__dict__)
    assert {"datasets", "resources", "organizations"} <= set(AsyncCatalogClient.__dict__)
    assert "request" in inspect.signature(SyncDatasetService.get).parameters
    assert "guard" in inspect.signature(SyncDatasetService.get).parameters
    assert "ResultEnvelope" in str(inspect.signature(SyncDatasetService.get).return_annotation)


def test_client_protocols_are_explicit_context_managers_with_matching_services() -> None:
    """Sync and async projections expose the same normalized service groups."""
    sync_services = {name for name in ("datasets", "resources", "organizations") if name in SyncCatalogClient.__dict__}
    async_services = {name for name in ("datasets", "resources", "organizations") if name in AsyncCatalogClient.__dict__}

    assert sync_services == async_services
    assert {"close", "__enter__", "__exit__"} <= set(SyncCatalogClient.__dict__)
    assert {"aclose", "__aenter__", "__aexit__"} <= set(AsyncCatalogClient.__dict__)


def test_connector_context_keeps_sync_and_async_executors_independent() -> None:
    """Connector factories receive independent executors, credentials, and ownership."""
    sync_executor = object()
    async_executor = object()
    context = CatalogConnectorContext(
        sync_executor=sync_executor,  # type: ignore[arg-type]
        async_executor=async_executor,  # type: ignore[arg-type]
        credentials=CKANCredential(api_token="secret"),
        manages_sync_executor=False,
        manages_async_executor=True,
    )

    assert context.sync_executor is sync_executor
    assert context.async_executor is async_executor
    assert context.credentials == CKANCredential(api_token="secret")
    assert not context.manages_sync_executor
    assert context.manages_async_executor
    assert SyncCatalogOperationExecutor.__name__ == "SyncCatalogOperationExecutor"
    assert AsyncCatalogOperationExecutor.__name__ == "AsyncCatalogOperationExecutor"


def test_public_protocols_have_no_transport_escape_hatch() -> None:
    """Public contract Protocols cannot expose raw HTTP helpers."""
    forbidden = {"request", "get_json", "download", "raw_response", "raw_request"}
    protocols = (SyncCatalogClient, AsyncCatalogClient, SyncCatalogOperationExecutor, AsyncCatalogOperationExecutor)

    assert all(forbidden.isdisjoint(set(get_type_hints(protocol).keys()) | set(protocol.__dict__)) for protocol in protocols)
