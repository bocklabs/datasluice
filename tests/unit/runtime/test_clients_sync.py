"""Synchronous catalog runtime client tests."""

from __future__ import annotations

import json
from datetime import date

import pytest

from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.contracts.catalog.protocols import (
    SyncCatalogClient as SyncCatalogClientProtocol,
)
from datasluice.domain.catalog.auth import EffectivePermissions
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.operations import (
    Atomicity,
    AuthClass,
    CapabilityClass,
    ConcurrencyRequirement,
    Idempotency,
    MutationClass,
    OperationId,
    OperationSpec,
    OperationTier,
)
from datasluice.domain.catalog.profiles import (
    CredentialClassification,
    DeclaredCapabilityProfile,
    ProbeEvidence,
    ProbeResponseClass,
    RoleClassification,
)
from datasluice.errors.catalog import CatalogNotFoundError, ForbiddenError, UnsupportedCapabilityError
from datasluice.runtime.clients import SyncCatalogClient
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse
from datasluice.runtime.transport.user_agent import build_user_agent


def _profile() -> DeclaredCapabilityProfile:
    operation = OperationId("reference", "datasets", "get")
    return DeclaredCapabilityProfile(
        profile_version="v1",
        schema_version="v1",
        platform_api_version="v1",
        official_source_uri="https://example.test/source",
        source_accessed_at=date(2026, 8, 20),
        fixture_fingerprint="fixture-v1",
        operations={
            operation: OperationSpec(
                id=operation,
                tier=OperationTier.NORMALIZED,
                request_type="DatasetRequest",
                response_type="DatasetRecord",
                auth_class=AuthClass.PUBLIC,
                mutation_class=MutationClass.READ,
                idempotency=Idempotency.SAFE,
                concurrency=ConcurrencyRequirement.NONE,
                atomicity=Atomicity.NONE,
                capability_class=CapabilityClass.CORE,
            )
        },
    )


class _Transport:
    def __init__(self, response: RuntimeResponse) -> None:
        self.response = response
        self.requests: list[RuntimeRequest] = []
        self.close_count = 0

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return self.response

    def close(self) -> None:
        self.close_count += 1


class _ProbeRunner:
    def __init__(self, response_class: ProbeResponseClass) -> None:
        self.response_class = response_class
        self.calls = 0

    def probe(self, operation_id: OperationId) -> ProbeEvidence:
        self.calls += 1
        return ProbeEvidence(
            operation_id=operation_id,
            deployment_url="https://catalog.example.test/api",
            credential_classification=CredentialClassification.ANONYMOUS,
            role_classification=RoleClassification.ANONYMOUS,
            observed_response_class=self.response_class,
        )


def _request(operation: OperationId | None = None) -> CatalogOperationRequest:
    return CatalogOperationRequest(
        operation or OperationId("reference", "datasets", "get"), {"url": "http://127.0.0.1:8000/datasets/fixture"}
    )


def _guard(operation: OperationId | None = None) -> CatalogOperationGuard:
    return CatalogOperationGuard(operation_id=operation or OperationId("reference", "datasets", "get"))


def _envelope() -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "kind": "result_envelope",
            "items": [
                {
                    "schema_version": 1,
                    "kind": "dataset",
                    "id": {
                        "schema_version": 1,
                        "kind": "catalog_id",
                        "platform": "reference",
                        "resource_kind": "dataset",
                        "value": "fixture",
                    },
                    "name": "Fixture dataset",
                    "description": None,
                    "extensions": {},
                }
            ],
            "page": None,
            "warnings": [],
            "platform": None,
        }
    ).encode()


def test_sync_client_dispatches_guarded_dataset_get_and_closes_once() -> None:
    transport = _Transport(RuntimeResponse(200, {}, _envelope()))
    client = SyncCatalogClient(transport, _profile())

    assert isinstance(client, SyncCatalogClientProtocol)
    result = client.datasets.get(_request(), _guard())

    assert result.items[0].name == "Fixture dataset"
    assert len(transport.requests) == 1
    assert transport.requests[0].headers["User-Agent"] == build_user_agent()
    client.close()
    client.close()
    assert transport.close_count == 1


def test_sync_client_rejects_unsupported_operation_without_dispatch() -> None:
    transport = _Transport(RuntimeResponse(200, {}, _envelope()))
    client = SyncCatalogClient(transport, _profile())
    operation = OperationId("reference", "resources", "get")

    with pytest.raises(UnsupportedCapabilityError) as raised:
        client.datasets.get(_request(operation), _guard(operation))

    assert raised.value.safe_action
    assert transport.requests == []


def test_sync_client_maps_not_found_response() -> None:
    client = SyncCatalogClient(_Transport(RuntimeResponse(404, {}, b"")), _profile())

    with pytest.raises(CatalogNotFoundError):
        client.datasets.get(_request(), _guard())


def test_sync_clients_own_independent_pools_and_capability_does_not_dispatch() -> None:
    first = _Transport(RuntimeResponse(200, {}, _envelope()))
    second = _Transport(RuntimeResponse(200, {}, _envelope()))
    client = SyncCatalogClient(first, _profile())

    assert first is not second
    assert client.capability(str(_request().operation_id)) == "available"
    assert first.requests == []


@pytest.mark.parametrize("service_method", ["get", "list"])
def test_sync_client_rejects_denied_caller_guard_before_probe_or_transport(service_method: str) -> None:
    operation = _request().operation_id
    permissions = EffectivePermissions(
        platform=CatalogPlatform("reference"),
        scopes=frozenset(),
        authenticated=True,
        operation_scopes={str(operation): frozenset({"datasets:read"})},
    )
    guard = CatalogOperationGuard(operation_id=operation, permissions=permissions)
    transport = _Transport(RuntimeResponse(200, {}, _envelope()))
    runner = _ProbeRunner(ProbeResponseClass.SUCCESS)
    client = SyncCatalogClient(transport, _profile(), probe_runner=runner)

    with pytest.raises(ForbiddenError):
        getattr(client.datasets, service_method)(_request(), guard)

    assert runner.calls == 0
    assert transport.requests == []


def test_sync_client_rejects_guard_for_different_operation_before_probe_or_transport() -> None:
    operation = _request().operation_id
    guard = CatalogOperationGuard(operation_id=OperationId("reference", "datasets", "list"))
    transport = _Transport(RuntimeResponse(200, {}, _envelope()))
    runner = _ProbeRunner(ProbeResponseClass.SUCCESS)
    client = SyncCatalogClient(transport, _profile(), probe_runner=runner)

    with pytest.raises(ValueError, match="does not match request"):
        client.datasets.get(_request(operation), guard)

    assert runner.calls == 0
    assert transport.requests == []


def test_sync_client_allowed_caller_guard_does_not_bypass_denied_effective_capability() -> None:
    operation = _request().operation_id
    guard = CatalogOperationGuard(operation_id=operation)
    transport = _Transport(RuntimeResponse(200, {}, _envelope()))
    runner = _ProbeRunner(ProbeResponseClass.UNAVAILABLE)
    client = SyncCatalogClient(transport, _profile(), probe_runner=runner)

    with pytest.raises(UnsupportedCapabilityError):
        client.datasets.get(_request(operation), guard)

    assert runner.calls == 1
    assert transport.requests == []
