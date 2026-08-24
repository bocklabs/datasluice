"""Synchronous catalog runtime client tests."""

from __future__ import annotations

import hashlib
import json

import pytest

from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.contracts.catalog.protocols import (
    SyncCatalogClient as SyncCatalogClientProtocol,
)
from datasluice.domain.catalog.auth import (
    CKANCredential,
    CredentialResolver,
    EffectivePermissions,
    SocrataCredential,
    UDataCredential,
)
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.models import OrganizationRecord, ResourceRecord
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.profiles import (
    CredentialClassification,
    ProbeEvidence,
    ProbeResponseClass,
    RoleClassification,
)
from datasluice.errors.catalog import (
    CatalogNotFoundError,
    ForbiddenError,
    NativeCatalogError,
    UnsupportedCapabilityError,
)
from datasluice.runtime.clients import SyncCatalogClient, _credential_scope
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse, TransportFailure
from datasluice.runtime.transport.user_agent import build_user_agent
from tests.unit.runtime._fixtures import _envelope, _guard, _profile, _request


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


class _FailingTransport:
    def __init__(self) -> None:
        self.requests: list[RuntimeRequest] = []
        self.close_count = 0

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        raise TransportFailure("connection refused")

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


def _resource_envelope() -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "kind": "result_envelope",
            "items": [
                {
                    "schema_version": 1,
                    "kind": "resource",
                    "id": {
                        "schema_version": 1,
                        "kind": "catalog_id",
                        "platform": "reference",
                        "resource_kind": "resource",
                        "value": "fixture-resource",
                    },
                    "dataset_id": {
                        "schema_version": 1,
                        "kind": "catalog_id",
                        "platform": "reference",
                        "resource_kind": "dataset",
                        "value": "fixture",
                    },
                    "name": "Fixture resource",
                    "url": None,
                    "extensions": {},
                }
            ],
            "page": None,
            "warnings": [],
            "platform": None,
        }
    ).encode()


def _organization_envelope() -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "kind": "result_envelope",
            "items": [
                {
                    "schema_version": 1,
                    "kind": "organization",
                    "id": {
                        "schema_version": 1,
                        "kind": "catalog_id",
                        "platform": "reference",
                        "resource_kind": "organization",
                        "value": "fixture-org",
                    },
                    "name": "Fixture organization",
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


def test_sync_client_rejects_malformed_json_body() -> None:
    client = SyncCatalogClient(_Transport(RuntimeResponse(200, {}, b"<html>not-json</html>")), _profile())

    with pytest.raises(NativeCatalogError, match="invalid JSON"):
        client.datasets.get(_request(), _guard())


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


def test_sync_capability_reads_cached_state_without_probe_or_transport_dispatch() -> None:
    transport = _Transport(RuntimeResponse(200, {}, _envelope()))
    runner = _ProbeRunner(ProbeResponseClass.SUCCESS)
    client = SyncCatalogClient(transport, _profile(), probe_runner=runner)

    assert client.capability(str(_request().operation_id)) == "available"

    assert runner.calls == 0
    assert transport.requests == []


def test_clients_keep_independent_transports() -> None:
    first_transport = _Transport(RuntimeResponse(200, {}, _envelope()))
    second_transport = _Transport(RuntimeResponse(200, {}, _envelope()))
    first = SyncCatalogClient(first_transport, _profile())
    second = SyncCatalogClient(second_transport, _profile())

    first.datasets.get(_request(), _guard())

    assert len(first_transport.requests) == 1
    assert second_transport.requests == []

    second.datasets.get(_request(), _guard())

    assert len(second_transport.requests) == 1
    assert first.transport is first_transport
    assert second.transport is second_transport
    assert first is not second


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


def test_service_projections_decode_their_own_record_kinds() -> None:
    resources = SyncCatalogClient(_Transport(RuntimeResponse(200, {}, _resource_envelope())), _profile())
    organizations = SyncCatalogClient(_Transport(RuntimeResponse(200, {}, _organization_envelope())), _profile())

    resource_result = resources.resources.get(_request(), _guard())
    organization_result = organizations.organizations.list(_request(), _guard())

    assert isinstance(resource_result.items[0], ResourceRecord)
    assert resource_result.items[0].name == "Fixture resource"
    assert isinstance(organization_result.items[0], OrganizationRecord)
    assert organization_result.items[0].name == "Fixture organization"


@pytest.mark.parametrize(
    ("credential", "header", "value"),
    [
        (CKANCredential(api_token="ckan-token"), "Authorization", "ckan-token"),
        (UDataCredential(api_key="udata-key"), "X-API-KEY", "udata-key"),
        (SocrataCredential(app_token="socrata-app"), "X-App-Token", "socrata-app"),
    ],
)
def test_platform_credentials_map_to_their_auth_headers(credential: object, header: str, value: str) -> None:
    transport = _Transport(RuntimeResponse(200, {}, _envelope()))
    client = SyncCatalogClient(transport, _profile(), credentials=credential)

    client.datasets.get(_request(), _guard())

    assert transport.requests[0].headers[header] == value


def test_resolver_explicit_credentials_reach_the_request_headers() -> None:
    transport = _Transport(RuntimeResponse(200, {}, _envelope()))
    resolver = CredentialResolver(explicit=CKANCredential(api_token="resolver-token"))
    client = SyncCatalogClient(transport, _profile(), credentials=resolver)

    client.datasets.get(_request(), _guard())

    assert transport.requests[0].headers["Authorization"] == "resolver-token"


def test_caller_headers_override_credential_headers() -> None:
    transport = _Transport(RuntimeResponse(200, {}, _envelope()))
    client = SyncCatalogClient(transport, _profile(), credentials=CKANCredential(api_token="ckan-token"))
    request = CatalogOperationRequest(
        _request().operation_id,
        {"url": "http://127.0.0.1:8000/datasets/fixture", "headers": {"Authorization": "override"}},
    )

    client.datasets.get(request, _guard())

    assert transport.requests[0].headers["Authorization"] == "override"


def test_undeclared_mutation_policy_defaults_retry_safety_by_http_method() -> None:
    post_method_operation = OperationId("reference", "datasets", "get")
    post_request = CatalogOperationRequest(
        post_method_operation, {"url": "http://127.0.0.1:8000/datasets/fixture", "method": "POST"}
    )
    get_request = _request()

    post_transport = _FailingTransport()
    with pytest.raises(TransportFailure):
        SyncCatalogClient(post_transport, _profile()).datasets.get(post_request, _guard(post_method_operation))
    assert len(post_transport.requests) == 1

    get_transport = _FailingTransport()
    with pytest.raises(TransportFailure):
        SyncCatalogClient(get_transport, _profile(), retry_sleep=lambda _: None).datasets.get(get_request, _guard())
    assert len(get_transport.requests) == 3


def test_credential_scopes_are_stable_unique_and_not_derivable_from_values() -> None:
    first = _credential_scope(UDataCredential(api_key="alpha-secret"))
    second = _credential_scope(UDataCredential(api_key="beta-secret"))
    again = _credential_scope(UDataCredential(api_key="alpha-secret"))

    assert first == again
    assert first != second
    assert hashlib.sha256(b"alpha-secret").hexdigest()[:16] not in first
    assert "alpha-secret" not in first
    assert _credential_scope(None) == "anonymous"
    resolver = CredentialResolver(explicit=UDataCredential(api_key="alpha-secret"))
    assert _credential_scope(resolver) == first


def test_sync_client_exposes_its_transport_seam() -> None:
    transport = _Transport(RuntimeResponse(200, {}, _envelope()))
    client = SyncCatalogClient(transport, _profile())

    assert client.transport is transport


def test_sync_client_close_marks_closed_without_closing_borrowed_transport() -> None:
    transport = _Transport(RuntimeResponse(200, {}, _envelope()))
    client = SyncCatalogClient(transport, _profile(), owns_transport=False)

    client.close()
    client.close()

    assert transport.close_count == 0
    with pytest.raises(RuntimeError, match="closed"):
        client.datasets.get(_request(), _guard())


def test_sync_client_context_exit_keeps_borrowed_transport_open() -> None:
    transport = _Transport(RuntimeResponse(200, {}, _envelope()))

    with SyncCatalogClient(transport, _profile(), owns_transport=False) as client:
        client.datasets.get(_request(), _guard())

    assert transport.close_count == 0
    assert len(transport.requests) == 1
