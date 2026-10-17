from __future__ import annotations

import asyncio
import inspect
import json
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest

from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient, declared_udata_profile
from datasluice.connectors.catalog.udata.models.datasets import DatasetFollowersQuery, DatasetMutationResult
from datasluice.connectors.catalog.udata.models.reuses import ReuseFollowersQuery
from datasluice.connectors.catalog.udata.wire import datasets as wire
from datasluice.contracts.catalog.native.udata import (
    AsyncUDataDatasetsService,
    AsyncUDataServices,
    SyncUDataDatasetsService,
    SyncUDataServices,
)
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform, ResourceKind
from datasluice.domain.catalog.models import MappingRecord
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.safety import (
    ConcurrencyPolicy,
    ConfirmationPolicy,
    DryRunPolicy,
    IdempotencyPolicy,
    MutationPolicy,
)
from datasluice.errors.catalog import CatalogError, CatalogValidationError, ForbiddenError, UnauthenticatedError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse, TransportError
from tests.unit.connectors.catalog.test_udata_datasets import RouterAsyncTransport, RouterTransport, _site_first

ORIGIN = "http://127.0.0.1:5640"
DATASET = "507f1f77bcf86cd799439011"
PATH = f"/api/1/datasets/{DATASET}/followers/"
KEY = "dataset-follower-test-key"
CREDENTIAL = UDataCredential(api_key=KEY)
PERMISSIONS = EffectivePermissions.for_credential(CREDENTIAL, platform=CatalogPlatform.UDATA)
FOLLOW = "udata/api-v1.follow-dataset"
UNFOLLOW = "udata/api-v1.unfollow-dataset"
LIST = "udata/api-v1.list-dataset-followers"


def _policy(operation: str, target: str = DATASET) -> MutationPolicy:
    return MutationPolicy(
        destructive=operation == UNFOLLOW,
        confirmation=ConfirmationPolicy(confirmed=True, operation=operation, target=target),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


def _transport(mode: str, routes: dict[tuple[str, str], object]) -> RouterTransport | RouterAsyncTransport:
    return RouterTransport(_site_first(routes)) if mode == "sync" else RouterAsyncTransport(_site_first(routes))


def _invoke(
    mode: str,
    transport: RouterTransport | RouterAsyncTransport,
    method: str,
    dataset_id: str = DATASET,
    *,
    credentials: Any = CREDENTIAL,
    origin: str = ORIGIN,
    **kwargs: Any,
) -> DatasetMutationResult | MappingRecord:
    if mode == "sync":
        assert isinstance(transport, RouterTransport)
        with SyncUDataClient(
            transport, declared_udata_profile(), origin=origin, credentials=credentials, owns_transport=False
        ) as client:
            return cast("DatasetMutationResult | MappingRecord", getattr(client.datasets, method)(dataset_id, **kwargs))

    async def run() -> DatasetMutationResult | MappingRecord:
        assert isinstance(transport, RouterAsyncTransport)
        async with AsyncUDataClient(
            transport, declared_udata_profile(), origin=origin, credentials=credentials, owns_transport=False
        ) as client:
            return cast(
                "DatasetMutationResult | MappingRecord", await getattr(client.datasets, method)(dataset_id, **kwargs)
            )

    return asyncio.run(run())


def _receipt(error: BaseException) -> MutationReceipt:
    receipt = getattr(error, "mutation_receipt", None)
    assert isinstance(receipt, MutationReceipt)
    return receipt


def _page() -> dict[str, object]:
    return {
        "data": [{"id": "follow-id", "follower": {"id": "user-id"}, "since": "2026-10-17T00:00:00Z"}],
        "page": 2,
        "page_size": 5,
        "total": 1,
        "previous_page": None,
        "next_page": None,
    }


def test_dataset_follower_routes_match_the_independent_pinned_capture() -> None:
    path = Path(__file__).parents[4] / ".planning/phases/04-udata-connector/oracle/udata-source-routes.json"
    if not path.exists():
        pytest.skip("the independent capture requires the local-only .planning tree")
    document = json.loads(path.read_text(encoding="utf-8"))
    assert document["source_commit"] == "0546582058d84706812a1c37387576efc4e5ad1f"
    routes = {
        route["method"]: route for route in document["routes"] if route["path"] == "/api/1/datasets/<id>/followers/"
    }
    assert set(routes) == {"GET", "POST", "DELETE"}
    for method in routes:
        assert routes[method]["api_version"] == "v1"
        assert routes[method]["signature"] == "udata.core.dataset.api:DatasetFollowersAPI"
    assert wire.list_dataset_followers_request(DATASET, DatasetFollowersQuery(page=2, page_size=5, user="a & b")) == (
        "GET",
        f"{PATH}?page=2&page_size=5&user=a+%26+b",
        {},
        None,
    )
    assert wire.follow_dataset_request(DATASET) == ("POST", PATH, {}, None)
    assert wire.unfollow_dataset_request(DATASET) == ("DELETE", PATH, {}, None)


def test_dataset_follower_query_reuses_the_existing_stock_follow_parser() -> None:
    assert DatasetFollowersQuery is ReuseFollowersQuery
    assert DatasetFollowersQuery().query_params() == [("page", "1"), ("page_size", "20")]
    for kwargs in ({"page": 0}, {"page_size": 101}, {"page": True}, {"user": ""}, {"user": 3}):
        with pytest.raises(ValueError):
            DatasetFollowersQuery(**kwargs)


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_dataset_follower_read_preserves_the_native_page_without_dataset_decoding(mode: str) -> None:
    payload = _page()
    transport = _transport(mode, {("GET", ORIGIN + PATH + "?page=2&page_size=5&user=user-id"): payload})
    result = _invoke(
        mode,
        transport,
        "list_dataset_followers",
        credentials=None,
        query=DatasetFollowersQuery(page=2, page_size=5, user="user-id"),
    )
    assert isinstance(result, MappingRecord)
    assert result.to_dict()["payload"] == payload
    request = transport.requests[-1]
    assert request.body is None and request.files == ()
    assert not any(key.lower() == "x-api-key" for key in request.headers)


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize(
    ("method", "verb", "operation", "status", "count"),
    [
        ("follow_dataset", "POST", FOLLOW, 201, 1),
        ("follow_dataset", "POST", FOLLOW, 200, 1),
        ("unfollow_dataset", "DELETE", UNFOLLOW, 200, 0),
    ],
)
def test_dataset_follower_mutations_keep_exact_wire_counts_and_receipt_targets(
    mode: str, method: str, verb: str, operation: str, status: int, count: int
) -> None:
    transport = _transport(mode, {(verb, ORIGIN + PATH): (status, {"followers": count})})
    result = _invoke(mode, transport, method, permissions=PERMISSIONS, mutation_policy=_policy(operation))
    assert isinstance(result, DatasetMutationResult)
    assert result.record is None
    assert result.extras == {"followers": count}
    assert str(result.receipt.operation) == operation
    assert result.receipt.target.resource_kind == ResourceKind.DATASET
    assert result.receipt.target.value == DATASET
    assert result.receipt.outcome == "succeeded"
    assert result.receipt.audit_metadata["status_code"] == status
    requests = [request for request in transport.requests if request.url == ORIGIN + PATH]
    assert len(requests) == 1
    assert requests[0].method == verb and requests[0].body is None and requests[0].files == ()
    assert requests[0].headers["X-API-KEY"] == KEY
    assert KEY not in json.dumps(result.to_dict())


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("method", ["follow_dataset", "unfollow_dataset"])
@pytest.mark.parametrize(
    "defect", ["no-credential", "no-permissions", "wrong-credential", "wrong-platform", "anonymous", "scope"]
)
def test_dataset_follower_permissions_fail_before_any_io(mode: str, method: str, defect: str) -> None:
    operation = FOLLOW if method == "follow_dataset" else UNFOLLOW
    credential = None if defect == "no-credential" else CREDENTIAL
    permissions = PERMISSIONS
    if defect == "no-permissions":
        permissions = cast("EffectivePermissions", None)
    elif defect == "wrong-credential":
        permissions = EffectivePermissions.for_credential(
            UDataCredential(api_key="other"), platform=CatalogPlatform.UDATA
        )
    elif defect == "wrong-platform":
        permissions = replace(PERMISSIONS, platform=CatalogPlatform.CKAN)
    elif defect == "anonymous":
        permissions = replace(PERMISSIONS, authenticated=False)
    elif defect == "scope":
        permissions = replace(PERMISSIONS, operation_scopes={operation: frozenset({"follow"})})
    transport = _transport(mode, {})
    with pytest.raises((ForbiddenError, UnauthenticatedError)) as raised:
        _invoke(
            mode, transport, method, credentials=credential, permissions=permissions, mutation_policy=_policy(operation)
        )
    assert transport.requests == []
    receipt = _receipt(raised.value)
    assert receipt.outcome == "rejected" and str(receipt.operation) == operation


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("method", ["follow_dataset", "unfollow_dataset"])
@pytest.mark.parametrize(
    "defect",
    [
        "missing",
        "unconfirmed",
        "wrong-operation",
        "wrong-target",
        "concurrency",
        "token",
        "retry",
        "dry-run",
        "destructive",
    ],
)
def test_dataset_follower_mutation_policy_fails_before_any_io(mode: str, method: str, defect: str) -> None:
    operation = FOLLOW if method == "follow_dataset" else UNFOLLOW
    valid = _policy(operation)
    policies = {
        "missing": None,
        "unconfirmed": replace(valid, confirmation=ConfirmationPolicy()),
        "wrong-operation": replace(
            valid, confirmation=ConfirmationPolicy(confirmed=True, operation="other", target=DATASET)
        ),
        "wrong-target": replace(
            valid, confirmation=ConfirmationPolicy(confirmed=True, operation=operation, target="other")
        ),
        "concurrency": replace(valid, concurrency=ConcurrencyPolicy()),
        "token": replace(valid, concurrency=ConcurrencyPolicy(overwrite=True, token="unsupported")),
        "retry": replace(valid, idempotency=IdempotencyPolicy(safe=True)),
        "dry-run": replace(valid, dry_run=DryRunPolicy(requested=True)),
        "destructive": replace(valid, destructive=not valid.destructive),
    }
    transport = _transport(mode, {})
    with pytest.raises(ForbiddenError) as raised:
        _invoke(mode, transport, method, permissions=PERMISSIONS, mutation_policy=policies[defect])
    assert transport.requests == []
    assert _receipt(raised.value).outcome == "rejected"


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("method", ["list_dataset_followers", "follow_dataset", "unfollow_dataset"])
@pytest.mark.parametrize("identifier", ["", ".", "..", "id/next", "id?query", "id#fragment", "id\n", 'id"', None])
def test_dataset_follower_ids_fail_before_any_io(mode: str, method: str, identifier: str | None) -> None:
    transport = _transport(mode, {})
    kwargs: dict[str, Any] = {} if method == "list_dataset_followers" else {"permissions": PERMISSIONS}
    with pytest.raises(CatalogValidationError) as raised:
        _invoke(mode, transport, method, cast("str", identifier), **kwargs)
    assert transport.requests == []
    if method != "list_dataset_followers":
        assert _receipt(raised.value).outcome == "rejected"


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("method", ["follow_dataset", "unfollow_dataset"])
@pytest.mark.parametrize(
    "payload",
    [None, {}, {"followers": None}, {"followers": True}, {"followers": -1}, {"followers": "1"}, {"followers": 1.0}],
)
def test_dataset_follower_malformed_mutation_responses_are_ambiguous(mode: str, method: str, payload: object) -> None:
    operation = FOLLOW if method == "follow_dataset" else UNFOLLOW
    verb = "POST" if operation == FOLLOW else "DELETE"
    transport = _transport(mode, {(verb, ORIGIN + PATH): (200, payload)})
    with pytest.raises(CatalogValidationError) as raised:
        _invoke(mode, transport, method, permissions=PERMISSIONS, mutation_policy=_policy(operation))
    receipt = _receipt(raised.value)
    assert receipt.outcome == "ambiguous" and receipt.audit_metadata["status_code"] == 200
    assert sum(request.url == ORIGIN + PATH for request in transport.requests) == 1


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        {},
        {"data": None},
        {"data": ["bad"]},
        {"data": [{"follower": "bad"}]},
        {"data": [{"id": 3}]},
        {"data": [{"since": 3}]},
    ],
)
def test_dataset_follower_malformed_read_responses_fail_typed(mode: str, payload: object) -> None:
    transport = _transport(mode, {("GET", ORIGIN + PATH + "?page=1&page_size=20"): payload})
    with pytest.raises(CatalogValidationError) as raised:
        _invoke(mode, transport, "list_dataset_followers", credentials=None)
    assert raised.value.operation == LIST


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("method", ["follow_dataset", "unfollow_dataset"])
@pytest.mark.parametrize("status", [401, 403, 404, 500])
def test_dataset_follower_http_errors_keep_the_failed_receipt(mode: str, method: str, status: int) -> None:
    operation = FOLLOW if method == "follow_dataset" else UNFOLLOW
    verb = "POST" if operation == FOLLOW else "DELETE"
    transport = _transport(mode, {(verb, ORIGIN + PATH): (status, {"message": "Refused"})})
    with pytest.raises(CatalogError) as raised:
        _invoke(mode, transport, method, permissions=PERMISSIONS, mutation_policy=_policy(operation))
    receipt = _receipt(raised.value)
    assert receipt.outcome == "failed" and receipt.audit_metadata["status_code"] == status
    assert receipt.target.value == DATASET and str(receipt.operation) == operation
    assert sum(request.url == ORIGIN + PATH for request in transport.requests) == 1


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("method", ["follow_dataset", "unfollow_dataset"])
def test_dataset_followers_refuse_public_mutations(mode: str, method: str) -> None:
    operation = FOLLOW if method == "follow_dataset" else UNFOLLOW
    public_origin = "https://example.org"
    transport = _transport(mode, {("GET", public_origin + "/api/1/site/"): {"version": "17.6.0"}})
    with pytest.raises(CatalogValidationError) as raised:
        _invoke(
            mode, transport, method, origin=public_origin, permissions=PERMISSIONS, mutation_policy=_policy(operation)
        )
    assert all(request.method == "GET" for request in transport.requests)
    assert _receipt(raised.value).outcome == "rejected"


def test_dataset_follower_profile_and_native_protocols_are_route_specific() -> None:
    operations = {str(identifier): operation for identifier, operation in declared_udata_profile().operations.items()}
    assert operations[LIST].mutation_class == "read" and operations[LIST].auth_class == "public"
    assert operations[FOLLOW].mutation_class == "update" and operations[FOLLOW].auth_class == "authenticated"
    assert operations[UNFOLLOW].mutation_class == "delete" and operations[UNFOLLOW].auth_class == "authenticated"
    for method in ("list_dataset_followers", "follow_dataset", "unfollow_dataset"):
        assert hasattr(SyncUDataDatasetsService, method)
        assert inspect.iscoroutinefunction(getattr(AsyncUDataDatasetsService, method))
    assert (
        inspect.getattr_static(SyncUDataServices, "datasets").fget.__annotations__["return"]
        == "SyncUDataDatasetsService"
    )
    assert (
        inspect.getattr_static(AsyncUDataServices, "datasets").fget.__annotations__["return"]
        == "AsyncUDataDatasetsService"
    )


def test_dataset_follower_paths_encode_raw_identifiers_once() -> None:
    identifier = "raw%2F ü"
    path = "/api/1/datasets/raw%252F%20%C3%BC/followers/"
    assert wire.follow_dataset_request(identifier)[1] == path
    assert wire.unfollow_dataset_request(identifier)[1] == path
    assert wire.list_dataset_followers_request(identifier, DatasetFollowersQuery())[1] == path + "?page=1&page_size=20"


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("method", ["follow_dataset", "unfollow_dataset"])
@pytest.mark.parametrize("failure", [TransportError, KeyboardInterrupt])
def test_dataset_follower_dispatch_failure_keeps_its_receipt_without_a_retry(
    mode: str, method: str, failure: type[BaseException], monkeypatch: pytest.MonkeyPatch
) -> None:
    operation = FOLLOW if method == "follow_dataset" else UNFOLLOW
    transport = _transport(mode, {})
    if isinstance(transport, RouterTransport):
        original = transport.send

        def send(request: RuntimeRequest) -> RuntimeResponse:
            if request.url == ORIGIN + PATH:
                transport.requests.append(request)
                raise failure("Synthetic dispatch failure")
            return original(request)

        monkeypatch.setattr(transport, "send", send)
    else:
        original_async = transport.send

        async def send_async(request: RuntimeRequest) -> RuntimeResponse:
            if request.url == ORIGIN + PATH:
                transport.requests.append(request)
                raise failure("Synthetic dispatch failure")
            return await original_async(request)

        monkeypatch.setattr(transport, "send", send_async)
    with pytest.raises(failure) as raised:
        _invoke(mode, transport, method, permissions=PERMISSIONS, mutation_policy=_policy(operation))
    assert _receipt(raised.value).outcome == ("cancelled" if failure is KeyboardInterrupt else "ambiguous")
    assert sum(request.url == ORIGIN + PATH for request in transport.requests) == 1


@pytest.mark.parametrize("method", ["follow_dataset", "unfollow_dataset"])
def test_dataset_follower_cancellation_keeps_its_receipt(method: str, monkeypatch: pytest.MonkeyPatch) -> None:
    operation = FOLLOW if method == "follow_dataset" else UNFOLLOW
    transport = RouterAsyncTransport(_site_first({}))
    original = transport.send

    async def send(request: RuntimeRequest) -> RuntimeResponse:
        if request.url == ORIGIN + PATH:
            transport.requests.append(request)
            raise asyncio.CancelledError
        return await original(request)

    monkeypatch.setattr(transport, "send", send)
    with pytest.raises(asyncio.CancelledError) as raised:
        _invoke("async", transport, method, permissions=PERMISSIONS, mutation_policy=_policy(operation))
    assert _receipt(raised.value).outcome == "cancelled"
    assert sum(request.url == ORIGIN + PATH for request in transport.requests) == 1


def test_dataset_followers_use_async_credential_resolution() -> None:
    class AsyncOnlyProvider:
        calls = 0

        async def resolve_async(self) -> UDataCredential:
            self.calls += 1
            return CREDENTIAL

    provider = AsyncOnlyProvider()
    transport = _transport("async", {("POST", ORIGIN + PATH): (201, {"followers": 1})})
    result = _invoke(
        "async",
        transport,
        "follow_dataset",
        credentials=provider,
        permissions=PERMISSIONS,
        mutation_policy=_policy(FOLLOW),
    )
    assert isinstance(result, DatasetMutationResult) and result.receipt.outcome == "succeeded"
    assert provider.calls == 1
