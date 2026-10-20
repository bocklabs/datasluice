from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING

from datasluice.connectors.catalog.udata.clients import (
    AsyncUDataClient,
    SyncUDataClient,
    declared_udata_profile,
)
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse

if TYPE_CHECKING:
    from datasluice.domain.catalog.receipts import MutationReceipt
    from datasluice.runtime.events import EventEmitter, ListSink

UDATA_ORIGIN = "http://127.0.0.1:5640"
UDATA_SITE = {"feed_size": 0, "id": "site", "keywords": [], "metrics": {}, "title": "uData", "version": "17.6.0"}
UDATA_SITE_ROUTE = ("GET", f"{UDATA_ORIGIN}/api/1/site/")
RouteTable = dict[tuple[str, str], tuple[int, object]]
MediaType = Callable[[object], str]

UDATA_CREDENTIAL = UDataCredential(api_key="secret-key")
UDATA_PERMISSIONS = EffectivePermissions.for_credential(UDATA_CREDENTIAL, platform=CatalogPlatform.UDATA)
UDATA_ADMIN_PERMISSIONS = EffectivePermissions.for_credential(
    UDATA_CREDENTIAL, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
)


def udata_url(path: str) -> str:
    return f"{UDATA_ORIGIN}{path}"


def with_site_route(routes: RouteTable, *, site: object = UDATA_SITE) -> RouteTable:
    return {UDATA_SITE_ROUTE: (200, site), **routes}


def declared_routes(*items: tuple[str, str, int, object], site: object = UDATA_SITE) -> RouteTable:
    return {
        UDATA_SITE_ROUTE: (200, site),
        **{(method, udata_url(path)): (status, body) for method, path, status, body in items},
    }


def udata_page(item: object, *, page_size: int = 20) -> dict[str, object]:
    return {
        "data": [item],
        "page": 1,
        "page_size": page_size,
        "total": 1,
        "next_page": None,
        "previous_page": None,
    }


def scoped_permissions(scope: str, operation: str) -> EffectivePermissions:
    return EffectivePermissions.for_credential(
        UDATA_CREDENTIAL,
        platform=CatalogPlatform.UDATA,
        scopes=frozenset({scope}),
        operation_scopes={operation: frozenset({scope})},
    )


def mutation_policy(operation: str, target: str, *, destructive: bool = False) -> MutationPolicy:
    return MutationPolicy(
        destructive=destructive,
        confirmation=ConfirmationPolicy(confirmed=True, operation=operation, target=target),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


def destructive_operations_policy(operations: frozenset[str]) -> Callable[..., MutationPolicy]:
    def build(operation: str, target: str) -> MutationPolicy:
        return mutation_policy(operation, target, destructive=operation in operations)

    return build


def thawed(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: thawed(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [thawed(item) for item in value]
    return value


def thawing_payload(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: thawing_payload(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [thawing_payload(item) for item in value]
    if hasattr(value, "payload"):
        return thawing_payload(value.payload)
    return value


def encoded_body(payload: object) -> bytes:
    if payload is None:
        return b""
    if isinstance(payload, bytes):
        return payload
    return json.dumps(payload, separators=(",", ":")).encode()


def json_media(payload: object) -> str:
    return "application/json"


def atom_or_json_media(payload: object) -> str:
    return "application/atom+xml" if isinstance(payload, bytes) else json_media(payload)


def empty_html_or_json_media(payload: object) -> str:
    return "text/html; charset=utf-8" if payload is None else json_media(payload)


class RouteRecorder:
    def __init__(self, routes: RouteTable, media_type: MediaType = json_media) -> None:
        self.routes = routes
        self.media_type = media_type
        self.requests: list[RuntimeRequest] = []

    def response_for(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        status, payload = self.routes[(request.method, request.url)]
        return RuntimeResponse(
            status_code=status,
            headers={"Content-Type": self.media_type(payload)},
            body=encoded_body(payload),
        )


class SyncRouteRouter(RouteRecorder):
    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        return self.response_for(request)

    def close(self) -> None:
        return None


class AsyncRouteRouter(RouteRecorder):
    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        return self.response_for(request)

    async def aclose(self) -> None:
        return None


def sync_route_table(routes: RouteTable) -> SyncRouteRouter:
    return SyncRouteRouter(routes)


def async_route_table(routes: RouteTable) -> AsyncRouteRouter:
    return AsyncRouteRouter(routes)


def atom_sync_route_table(routes: RouteTable) -> SyncRouteRouter:
    return SyncRouteRouter(routes, atom_or_json_media)


def atom_async_route_table(routes: RouteTable) -> AsyncRouteRouter:
    return AsyncRouteRouter(routes, atom_or_json_media)


def html_sync_route_table(routes: RouteTable) -> SyncRouteRouter:
    return SyncRouteRouter(routes, empty_html_or_json_media)


def html_async_route_table(routes: RouteTable) -> AsyncRouteRouter:
    return AsyncRouteRouter(routes, empty_html_or_json_media)


def sync_client(
    router: SyncRouteRouter, credentials: object | None = UDATA_CREDENTIAL, *, emitter: EventEmitter | None = None
) -> SyncUDataClient:
    return SyncUDataClient(
        router,
        declared_udata_profile(),
        origin=UDATA_ORIGIN,
        credentials=credentials,
        emitter=emitter,
    )


def async_client(
    router: AsyncRouteRouter, credentials: object | None = UDATA_CREDENTIAL, *, emitter: EventEmitter | None = None
) -> AsyncUDataClient:
    return AsyncUDataClient(
        router,
        declared_udata_profile(),
        origin=UDATA_ORIGIN,
        credentials=credentials,
        emitter=emitter,
    )


def anonymous_sync_client(router: SyncRouteRouter) -> SyncUDataClient:
    return sync_client(router, None)


def assert_ambiguous_mutation_receipt(
    raised: BaseException,
    receipt: MutationReceipt,
    events: ListSink,
    *,
    operation: str,
    target: str,
    status_code: int,
    secret: str,
) -> None:
    assert receipt.operation == operation
    assert receipt.outcome == "ambiguous"
    assert receipt.target.value == target
    assert receipt.audit_metadata["status_code"] == status_code
    assert secret not in repr(raised) + repr(receipt.to_dict())
    assert [event.outcome for event in events.events if event.operation_id == operation] == ["failed"]
