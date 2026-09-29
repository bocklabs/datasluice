"""Exact wire and safety coverage for the uData reuse and reuse-follower family.

Expectations are transcribed from the pinned uData 17.6.0 source
(`udata.core.reuse.api`, `udata.core.reuse.apiv2`, and
`udata.core.followers.api` at commit 0546582), not from the production
request builders.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from typing import cast

import pytest

from datasluice.connectors.catalog.udata.clients import (
    AsyncUDataClient,
    SyncUDataClient,
    declared_udata_profile,
)
from datasluice.connectors.catalog.udata.models.reuses import (
    ReuseCreateInput,
    ReuseFollowersQuery,
    ReuseListQuery,
    ReuseSearchQuery,
    ReuseSuggestQuery,
    ReuseUpdateInput,
    segment,
)
from datasluice.connectors.catalog.udata.services.reuses import (
    AsyncReusesService,
    SyncReusesService,
)
from datasluice.connectors.catalog.udata.wire import reuses as wire
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.errors.catalog import CatalogNotFoundError, CatalogValidationError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse

ORIGIN = "http://127.0.0.1:5640"
SITE = {"feed_size": 0, "id": "site", "keywords": [], "metrics": {}, "title": "uData", "version": "17.6.0"}
CREDENTIAL = UDataCredential(api_key="secret-key")
PERMISSIONS = EffectivePermissions.for_credential(CREDENTIAL, platform=CatalogPlatform.UDATA)
CREATE_ONLY_PERMISSIONS = EffectivePermissions.for_credential(
    CREDENTIAL,
    platform=CatalogPlatform.UDATA,
    scopes=frozenset({"create-reuse"}),
    operation_scopes={wire.CREATE_REUSE_OPERATION: frozenset({"create-reuse"})},
)
DELETE_ONLY_PERMISSIONS = EffectivePermissions.for_credential(
    CREDENTIAL,
    platform=CatalogPlatform.UDATA,
    scopes=frozenset({"delete-reuse"}),
    operation_scopes={wire.DELETE_REUSE_OPERATION: frozenset({"delete-reuse"})},
)
ADMIN_PERMISSIONS = EffectivePermissions.for_credential(
    CREDENTIAL, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
)


class _Router:
    def __init__(self, routes: dict[tuple[str, str], tuple[int, object]]) -> None:
        self.routes = routes
        self.requests: list[RuntimeRequest] = []

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        status, payload = self.routes[(request.method, request.url)]
        if payload is None:
            body = b""
        elif isinstance(payload, bytes):
            body = payload
        else:
            body = json.dumps(payload, separators=(",", ":")).encode()
        media = "application/atom+xml" if isinstance(payload, bytes) else "application/json"
        return RuntimeResponse(status_code=status, headers={"Content-Type": media}, body=body)

    def close(self) -> None:
        return None


class _AsyncRouter:
    def __init__(self, routes: dict[tuple[str, str], tuple[int, object]]) -> None:
        self.routes = routes
        self.requests: list[RuntimeRequest] = []

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        status, payload = self.routes[(request.method, request.url)]
        if payload is None:
            body = b""
        elif isinstance(payload, bytes):
            body = payload
        else:
            body = json.dumps(payload, separators=(",", ":")).encode()
        media = "application/atom+xml" if isinstance(payload, bytes) else "application/json"
        return RuntimeResponse(status_code=status, headers={"Content-Type": media}, body=body)

    async def aclose(self) -> None:
        return None


def _routes(routes: dict[tuple[str, str], tuple[int, object]]) -> dict[tuple[str, str], tuple[int, object]]:
    return {("GET", f"{ORIGIN}/api/1/site/"): (200, SITE), **routes}


def _policy(operation: str, target: str) -> MutationPolicy:
    return MutationPolicy(
        destructive=operation
        in {
            wire.DELETE_REUSE_OPERATION,
            wire.DELETE_REUSE_BADGE_OPERATION,
            wire.UNFEATURE_REUSE_OPERATION,
            wire.UNFOLLOW_REUSE_OPERATION,
        },
        confirmation=ConfirmationPolicy(confirmed=True, operation=operation, target=target),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


def _reuse(id_: str = "reuse-1", featured: bool = False) -> dict[str, object]:
    return {
        "id": id_,
        "title": "A reuse",
        "description": "A description",
        "type": "application",
        "slug": "a-reuse",
        "tags": ["tag"],
        "topic": "health",
        "private": False,
        "featured": featured,
        "image": None,
        "created_at": "2026-01-01T00:00:00+00:00",
        "last_modified": "2026-01-01T00:00:00+00:00",
        "extras": {},
        "datasets": [],
        "dataservices": [],
    }


def _page_body(item: object | None = None) -> dict[str, object]:
    return {
        "data": [item if item is not None else _reuse()],
        "page": 1,
        "page_size": 20,
        "total": 1,
        "next_page": None,
        "previous_page": None,
    }


def _thawed(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thawed(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_thawed(item) for item in value]
    return value


@pytest.mark.parametrize("identifier", [".", ".."])
def test_reuses_identifiers_reject_dot_segments(identifier: str) -> None:
    """A bare dot segment is removed by RFC 3986 resolution, retargeting the route."""
    with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
        segment(identifier, "list_reuses")


def test_reuse_contract_exposes_every_assigned_method_in_both_modes() -> None:
    sync_names = {
        name
        for name in dir(SyncReusesService)
        if not name.startswith("_") and callable(getattr(SyncReusesService, name))
    }
    async_names = {
        name
        for name in dir(AsyncReusesService)
        if not name.startswith("_") and callable(getattr(AsyncReusesService, name))
    }
    assert sync_names == async_names
    assert sync_names == {
        "list_reuses",
        "create_reuse",
        "recent_reuses_atom_feed",
        "get_reuse",
        "update_reuse",
        "delete_reuse",
        "reuse_add_dataset",
        "reuse_add_dataservice",
        "available_reuse_badges",
        "add_reuse_badge",
        "delete_reuse_badge",
        "feature_reuse",
        "unfeature_reuse",
        "suggest_reuses",
        "reuse_image",
        "reuse_types",
        "reuse_topics",
        "search_v2",
        "list_v2",
        "list_reuse_followers",
        "follow_reuse",
        "unfollow_reuse",
    }


def test_reuse_reads_match_exact_wire_and_preserve_native_envelopes() -> None:
    router = _Router(
        _routes(
            {
                ("GET", f"{ORIGIN}/api/1/reuses/?page=1&page_size=20"): (200, _page_body()),
                ("GET", f"{ORIGIN}/api/1/reuses/?page=1&page_size=20&q=x"): (200, _page_body()),
                ("GET", f"{ORIGIN}/api/1/reuses/reuse-1/"): (200, _reuse()),
                ("GET", f"{ORIGIN}/api/1/reuses/recent.atom?page=1&page_size=20"): (200, b"<feed/>"),
                ("GET", f"{ORIGIN}/api/1/reuses/badges/"): (200, {"badger": "Badger"}),
                ("GET", f"{ORIGIN}/api/1/reuses/suggest/?q=ab&size=3"): (200, [{"id": "r1", "title": "T"}]),
                ("GET", f"{ORIGIN}/api/1/reuses/types/"): (200, [{"id": "application", "label": "Application"}]),
                ("GET", f"{ORIGIN}/api/1/reuses/topics/"): (200, [{"id": "health", "label": "Health"}]),
                ("GET", f"{ORIGIN}/api/2/reuses/?page=1&page_size=20"): (200, _page_body()),
                ("GET", f"{ORIGIN}/api/2/reuses/search/?page=1&page_size=50&q=x"): (200, _page_body()),
                ("GET", f"{ORIGIN}/api/1/reuses/reuse-1/followers/?page=1&page_size=20&user=u1"): (
                    200,
                    {
                        "data": [{"id": "f1", "follower": {"id": "u1"}, "since": "2026-01-01T00:00:00+00:00"}],
                        "page": 1,
                        "page_size": 20,
                        "total": 1,
                        "next_page": None,
                        "previous_page": None,
                    },
                ),
            }
        )
    )
    with SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client:
        assert _thawed(client.reuses.list_reuses().payload) == _page_body()
        assert _thawed(client.reuses.list_reuses(ReuseListQuery(q="x")).payload) == _page_body()
        assert _thawed(client.reuses.get_reuse("reuse-1").payload) == _reuse()
        atom = client.reuses.recent_reuses_atom_feed()
        assert atom.payload["media_type"] == "application/atom+xml"
        assert atom.payload["size_bytes"] == len("<feed/>")
        assert atom.payload["sha256"]
        assert _thawed(client.reuses.available_reuse_badges().payload) == {"badger": "Badger"}
        assert _thawed(client.reuses.suggest_reuses(ReuseSuggestQuery(q="ab", size=3))[0].payload) == {
            "id": "r1",
            "title": "T",
        }
        assert _thawed(client.reuses.reuse_types()[0].payload) == {"id": "application", "label": "Application"}
        assert _thawed(client.reuses.reuse_topics()[0].payload) == {"id": "health", "label": "Health"}
        assert _thawed(client.reuses.list_v2().payload) == _page_body()
        assert _thawed(client.reuses.search_v2(ReuseSearchQuery(q="x")).payload) == _page_body()
        followers = client.reuses.list_reuse_followers("reuse-1", ReuseFollowersQuery(user="u1"))
        assert cast(Mapping[str, object], _thawed(followers.payload))["total"] == 1
    reuse_requests = [r for r in router.requests if "/reuses/" in r.url]
    assert reuse_requests[0].method == "GET"
    assert reuse_requests[0].url == f"{ORIGIN}/api/1/reuses/?page=1&page_size=20"


def test_reuse_create_required_fields_reject_whitespace() -> None:
    invalid_inputs = [
        lambda: ReuseCreateInput(
            title=" ", description="d", type="application", url="https://example.com", topic="health"
        ),
        lambda: ReuseCreateInput(
            title="A", description=" ", type="application", url="https://example.com", topic="health"
        ),
        lambda: ReuseCreateInput(title="A", description="d", type=" ", url="https://example.com", topic="health"),
        lambda: ReuseCreateInput(title="A", description="d", type="application", url=" ", topic="health"),
        lambda: ReuseCreateInput(title="A", description="d", type="application", url="https://example.com", topic=" "),
    ]
    for create_invalid in invalid_inputs:
        with pytest.raises(ValueError):
            create_invalid()


def test_reuse_create_and_update_match_exact_wire_and_receipts() -> None:
    router = _Router(
        _routes(
            {
                ("POST", f"{ORIGIN}/api/1/reuses/"): (201, _reuse()),
                ("PUT", f"{ORIGIN}/api/1/reuses/reuse-1/"): (200, _reuse()),
            }
        )
    )
    with SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client:
        created = client.reuses.create_reuse(
            ReuseCreateInput(
                title="A reuse",
                description="A description",
                type="application",
                url="https://example.com",
                topic="health",
            ),
            CREATE_ONLY_PERMISSIONS,
            _policy(wire.CREATE_REUSE_OPERATION, "A reuse"),
        )
        assert created.receipt.target.value == "A reuse"
        assert created.receipt.outcome == "succeeded"
        assert router.requests[-1].method == "POST"
        assert router.requests[-1].url == f"{ORIGIN}/api/1/reuses/"
        assert dict(router.requests[-1].headers) == {"X-API-KEY": "secret-key", "Content-Type": "application/json"}
        assert json.loads(router.requests[-1].body or b"{}") == {
            "title": "A reuse",
            "description": "A description",
            "type": "application",
            "url": "https://example.com",
            "topic": "health",
        }
        updated = client.reuses.update_reuse(
            "reuse-1",
            ReuseUpdateInput(title="A reuse"),
            PERMISSIONS,
            _policy(wire.UPDATE_REUSE_OPERATION, "reuse-1"),
        )
        assert updated.receipt.target.value == "reuse-1"
        assert updated.receipt.outcome == "succeeded"


def test_reuse_delete_targets_exact_entity_and_receipts() -> None:
    router = _Router(_routes({("DELETE", f"{ORIGIN}/api/1/reuses/reuse-1/"): (204, None)}))
    with SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client:
        deleted = client.reuses.delete_reuse(
            "reuse-1", DELETE_ONLY_PERMISSIONS, _policy(wire.DELETE_REUSE_OPERATION, "reuse-1")
        )
        assert deleted.receipt.target.value == "reuse-1"
        assert deleted.receipt.outcome == "succeeded"
        assert router.requests[-1].method == "DELETE"
        assert router.requests[-1].url == f"{ORIGIN}/api/1/reuses/reuse-1/"


def test_reuse_add_dataset_and_dataservice_match_exact_wire() -> None:
    router = _Router(
        _routes(
            {
                ("POST", f"{ORIGIN}/api/1/reuses/reuse-1/datasets/"): (201, _reuse()),
                ("POST", f"{ORIGIN}/api/1/reuses/reuse-1/dataservices/"): (201, _reuse()),
            }
        )
    )
    with SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client:
        client.reuses.reuse_add_dataset(
            "reuse-1", "dataset-1", PERMISSIONS, _policy(wire.REUSE_ADD_DATASET_OPERATION, "reuse-1")
        )
        client.reuses.reuse_add_dataservice(
            "reuse-1", "dataservice-1", PERMISSIONS, _policy(wire.REUSE_ADD_DATASERVICE_OPERATION, "reuse-1")
        )
    reuse_requests = [r for r in router.requests if "/reuses/" in r.url]
    assert reuse_requests[0].method == "POST"
    assert reuse_requests[0].url == f"{ORIGIN}/api/1/reuses/reuse-1/datasets/"
    assert json.loads(reuse_requests[0].body or b"{}") == {"id": "dataset-1"}
    assert reuse_requests[1].method == "POST"
    assert reuse_requests[1].url == f"{ORIGIN}/api/1/reuses/reuse-1/dataservices/"
    assert json.loads(reuse_requests[1].body or b"{}") == {"id": "dataservice-1"}


def test_reuse_badge_and_feature_mutations_match_exact_wire() -> None:
    router = _Router(
        _routes(
            {
                ("POST", f"{ORIGIN}/api/1/reuses/reuse-1/badges/"): (201, {"id": "badger", "label": "Badger"}),
                ("DELETE", f"{ORIGIN}/api/1/reuses/reuse-1/badges/badger/"): (204, None),
                ("POST", f"{ORIGIN}/api/1/reuses/reuse-1/featured/"): (200, _reuse(featured=True)),
                ("DELETE", f"{ORIGIN}/api/1/reuses/reuse-1/featured/"): (200, _reuse()),
            }
        )
    )
    with SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client:
        client.reuses.add_reuse_badge(
            "reuse-1", "badger", PERMISSIONS, _policy(wire.ADD_REUSE_BADGE_OPERATION, "reuse-1")
        )
        client.reuses.delete_reuse_badge(
            "reuse-1", "badger", PERMISSIONS, _policy(wire.DELETE_REUSE_BADGE_OPERATION, "reuse-1")
        )
        client.reuses.feature_reuse("reuse-1", ADMIN_PERMISSIONS, _policy(wire.FEATURE_REUSE_OPERATION, "reuse-1"))
        client.reuses.unfeature_reuse("reuse-1", ADMIN_PERMISSIONS, _policy(wire.UNFEATURE_REUSE_OPERATION, "reuse-1"))
    reuse_requests = [r for r in router.requests if "/reuses/" in r.url]
    assert reuse_requests[0].url == f"{ORIGIN}/api/1/reuses/reuse-1/badges/"
    assert json.loads(reuse_requests[0].body or b"{}") == {"kind": "badger"}
    assert reuse_requests[1].url == f"{ORIGIN}/api/1/reuses/reuse-1/badges/badger/"
    assert reuse_requests[2].url == f"{ORIGIN}/api/1/reuses/reuse-1/featured/"
    assert reuse_requests[3].url == f"{ORIGIN}/api/1/reuses/reuse-1/featured/"


def test_reuse_image_upload_streams_multipart_and_receipts() -> None:
    router = _Router(_routes({("POST", f"{ORIGIN}/api/1/reuses/reuse-1/image/"): (200, _reuse())}))
    with SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client:
        result = client.reuses.reuse_image(
            "reuse-1", b"img-bytes", "image/png", PERMISSIONS, _policy(wire.REUSE_IMAGE_OPERATION, "reuse-1")
        )
        assert result.receipt.target.value == "reuse-1"
        assert router.requests[-1].method == "POST"
        assert router.requests[-1].url == f"{ORIGIN}/api/1/reuses/reuse-1/image/"


def test_reuse_follower_mutations_match_exact_wire() -> None:
    router = _Router(
        _routes(
            {
                ("POST", f"{ORIGIN}/api/1/reuses/reuse-1/followers/"): (201, {"followers": 2}),
                ("DELETE", f"{ORIGIN}/api/1/reuses/reuse-1/followers/"): (200, {"followers": 1}),
            }
        )
    )
    with SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client:
        client.reuses.follow_reuse("reuse-1", PERMISSIONS, _policy(wire.FOLLOW_REUSE_OPERATION, "reuse-1"))
        client.reuses.unfollow_reuse("reuse-1", PERMISSIONS, _policy(wire.UNFOLLOW_REUSE_OPERATION, "reuse-1"))
    reuse_requests = [r for r in router.requests if "/reuses/" in r.url]
    assert reuse_requests[0].method == "POST"
    assert reuse_requests[0].url == f"{ORIGIN}/api/1/reuses/reuse-1/followers/"
    assert reuse_requests[1].method == "DELETE"
    assert reuse_requests[1].url == f"{ORIGIN}/api/1/reuses/reuse-1/followers/"


def test_reuse_mutation_failure_yields_redacted_receipt_with_exact_target() -> None:
    router = _Router(_routes({("DELETE", f"{ORIGIN}/api/1/reuses/reuse-1/"): (404, {"message": "Not found"})}))
    with SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client:
        with pytest.raises(CatalogNotFoundError) as error:
            client.reuses.delete_reuse(
                "reuse-1", DELETE_ONLY_PERMISSIONS, _policy(wire.DELETE_REUSE_OPERATION, "reuse-1")
            )
        receipt = error.value.__dict__["mutation_receipt"]
        assert receipt.target.value == "reuse-1"
        assert receipt.outcome == "failed"


def test_reuse_async_mode_matches_sync_exact_wire() -> None:
    router = _AsyncRouter(
        _routes(
            {
                ("GET", f"{ORIGIN}/api/1/reuses/?page=1&page_size=20"): (200, _page_body()),
                ("DELETE", f"{ORIGIN}/api/1/reuses/reuse-1/"): (204, None),
            }
        )
    )

    async def run() -> None:
        async with AsyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client:
            page = await client.reuses.list_reuses()
            assert _thawed(page.payload) == _page_body()
            deleted = await client.reuses.delete_reuse(
                "reuse-1", DELETE_ONLY_PERMISSIONS, _policy(wire.DELETE_REUSE_OPERATION, "reuse-1")
            )
            assert deleted.receipt.target.value == "reuse-1"

    asyncio.run(run())
    reuse_requests = [r for r in router.requests if "/reuses/" in r.url]
    assert reuse_requests[0].method == "GET"
    assert reuse_requests[0].url == f"{ORIGIN}/api/1/reuses/?page=1&page_size=20"
    assert reuse_requests[1].method == "DELETE"
    assert reuse_requests[1].url == f"{ORIGIN}/api/1/reuses/reuse-1/"
