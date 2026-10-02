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
from datasluice.errors.catalog import CatalogNotFoundError, CatalogValidationError
from tests.helpers.udata_test_support import (
    UDATA_ADMIN_PERMISSIONS,
    UDATA_CREDENTIAL,
    UDATA_ORIGIN,
    UDATA_PERMISSIONS,
    async_client,
    atom_async_route_table,
    atom_sync_route_table,
    destructive_operations_policy,
    scoped_permissions,
    sync_client,
    thawed,
    udata_page,
    with_site_route,
)

ORIGIN = UDATA_ORIGIN
PERMISSIONS = UDATA_PERMISSIONS
ADMIN_PERMISSIONS = UDATA_ADMIN_PERMISSIONS
CREATE_ONLY_PERMISSIONS = scoped_permissions("create-reuse", wire.CREATE_REUSE_OPERATION)
DELETE_ONLY_PERMISSIONS = scoped_permissions("delete-reuse", wire.DELETE_REUSE_OPERATION)
_policy = destructive_operations_policy(
    frozenset(
        {
            wire.DELETE_REUSE_OPERATION,
            wire.DELETE_REUSE_BADGE_OPERATION,
            wire.UNFEATURE_REUSE_OPERATION,
            wire.UNFOLLOW_REUSE_OPERATION,
        }
    )
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
    router = atom_sync_route_table(
        with_site_route(
            {
                ("GET", f"{ORIGIN}/api/1/reuses/?page=1&page_size=20"): (200, udata_page(_reuse())),
                ("GET", f"{ORIGIN}/api/1/reuses/?page=1&page_size=20&q=x"): (200, udata_page(_reuse())),
                ("GET", f"{ORIGIN}/api/1/reuses/reuse-1/"): (200, _reuse()),
                ("GET", f"{ORIGIN}/api/1/reuses/recent.atom?page=1&page_size=20"): (200, b"<feed/>"),
                ("GET", f"{ORIGIN}/api/1/reuses/badges/"): (200, {"badger": "Badger"}),
                ("GET", f"{ORIGIN}/api/1/reuses/suggest/?q=ab&size=3"): (200, [{"id": "r1", "title": "T"}]),
                ("GET", f"{ORIGIN}/api/1/reuses/types/"): (200, [{"id": "application", "label": "Application"}]),
                ("GET", f"{ORIGIN}/api/1/reuses/topics/"): (200, [{"id": "health", "label": "Health"}]),
                ("GET", f"{ORIGIN}/api/2/reuses/?page=1&page_size=20"): (200, udata_page(_reuse())),
                ("GET", f"{ORIGIN}/api/2/reuses/search/?page=1&page_size=50&q=x"): (200, udata_page(_reuse())),
                ("GET", f"{ORIGIN}/api/1/reuses/reuse-1/followers/?page=1&page_size=20&user=u1"): (
                    200,
                    udata_page({"id": "f1", "follower": {"id": "u1"}, "since": "2026-01-01T00:00:00+00:00"}),
                ),
            }
        )
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        assert thawed(client.reuses.list_reuses().payload) == udata_page(_reuse())
        assert thawed(client.reuses.list_reuses(ReuseListQuery(q="x")).payload) == udata_page(_reuse())
        assert thawed(client.reuses.get_reuse("reuse-1").payload) == _reuse()
        atom = client.reuses.recent_reuses_atom_feed()
        assert atom.payload["media_type"] == "application/atom+xml"
        assert atom.payload["size_bytes"] == len("<feed/>")
        assert atom.payload["sha256"]
        assert thawed(client.reuses.available_reuse_badges().payload) == {"badger": "Badger"}
        assert thawed(client.reuses.suggest_reuses(ReuseSuggestQuery(q="ab", size=3))[0].payload) == {
            "id": "r1",
            "title": "T",
        }
        assert thawed(client.reuses.reuse_types()[0].payload) == {"id": "application", "label": "Application"}
        assert thawed(client.reuses.reuse_topics()[0].payload) == {"id": "health", "label": "Health"}
        assert thawed(client.reuses.list_v2().payload) == udata_page(_reuse())
        assert thawed(client.reuses.search_v2(ReuseSearchQuery(q="x")).payload) == udata_page(_reuse())
        followers = client.reuses.list_reuse_followers("reuse-1", ReuseFollowersQuery(user="u1"))
        assert cast(Mapping[str, object], thawed(followers.payload))["total"] == 1
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
    router = atom_sync_route_table(
        with_site_route(
            {
                ("POST", f"{ORIGIN}/api/1/reuses/"): (201, _reuse()),
                ("PUT", f"{ORIGIN}/api/1/reuses/reuse-1/"): (200, _reuse()),
            }
        )
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
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
    router = atom_sync_route_table(with_site_route({("DELETE", f"{ORIGIN}/api/1/reuses/reuse-1/"): (204, None)}))
    with sync_client(router, UDATA_CREDENTIAL) as client:
        deleted = client.reuses.delete_reuse(
            "reuse-1", DELETE_ONLY_PERMISSIONS, _policy(wire.DELETE_REUSE_OPERATION, "reuse-1")
        )
        assert deleted.receipt.target.value == "reuse-1"
        assert deleted.receipt.outcome == "succeeded"
        assert router.requests[-1].method == "DELETE"
        assert router.requests[-1].url == f"{ORIGIN}/api/1/reuses/reuse-1/"


def test_reuse_add_dataset_and_dataservice_match_exact_wire() -> None:
    router = atom_sync_route_table(
        with_site_route(
            {
                ("POST", f"{ORIGIN}/api/1/reuses/reuse-1/datasets/"): (201, _reuse()),
                ("POST", f"{ORIGIN}/api/1/reuses/reuse-1/dataservices/"): (201, _reuse()),
            }
        )
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
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
    router = atom_sync_route_table(
        with_site_route(
            {
                ("POST", f"{ORIGIN}/api/1/reuses/reuse-1/badges/"): (201, {"id": "badger", "label": "Badger"}),
                ("DELETE", f"{ORIGIN}/api/1/reuses/reuse-1/badges/badger/"): (204, None),
                ("POST", f"{ORIGIN}/api/1/reuses/reuse-1/featured/"): (200, _reuse(featured=True)),
                ("DELETE", f"{ORIGIN}/api/1/reuses/reuse-1/featured/"): (200, _reuse()),
            }
        )
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
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
    router = atom_sync_route_table(
        with_site_route({("POST", f"{ORIGIN}/api/1/reuses/reuse-1/image/"): (200, _reuse())})
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        result = client.reuses.reuse_image(
            "reuse-1", b"img-bytes", "image/png", PERMISSIONS, _policy(wire.REUSE_IMAGE_OPERATION, "reuse-1")
        )
        assert result.receipt.target.value == "reuse-1"
        assert router.requests[-1].method == "POST"
        assert router.requests[-1].url == f"{ORIGIN}/api/1/reuses/reuse-1/image/"


def test_reuse_follower_mutations_match_exact_wire() -> None:
    router = atom_sync_route_table(
        with_site_route(
            {
                ("POST", f"{ORIGIN}/api/1/reuses/reuse-1/followers/"): (201, {"followers": 2}),
                ("DELETE", f"{ORIGIN}/api/1/reuses/reuse-1/followers/"): (200, {"followers": 1}),
            }
        )
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        client.reuses.follow_reuse("reuse-1", PERMISSIONS, _policy(wire.FOLLOW_REUSE_OPERATION, "reuse-1"))
        client.reuses.unfollow_reuse("reuse-1", PERMISSIONS, _policy(wire.UNFOLLOW_REUSE_OPERATION, "reuse-1"))
    reuse_requests = [r for r in router.requests if "/reuses/" in r.url]
    assert reuse_requests[0].method == "POST"
    assert reuse_requests[0].url == f"{ORIGIN}/api/1/reuses/reuse-1/followers/"
    assert reuse_requests[1].method == "DELETE"
    assert reuse_requests[1].url == f"{ORIGIN}/api/1/reuses/reuse-1/followers/"


def test_reuse_mutation_failure_yields_redacted_receipt_with_exact_target() -> None:
    router = atom_sync_route_table(
        with_site_route({("DELETE", f"{ORIGIN}/api/1/reuses/reuse-1/"): (404, {"message": "Not found"})})
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        with pytest.raises(CatalogNotFoundError) as error:
            client.reuses.delete_reuse(
                "reuse-1", DELETE_ONLY_PERMISSIONS, _policy(wire.DELETE_REUSE_OPERATION, "reuse-1")
            )
        receipt = error.value.__dict__["mutation_receipt"]
        assert receipt.target.value == "reuse-1"
        assert receipt.outcome == "failed"


def test_reuse_async_mode_matches_sync_exact_wire() -> None:
    router = atom_async_route_table(
        with_site_route(
            {
                ("GET", f"{ORIGIN}/api/1/reuses/?page=1&page_size=20"): (200, udata_page(_reuse())),
                ("DELETE", f"{ORIGIN}/api/1/reuses/reuse-1/"): (204, None),
            }
        )
    )

    async def run() -> None:
        async with async_client(router, UDATA_CREDENTIAL) as client:
            page = await client.reuses.list_reuses()
            assert thawed(page.payload) == udata_page(_reuse())
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
