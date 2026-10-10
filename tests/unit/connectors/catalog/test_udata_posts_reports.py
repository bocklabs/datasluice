"""Exact wire and safety coverage for the uData post, report, and notification family."""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any, cast

import pytest

from datasluice.connectors.catalog.udata.models.posts_reports import (
    _REPORT_REASONS,
    NotificationQuery,
    PostCreateInput,
    PostListQuery,
    PostMutationResult,
    PostSearchQuery,
    PostUpdateInput,
    ReportCreateInput,
    ReportQuery,
    ReportUpdateInput,
    segment,
)
from datasluice.connectors.catalog.udata.services.posts_reports import (
    AsyncPostsReportsService,
    SyncPostsReportsService,
)
from datasluice.connectors.catalog.udata.wire import posts_reports as wire
from datasluice.errors.catalog import CatalogNotFoundError, CatalogValidationError
from tests.helpers.udata_test_support import (
    UDATA_ADMIN_PERMISSIONS,
    UDATA_CREDENTIAL,
    UDATA_ORIGIN,
    UDATA_PERMISSIONS,
    async_client,
    atom_async_route_table,
    atom_sync_route_table,
    mutation_policy,
    sync_client,
    thawing_payload,
    udata_page,
    with_site_route,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from datasluice.domain.catalog.auth import UDataCredential
    from datasluice.domain.catalog.models import MappingRecord
    from datasluice.runtime.transport.base import RuntimeRequest

ORIGIN = UDATA_ORIGIN
PERMISSIONS = UDATA_PERMISSIONS
ADMIN_PERMISSIONS = UDATA_ADMIN_PERMISSIONS
_REASON = sorted(_REPORT_REASONS)[0]
_SUBJECT = {"id": "subject-1", "class": "Dataset"}


def _post() -> dict[str, object]:
    return {
        "id": "post-1",
        "name": "A post",
        "slug": "a-post",
        "headline": "Headline",
        "content": "Content",
        "body_type": "markdown",
        "kind": "news",
        "tags": ["tag"],
        "datasets": [],
        "reuses": [],
        "owner": None,
        "published": None,
        "image": None,
        "image_url": None,
    }


def _report() -> dict[str, object]:
    return {
        "id": "report-1",
        "subject": {"class": "Dataset", "id": "dataset-1"},
        "reason": "spam",
        "message": "A message",
        "by": None,
        "dismissed_at": None,
        "dismissed_by": None,
        "subject_label": "A dataset",
    }


@pytest.mark.parametrize("identifier", [".", ".."])
def test_posts_reports_identifiers_reject_dot_segments(identifier: str) -> None:
    """A bare dot segment is removed by RFC 3986 resolution, retargeting the route."""
    with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
        segment(identifier, "get_post")


def test_posts_reports_contract_exposes_exactly_eighteen_methods_in_both_modes() -> None:
    expected = {
        "list_reports",
        "create_report",
        "get_report",
        "update_report",
        "list_reports_reasons",
        "list_posts",
        "create_post",
        "recent_posts_atom_feed",
        "get_post",
        "update_post",
        "delete_post",
        "publish_post",
        "unpublish_post",
        "post_image",
        "resize_post_image",
        "search_posts",
        "list_notifications",
        "read_notification",
    }
    for service in (SyncPostsReportsService, AsyncPostsReportsService):
        names = {name for name in dir(service) if not name.startswith("_") and callable(getattr(service, name))}
        assert names == expected
    assert len(expected) == 18
    assert len(wire.OPERATIONS) == 18


def test_posts_reports_wire_requests_match_pinned_routes() -> None:
    assert wire.list_posts_request(PostListQuery(q="x", kind="news", with_drafts=True)) == (
        "GET",
        "/api/1/posts/?page=1&page_size=20&q=x&kind=news&with_drafts=true",
        {},
        None,
    )
    assert wire.create_post_request(PostCreateInput(name="A post", content="Content")) == (
        "POST",
        "/api/1/posts/",
        {},
        {"name": "A post", "content": "Content"},
    )
    assert wire.recent_posts_atom_feed_request() == ("GET", "/api/1/posts/recent.atom", {}, None)
    assert wire.get_post_request("post-1") == ("GET", "/api/1/posts/post-1/", {}, None)
    assert wire.update_post_request("post-1", PostUpdateInput(name="Next")) == (
        "PUT",
        "/api/1/posts/post-1/",
        {},
        {"name": "Next"},
    )
    assert wire.delete_post_request("post-1") == ("DELETE", "/api/1/posts/post-1/", {}, None)
    assert wire.publish_post_request("post-1") == ("POST", "/api/1/posts/post-1/publish/", {}, None)
    assert wire.unpublish_post_request("post-1") == ("DELETE", "/api/1/posts/post-1/publish/", {}, None)
    assert wire.post_image_request("post-1") == ("POST", "/api/1/posts/post-1/image/", {})
    assert wire.resize_post_image_request("post-1") == ("PUT", "/api/1/posts/post-1/image/", {})
    assert wire.search_posts_request(PostSearchQuery(q="x")) == (
        "GET",
        "/api/2/posts/search/?page=1&page_size=20&q=x",
        {},
        None,
    )
    assert wire.list_reports_request(ReportQuery(handled=False)) == (
        "GET",
        "/api/1/reports/?page=1&page_size=20&handled=false",
        {},
        None,
    )
    assert wire.create_report_request(ReportCreateInput(subject={"class": "Dataset", "id": "d1"}, reason="spam")) == (
        "POST",
        "/api/1/reports/",
        {},
        {"subject": {"class": "Dataset", "id": "d1"}, "reason": "spam"},
    )
    assert wire.get_report_request("report-1") == ("GET", "/api/1/reports/report-1/", {}, None)
    assert wire.update_report_request("report-1", ReportUpdateInput(message="Next")) == (
        "PATCH",
        "/api/1/reports/report-1/",
        {},
        {"message": "Next"},
    )
    assert wire.list_reports_reasons_request() == ("GET", "/api/1/reports/reasons/", {}, None)
    assert wire.list_notifications_request(NotificationQuery(handled=True)) == (
        "GET",
        "/api/1/notifications/?page=1&page_size=20&handled=true",
        {},
        None,
    )
    assert wire.read_notification_request("notification-1") == (
        "POST",
        "/api/1/notifications/notification-1/read/",
        {},
        None,
    )


def test_post_reads_decode_native_pages_and_documents() -> None:
    router = atom_sync_route_table(
        with_site_route(
            {
                ("GET", f"{ORIGIN}/api/1/posts/?page=1&page_size=20&q=x"): (200, udata_page(_post())),
                ("GET", f"{ORIGIN}/api/1/posts/post-1/"): (200, _post()),
                ("GET", f"{ORIGIN}/api/1/posts/recent.atom"): (200, b"<feed/>"),
                ("GET", f"{ORIGIN}/api/2/posts/search/?page=1&page_size=20&q=x"): (200, udata_page(_post())),
                ("GET", f"{ORIGIN}/api/1/reports/?page=1&page_size=20"): (200, udata_page(_report())),
                ("GET", f"{ORIGIN}/api/1/reports/report-1/"): (200, _report()),
                ("GET", f"{ORIGIN}/api/1/reports/reasons/"): (200, [{"value": "spam", "label": "Spam"}]),
                ("GET", f"{ORIGIN}/api/1/notifications/?page=1&page_size=20"): (200, udata_page(_report())),
            }
        )
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        assert thawing_payload(client.posts_reports.list_posts(PostListQuery(q="x")).payload) == udata_page(_post())
        assert thawing_payload(client.posts_reports.get_post("post-1").payload) == _post()
        atom = client.posts_reports.recent_posts_atom_feed()
        assert atom.payload["media_type"] == "application/atom+xml"
        assert thawing_payload(client.posts_reports.search_posts(PostSearchQuery(q="x")).payload) == udata_page(_post())
        assert thawing_payload(client.posts_reports.list_reports().payload) == udata_page(_report())
        assert thawing_payload(client.posts_reports.get_report("report-1").payload) == _report()
        reasons: tuple[MappingRecord, ...] = client.posts_reports.list_reports_reasons()
        assert cast("Mapping[str, object]", thawing_payload(reasons[0].payload))["value"] == "spam"
        assert thawing_payload(client.posts_reports.list_notifications(PERMISSIONS).payload) == udata_page(_report())
    post_requests = [r for r in router.requests if "/posts/" in r.url]
    assert post_requests[0].method == "GET"
    assert post_requests[0].url == f"{ORIGIN}/api/1/posts/?page=1&page_size=20&q=x"


def test_post_mutations_match_exact_wire_and_receipt_targets() -> None:
    router = atom_sync_route_table(
        with_site_route(
            {
                ("POST", f"{ORIGIN}/api/1/posts/"): (201, _post()),
                ("PUT", f"{ORIGIN}/api/1/posts/post-1/"): (200, _post()),
                ("DELETE", f"{ORIGIN}/api/1/posts/post-1/"): (204, None),
                ("POST", f"{ORIGIN}/api/1/posts/post-1/publish/"): (200, _post()),
                ("DELETE", f"{ORIGIN}/api/1/posts/post-1/publish/"): (200, _post()),
                ("POST", f"{ORIGIN}/api/1/posts/post-1/image/"): (200, _post()),
                ("PUT", f"{ORIGIN}/api/1/posts/post-1/image/"): (200, _post()),
            }
        )
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        created: PostMutationResult = client.posts_reports.create_post(
            PostCreateInput(name="A post", content="Content"),
            ADMIN_PERMISSIONS,
            mutation_policy(wire.CREATE_POST_OPERATION, "A post"),
        )
        updated = client.posts_reports.update_post(
            "post-1",
            PostUpdateInput(name="Next"),
            ADMIN_PERMISSIONS,
            mutation_policy(wire.UPDATE_POST_OPERATION, "post-1"),
        )
        deleted = client.posts_reports.delete_post(
            "post-1", ADMIN_PERMISSIONS, mutation_policy(wire.DELETE_POST_OPERATION, "post-1", destructive=True)
        )
        published = client.posts_reports.publish_post(
            "post-1", ADMIN_PERMISSIONS, mutation_policy(wire.PUBLISH_POST_OPERATION, "post-1")
        )
        unpublished = client.posts_reports.unpublish_post(
            "post-1", ADMIN_PERMISSIONS, mutation_policy(wire.UNPUBLISH_POST_OPERATION, "post-1")
        )
        image = client.posts_reports.post_image(
            "post-1", b"img-bytes", "image/png", ADMIN_PERMISSIONS, mutation_policy(wire.POST_IMAGE_OPERATION, "post-1")
        )
        resized = client.posts_reports.resize_post_image(
            "post-1",
            b"img-bytes",
            "image/png",
            ADMIN_PERMISSIONS,
            mutation_policy(wire.RESIZE_POST_IMAGE_OPERATION, "post-1"),
        )
        for result in (created, updated, deleted, published, unpublished, image, resized):
            assert result.receipt.outcome == "succeeded"
        assert created.receipt.target.value == "post-1"
        assert deleted.receipt.target.value == "post-1"
    post_requests = [r for r in router.requests if "/posts/" in r.url]
    assert [(r.method, r.url.replace(ORIGIN, "")) for r in post_requests] == [
        ("POST", "/api/1/posts/"),
        ("PUT", "/api/1/posts/post-1/"),
        ("DELETE", "/api/1/posts/post-1/"),
        ("POST", "/api/1/posts/post-1/publish/"),
        ("DELETE", "/api/1/posts/post-1/publish/"),
        ("POST", "/api/1/posts/post-1/image/"),
        ("PUT", "/api/1/posts/post-1/image/"),
    ]
    assert json.loads(post_requests[0].body or b"{}") == {"name": "A post", "content": "Content"}
    assert dict(post_requests[5].headers).get("X-API-KEY") == "secret-key"


def test_anonymous_report_create_matches_stock_public_contract() -> None:
    router = atom_sync_route_table(with_site_route({("POST", f"{ORIGIN}/api/1/reports/"): (201, _report())}))
    with sync_client(router, None) as client:
        created = client.posts_reports.create_report(
            ReportCreateInput(subject={"class": "Dataset", "id": "dataset-1"}, reason="spam"),
            None,
            mutation_policy(wire.CREATE_REPORT_OPERATION, "dataset-1"),
        )
    assert created.receipt.outcome == "succeeded"
    assert created.receipt.target.value == "report-1"
    (request,) = (r for r in router.requests if r.method == "POST")
    assert "X-API-KEY" not in dict(request.headers)
    assert json.loads(request.body or b"{}") == {"subject": {"class": "Dataset", "id": "dataset-1"}, "reason": "spam"}


class _AsyncOnlyResolver:
    def __init__(self, credential: UDataCredential) -> None:
        self._credential = credential

    async def resolve_async(self) -> UDataCredential:
        return self._credential


_REPORT_CREATE = ReportCreateInput(subject={"class": "Dataset", "id": "dataset-1"}, reason="spam")


def _report_route_table(atomic: bool):
    table = atom_async_route_table if atomic else atom_sync_route_table
    return table(with_site_route({("POST", f"{ORIGIN}/api/1/reports/"): (201, _report())}))


async def _create_report_async(credentials: object | None):
    router = _report_route_table(atomic=True)
    async with async_client(router, credentials) as client:
        created = await client.posts_reports.create_report(
            _REPORT_CREATE,
            None,
            mutation_policy(wire.CREATE_REPORT_OPERATION, "dataset-1"),
        )
    return router, created


def _sole_report_post(router) -> RuntimeRequest:
    (request,) = (r for r in router.requests if r.method == "POST")
    return request


def test_async_report_create_resolves_credentials_through_the_async_resolver() -> None:
    router, created = asyncio.run(_create_report_async(_AsyncOnlyResolver(UDATA_CREDENTIAL)))

    assert created.receipt.outcome == "succeeded"
    assert dict(_sole_report_post(router).headers).get("X-API-KEY") == "secret-key"


def test_attributed_report_create_still_requires_matching_permission_evidence() -> None:
    router = _report_route_table(atomic=False)
    with sync_client(router, UDATA_CREDENTIAL) as client:
        created = client.posts_reports.create_report(
            _REPORT_CREATE,
            PERMISSIONS,
            mutation_policy(wire.CREATE_REPORT_OPERATION, "dataset-1"),
        )
    assert created.receipt.outcome == "succeeded"
    assert dict(_sole_report_post(router).headers).get("X-API-KEY") == "secret-key"


def test_anonymous_report_create_matches_stock_public_contract_async() -> None:
    router, created = asyncio.run(_create_report_async(None))

    assert created.receipt.outcome == "succeeded"
    assert "X-API-KEY" not in dict(_sole_report_post(router).headers)


def test_report_and_notification_mutations_match_exact_wire_and_receipt_targets() -> None:
    router = atom_sync_route_table(
        with_site_route(
            {
                ("POST", f"{ORIGIN}/api/1/reports/"): (201, _report()),
                ("PATCH", f"{ORIGIN}/api/1/reports/report-1/"): (200, _report()),
                ("POST", f"{ORIGIN}/api/1/notifications/notification-1/read/"): (200, _report()),
            }
        )
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        created = client.posts_reports.create_report(
            ReportCreateInput(subject={"class": "Dataset", "id": "dataset-1"}, reason="spam"),
            PERMISSIONS,
            mutation_policy(wire.CREATE_REPORT_OPERATION, "dataset-1"),
        )
        updated = client.posts_reports.update_report(
            "report-1",
            ReportUpdateInput(message="Next"),
            ADMIN_PERMISSIONS,
            mutation_policy(wire.UPDATE_REPORT_OPERATION, "report-1"),
        )
        read = client.posts_reports.read_notification(
            "notification-1", PERMISSIONS, mutation_policy(wire.READ_NOTIFICATION_OPERATION, "notification-1")
        )
        assert created.receipt.target.value == "report-1"
        assert updated.receipt.target.value == "report-1"
        assert read.receipt.target.value == "notification-1"


def test_post_image_rejects_invalid_upload_before_dispatch() -> None:
    router = atom_sync_route_table(with_site_route({}))
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(ValueError):
        client.posts_reports.post_image(
            "post-1",
            cast("bytes", "not-bytes"),
            "image/png",
            ADMIN_PERMISSIONS,
            mutation_policy(wire.POST_IMAGE_OPERATION, "post-1"),
        )
    assert router.requests == []


def test_mutation_failure_carries_exact_target_receipt() -> None:
    router = atom_sync_route_table(
        with_site_route({("DELETE", f"{ORIGIN}/api/1/posts/post-1/"): (404, {"message": "Not found"})})
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        with pytest.raises(CatalogNotFoundError) as error:
            client.posts_reports.delete_post(
                "post-1", ADMIN_PERMISSIONS, mutation_policy(wire.DELETE_POST_OPERATION, "post-1", destructive=True)
            )
        assert error.value.__dict__["mutation_receipt"].target.value == "post-1"


def test_async_mode_matches_exact_wire() -> None:
    router = atom_async_route_table(
        with_site_route(
            {
                ("GET", f"{ORIGIN}/api/2/posts/search/?page=1&page_size=20&q=x"): (200, udata_page(_post())),
                ("POST", f"{ORIGIN}/api/1/reports/"): (201, _report()),
                ("POST", f"{ORIGIN}/api/1/notifications/notification-1/read/"): (200, _report()),
            }
        )
    )

    async def run() -> None:
        async with async_client(router, UDATA_CREDENTIAL) as client:
            page = await client.posts_reports.search_posts(PostSearchQuery(q="x"))
            assert thawing_payload(page.payload) == udata_page(_post())
            created = await client.posts_reports.create_report(
                ReportCreateInput(subject={"class": "Dataset", "id": "dataset-1"}, reason="spam"),
                PERMISSIONS,
                mutation_policy(wire.CREATE_REPORT_OPERATION, "dataset-1"),
            )
            read = await client.posts_reports.read_notification(
                "notification-1", PERMISSIONS, mutation_policy(wire.READ_NOTIFICATION_OPERATION, "notification-1")
            )
            assert created.receipt.target.value == "report-1"
            assert read.receipt.target.value == "notification-1"

    asyncio.run(run())
    assert [r.url for r in router.requests if "/site/" not in r.url] == [
        f"{ORIGIN}/api/2/posts/search/?page=1&page_size=20&q=x",
        f"{ORIGIN}/api/1/reports/",
        f"{ORIGIN}/api/1/notifications/notification-1/read/",
    ]


_NESTED = {"on_success": {"retry": {"count": 3}}, "tags": ["a", "b"]}


@pytest.mark.parametrize(
    ("factory", "field"),
    [
        (lambda value: ReportCreateInput(reason=_REASON, subject=_SUBJECT, callbacks=value), "callbacks"),
        (lambda value: ReportUpdateInput(reason=_REASON, callbacks=value), "callbacks"),
        (lambda value: ReportCreateInput(reason=_REASON, subject=_SUBJECT, dismissed_by=value), "dismissed_by"),
    ],
    ids=["create-callbacks", "update-callbacks", "create-dismissed-by"],
)
def test_frozen_report_mappings_stay_json_serializable(factory: Any, field: str) -> None:
    """Frozen mappings must thaw back to plain JSON in payload().

    A nested value raised TypeError from json.dumps once the mapping was frozen
    without thawing, so the deep case is pinned explicitly rather than only the
    flat one.
    """
    encoded = factory(_NESTED).payload()

    assert json.loads(json.dumps(encoded, allow_nan=False))[field] == _NESTED


@pytest.mark.parametrize("bad", [{"when": object()}, {"nested": {"deep": object()}}])
def test_report_mappings_reject_non_json_safe_values(bad: Mapping[str, object]) -> None:
    with pytest.raises(ValueError, match="JSON-safe values only"):
        ReportCreateInput(reason=_REASON, subject=_SUBJECT, callbacks=bad)
