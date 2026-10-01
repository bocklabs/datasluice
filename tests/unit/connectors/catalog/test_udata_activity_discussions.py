"""Exact wire and safety coverage for the uData activity and discussion family.

Expectations are transcribed from the pinned uData 17.6.0 source
(`udata.core.activity.api`, `udata.core.disccussions.api`, and
`udata.core.discussions.apiv2` at commit 0546582), not from the
production request builders.
"""

from __future__ import annotations

import asyncio
import json
from typing import cast

import pytest

from datasluice.connectors.catalog.udata.models.activity_discussions import (
    ActivityQuery,
    CommentInput,
    DiscussionCreateInput,
    DiscussionMutationResult,
    DiscussionSearchQuery,
    DiscussionUpdateInput,
    segment,
)
from datasluice.connectors.catalog.udata.services.activity_discussions import (
    AsyncActivityDiscussionsService,
    SyncActivityDiscussionsService,
)
from datasluice.connectors.catalog.udata.wire import activity_discussions as wire
from datasluice.contracts.catalog.native.udata import (
    AsyncUDataActivityDiscussionsService,
    SyncUDataActivityDiscussionsService,
)
from datasluice.errors.catalog import CatalogValidationError, ForbiddenError, NativeCatalogError
from tests.helpers.udata_test_support import (
    UDATA_CREDENTIAL,
    UDATA_ORIGIN,
    UDATA_PERMISSIONS,
    RouteTable,
    async_client,
    async_route_table,
    destructive_operations_policy,
    scoped_permissions,
    sync_client,
    sync_route_table,
    thawed,
    with_site_route,
)

ORIGIN = UDATA_ORIGIN
PERMISSIONS = UDATA_PERMISSIONS
CREATE_ONLY_PERMISSIONS = scoped_permissions("create-discussion", wire.CREATE_DISCUSSION_OPERATION)
DELETE_ONLY_PERMISSIONS = scoped_permissions("delete-discussion", wire.DELETE_DISCUSSION_OPERATION)
_policy = destructive_operations_policy(
    frozenset({wire.DELETE_DISCUSSION_OPERATION, wire.DELETE_DISCUSSION_COMMENT_OPERATION})
)


@pytest.mark.parametrize("identifier", [".", ".."])
def test_activity_discussions_identifiers_reject_dot_segments(identifier: str) -> None:
    """A bare dot segment is removed by RFC 3986 resolution, retargeting the route."""
    with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
        segment(identifier, "get_discussion")


def test_activity_discussion_contract_exposes_every_assigned_method_in_both_modes() -> None:
    sync_names = {
        name
        for name in dir(SyncActivityDiscussionsService)
        if not name.startswith("_") and callable(getattr(SyncActivityDiscussionsService, name))
    }
    async_names = {
        name
        for name in dir(AsyncActivityDiscussionsService)
        if not name.startswith("_") and callable(getattr(AsyncActivityDiscussionsService, name))
    }
    assert sync_names == async_names
    assert sync_names == {
        "activity",
        "list_discussions",
        "get_discussion",
        "create_discussion",
        "comment_discussion",
        "update_discussion",
        "delete_discussion",
        "edit_discussion_comment",
        "delete_discussion_comment",
        "search_discussions",
    }
    with sync_client(sync_route_table(with_site_route({})), None) as client:
        assert isinstance(client.activity_discussions, SyncUDataActivityDiscussionsService)

    async def run() -> None:
        async with async_client(async_route_table(with_site_route({})), None) as client:
            assert isinstance(client.activity_discussions, AsyncUDataActivityDiscussionsService)

    asyncio.run(run())


def test_every_activity_discussion_route_has_an_exact_wire_shape() -> None:
    actual = [
        wire.activity_request(ActivityQuery())[0:2],
        wire.list_discussions_request()[0:2],
        wire.get_discussion_request("discussion-1")[0:2],
        wire.create_discussion_request(
            DiscussionCreateInput(title="t", comment="c", subject={"id": "d", "class": "Dataset"})
        )[0:2],
        wire.comment_discussion_request("discussion-1", CommentInput(comment="hello"))[0:2],
        wire.update_discussion_request("discussion-1", DiscussionUpdateInput(title="new"))[0:2],
        wire.delete_discussion_request("discussion-1")[0:2],
        wire.edit_discussion_comment_request("discussion-1", "0", CommentInput(comment="edited"))[0:2],
        wire.delete_discussion_comment_request("discussion-1", "0")[0:2],
        wire.search_discussions_request(DiscussionSearchQuery())[0:2],
    ]
    assert actual == [
        ("GET", "/api/1/activity/"),
        ("GET", "/api/1/discussions/"),
        ("GET", "/api/1/discussions/discussion-1/"),
        ("POST", "/api/1/discussions/"),
        ("POST", "/api/1/discussions/discussion-1/"),
        ("PUT", "/api/1/discussions/discussion-1/"),
        ("DELETE", "/api/1/discussions/discussion-1/"),
        ("PUT", "/api/1/discussions/discussion-1/comments/0/"),
        ("DELETE", "/api/1/discussions/discussion-1/comments/0/"),
        ("GET", "/api/2/discussions/search/?page=1&page_size=20"),
    ]


def test_discussion_identifiers_are_url_encoded_and_rejected_before_dispatch() -> None:
    assert wire.get_discussion_request("a b")[0:2] == ("GET", "/api/1/discussions/a%20b/")
    for builder in (
        lambda: wire.get_discussion_request("../secret"),
        lambda: wire.get_discussion_request(""),
        lambda: wire.delete_discussion_request("discussion-1/../x"),
        lambda: wire.edit_discussion_comment_request("discussion-1", "0/../x", CommentInput(comment="e")),
    ):
        with pytest.raises(CatalogValidationError):
            builder()


def test_activity_filters_encode_only_supplied_keys() -> None:
    assert wire.activity_request(ActivityQuery())[1] == "/api/1/activity/"
    assert (
        wire.activity_request(ActivityQuery(user="u1", organization="org1", related_to="abc"))[1]
        == "/api/1/activity/?organization=org1&user=u1&related_to=abc"
    )
    assert wire.activity_request(ActivityQuery(user="u1"))[1] == "/api/1/activity/?user=u1"


def test_create_discussion_body_omits_absent_optional_keys() -> None:
    method, path, headers, body = wire.create_discussion_request(
        DiscussionCreateInput(title="t", comment="c", subject={"id": "d", "class": "Dataset"})
    )
    assert method == "POST"
    assert path == "/api/1/discussions/"
    assert headers == {}
    assert body == {"title": "t", "comment": "c", "subject": {"id": "d", "class": "Dataset"}}


def test_comment_close_and_organization_keys_are_explicit() -> None:
    _, _, _, close_only = wire.comment_discussion_request("discussion-1", CommentInput(close=True))
    assert close_only == {"close": True}
    _, _, _, with_org = wire.comment_discussion_request(
        "discussion-1", CommentInput(comment="hi", organization={"id": "org-1"})
    )
    assert with_org == {"comment": "hi", "organization": {"id": "org-1"}}
    with pytest.raises(ValueError, match="close"):
        CommentInput()
    org_only = wire.comment_discussion_request("discussion-1", CommentInput(organization={"id": "org-1"}))[3]
    assert org_only == {"organization": {"id": "org-1"}}


def test_update_and_edit_comment_bodies_carry_only_the_documented_key() -> None:
    assert wire.update_discussion_request("d1", DiscussionUpdateInput(title="new"))[3] == {"title": "new"}
    assert wire.edit_discussion_comment_request("d1", "0", CommentInput(comment="edited"))[3] == {"comment": "edited"}


def test_activity_discussion_wire_reaches_transport_with_exact_verb_path_and_body() -> None:
    router = sync_route_table(
        with_site_route({("POST", f"{ORIGIN}/api/1/discussions/"): (201, {"id": "discussion-1", "title": "t"})})
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        client.activity_discussions.create_discussion(
            DiscussionCreateInput(title="t", comment="c", subject={"id": "d", "class": "Dataset"}),
            CREATE_ONLY_PERMISSIONS,
            _policy(wire.CREATE_DISCUSSION_OPERATION, "d"),
        )
    request = router.requests[-1]
    assert request.method == "POST"
    assert request.url == f"{ORIGIN}/api/1/discussions/"
    assert dict(request.headers) == {"X-API-KEY": "secret-key", "Content-Type": "application/json"}
    assert json.loads(request.body or b"{}") == {
        "title": "t",
        "comment": "c",
        "subject": {"id": "d", "class": "Dataset"},
    }


def test_activity_and_discussion_reads_preserve_native_envelopes() -> None:
    activity_page = {
        "page": 1,
        "page_size": 2,
        "total": 2,
        "items": [
            {
                "id": "activity-1",
                "label": "dataset.created",
                "key": "dataset",
                "icon": "fa-plus",
                "related_to": "Dataset",
                "related_to_id": "d1",
                "related_to_kind": "Dataset",
                "related_to_url": "http://127.0.0.1:5640/api/1/datasets/d1/",
                "created_at": "2026-01-01T00:00:00+00:00",
                "actor": {"id": "user-1"},
                "organization": None,
                "changes": [],
                "extras": {},
            }
        ],
    }
    discussion = {
        "id": "discussion-1",
        "title": "A question",
        "user": {"id": "user-1"},
        "organization": None,
        "subject": {"class": "Dataset", "id": "d1"},
        "discussion": ({"content": "first", "posted_by": {"id": "user-1"}},),
        "created": "2026-01-01T00:00:00+00:00",
        "closed": None,
        "closed_by": None,
        "closed_by_organization": None,
        "extras": {},
    }
    list_page = {"page": 1, "page_size": 20, "total": 1, "items": [discussion]}
    search_page = {
        "data": [discussion],
        "facets": {"closed": {"true": 1}},
        "links": {"self": "/api/2/discussions/search/?q="},
        "meta": {"total": 1, "page": 1, "page_size": 20},
    }
    routes: RouteTable = {
        ("GET", f"{ORIGIN}/api/1/activity/"): (200, activity_page),
        ("GET", f"{ORIGIN}/api/1/activity/?user=u1"): (200, activity_page),
        ("GET", f"{ORIGIN}/api/1/discussions/"): (200, list_page),
        ("GET", f"{ORIGIN}/api/1/discussions/discussion-1/"): (200, discussion),
        ("GET", f"{ORIGIN}/api/2/discussions/search/?page=1&page_size=20"): (200, search_page),
    }
    with sync_client(sync_route_table(with_site_route(routes)), None) as client:
        service = client.activity_discussions
        assert thawed(service.activity(ActivityQuery()).payload) == thawed(activity_page)
        assert thawed(service.list_discussions().payload) == thawed(list_page)
        assert thawed(service.get_discussion("discussion-1").payload) == thawed(discussion)
        assert thawed(service.search_discussions(DiscussionSearchQuery()).payload) == thawed(search_page)

    async def run() -> None:
        async with async_client(async_route_table(with_site_route(routes)), None) as client:
            assert thawed((await client.activity_discussions.get_discussion("discussion-1")).payload) == thawed(
                discussion
            )
            assert thawed((await client.activity_discussions.activity(ActivityQuery(user="u1"))).payload) is not None

    asyncio.run(run())


def test_search_query_encodes_only_supplied_filters_plus_stock_paging() -> None:
    assert (
        wire.search_discussions_request(DiscussionSearchQuery())[1] == "/api/2/discussions/search/?page=1&page_size=20"
    )
    assert (
        wire.search_discussions_request(DiscussionSearchQuery(q="dataset", closed=True, page_size=5))[1]
        == "/api/2/discussions/search/?q=dataset&closed=true&page=1&page_size=5"
    )
    assert (
        wire.search_discussions_request(
            DiscussionSearchQuery(sort="-created", subject_ids=("d1", "d2"), last_update_range="last_30_days")
        )[1]
        == "/api/2/discussions/search/?for=d1&for=d2&last_update_range=last_30_days&sort=-created&page=1&page_size=20"
    )
    with pytest.raises(ValueError, match="sort"):
        DiscussionSearchQuery(sort="nope")
    with pytest.raises(ValueError, match="last_update_range"):
        DiscussionSearchQuery(last_update_range="last_decade")


def test_invalid_inputs_fail_before_dispatch_without_a_raw_body() -> None:
    with pytest.raises(ValueError, match="title"):
        DiscussionCreateInput(title="", comment="c", subject={"id": "d", "class": "Dataset"})
    with pytest.raises(ValueError, match="subject"):
        DiscussionCreateInput(title="t", comment="c", subject={})
    with pytest.raises(ValueError, match="comment"):
        CommentInput(comment="")
    with pytest.raises(ValueError, match="title"):
        DiscussionUpdateInput(title="")
    with pytest.raises(CatalogValidationError):
        wire.parse_mapping(["not", "an", "object"], operation=wire.ACTIVITY_OPERATION)
    with pytest.raises(CatalogValidationError):
        wire.parse_mapping("not-a-document", operation=wire.SEARCH_DISCUSSIONS_OPERATION)


def test_discussion_mutations_return_redacted_receipts_with_exact_targets() -> None:
    discussion = {"id": "discussion-1", "title": "t", "discussion": ()}
    routes: RouteTable = {
        ("POST", f"{ORIGIN}/api/1/discussions/discussion-1/"): (200, discussion),
        ("PUT", f"{ORIGIN}/api/1/discussions/discussion-1/"): (200, {**discussion, "title": "new"}),
        ("PUT", f"{ORIGIN}/api/1/discussions/discussion-1/comments/0/"): (200, discussion),
        ("DELETE", f"{ORIGIN}/api/1/discussions/discussion-1/"): (204, None),
        ("DELETE", f"{ORIGIN}/api/1/discussions/discussion-1/comments/0/"): (204, None),
    }
    with sync_client(sync_route_table(with_site_route(routes)), UDATA_CREDENTIAL) as client:
        service = client.activity_discussions
        commented = service.comment_discussion(
            "discussion-1",
            CommentInput(comment="hello"),
            PERMISSIONS,
            _policy(wire.COMMENT_DISCUSSION_OPERATION, "discussion-1"),
        )
        assert isinstance(commented, DiscussionMutationResult)
        assert commented.record is not None
        assert commented.record.payload == discussion
        edited = service.update_discussion(
            "discussion-1",
            DiscussionUpdateInput(title="new"),
            PERMISSIONS,
            _policy(wire.UPDATE_DISCUSSION_OPERATION, "discussion-1"),
        )
        comment_edited = service.edit_discussion_comment(
            "discussion-1",
            "0",
            CommentInput(comment="edited"),
            PERMISSIONS,
            _policy(wire.EDIT_DISCUSSION_COMMENT_OPERATION, "discussion-1:0"),
        )
        deleted = service.delete_discussion(
            "discussion-1",
            PERMISSIONS,
            _policy(wire.DELETE_DISCUSSION_OPERATION, "discussion-1"),
        )
        comment_deleted = service.delete_discussion_comment(
            "discussion-1",
            "0",
            PERMISSIONS,
            _policy(wire.DELETE_DISCUSSION_COMMENT_OPERATION, "discussion-1:0"),
        )
    assert deleted.record is None
    assert comment_deleted.record is None
    assert commented.receipt.operation == wire.COMMENT_DISCUSSION_OPERATION
    assert edited.receipt.operation == wire.UPDATE_DISCUSSION_OPERATION
    assert comment_edited.receipt.operation == wire.EDIT_DISCUSSION_COMMENT_OPERATION
    assert deleted.receipt.operation == wire.DELETE_DISCUSSION_OPERATION
    assert comment_deleted.receipt.operation == wire.DELETE_DISCUSSION_COMMENT_OPERATION
    metadata = cast(dict[str, object], comment_edited.receipt.to_dict()["audit_metadata"])
    assert metadata["target_valid"] is True
    assert b"secret-key" not in json.dumps(comment_edited.receipt.to_dict()).encode()


def test_discussion_permissions_discriminate_create_from_delete_in_both_modes() -> None:
    routes = with_site_route(
        {
            ("POST", f"{ORIGIN}/api/1/discussions/"): (201, {"id": "discussion-1"}),
            ("DELETE", f"{ORIGIN}/api/1/discussions/discussion-1/"): (204, None),
        }
    )
    router = sync_route_table(routes)
    create_input = DiscussionCreateInput(title="t", comment="c", subject={"id": "d", "class": "Dataset"})

    def run_sync() -> None:
        with sync_client(router, UDATA_CREDENTIAL) as client:
            with pytest.raises(ForbiddenError) as denied_create:
                client.activity_discussions.create_discussion(create_input, DELETE_ONLY_PERMISSIONS)
            with pytest.raises(ForbiddenError) as denied_delete:
                client.activity_discussions.delete_discussion("discussion-1", CREATE_ONLY_PERMISSIONS)
            assert denied_create.value.operation == wire.CREATE_DISCUSSION_OPERATION
            assert denied_delete.value.operation == wire.DELETE_DISCUSSION_OPERATION
            assert router.requests == []

    async def run_async() -> None:
        async with async_client(async_route_table(routes), UDATA_CREDENTIAL) as client:
            with pytest.raises(ForbiddenError) as denied_create:
                await client.activity_discussions.create_discussion(create_input, DELETE_ONLY_PERMISSIONS)
            with pytest.raises(ForbiddenError) as denied_delete:
                await client.activity_discussions.delete_discussion("discussion-1", CREATE_ONLY_PERMISSIONS)
            assert denied_create.value.operation == wire.CREATE_DISCUSSION_OPERATION
            assert denied_delete.value.operation == wire.DELETE_DISCUSSION_OPERATION
            assert router.requests == []

    run_sync()
    asyncio.run(run_async())


def test_every_discussion_mutation_dispatches_under_its_own_operation(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    def capture(**kwargs: object) -> tuple[int, object, object]:
        calls.append(kwargs)
        status = 204 if kwargs["method"] == "DELETE" else 200
        return status, None if status == 204 else {"id": "discussion-1"}, object()

    async def capture_async(**kwargs: object) -> tuple[int, object, object]:
        return capture(**kwargs)

    router = sync_route_table(with_site_route({}))
    with sync_client(router, UDATA_CREDENTIAL) as client:
        monkeypatch.setattr(client, "_dataset_call", capture)
        service = client.activity_discussions
        service.comment_discussion(
            "discussion-1",
            CommentInput(comment="c"),
            PERMISSIONS,
            _policy(wire.COMMENT_DISCUSSION_OPERATION, "discussion-1"),
        )
        service.update_discussion(
            "discussion-1",
            DiscussionUpdateInput(title="t"),
            PERMISSIONS,
            _policy(wire.UPDATE_DISCUSSION_OPERATION, "discussion-1"),
        )
        service.edit_discussion_comment(
            "discussion-1",
            "0",
            CommentInput(comment="c"),
            PERMISSIONS,
            _policy(wire.EDIT_DISCUSSION_COMMENT_OPERATION, "discussion-1:0"),
        )
        service.delete_discussion(
            "discussion-1", PERMISSIONS, _policy(wire.DELETE_DISCUSSION_OPERATION, "discussion-1")
        )
        service.delete_discussion_comment(
            "discussion-1", "0", PERMISSIONS, _policy(wire.DELETE_DISCUSSION_COMMENT_OPERATION, "discussion-1:0")
        )

    async def run_async() -> None:
        async with async_client(async_route_table(with_site_route({})), UDATA_CREDENTIAL) as client:
            monkeypatch.setattr(client, "_dataset_call_async", capture_async)
            await client.activity_discussions.comment_discussion(
                "discussion-1",
                CommentInput(comment="c"),
                PERMISSIONS,
                _policy(wire.COMMENT_DISCUSSION_OPERATION, "discussion-1"),
            )

    asyncio.run(run_async())
    dispatched_operations = [call["owning_operation"] for call in calls]
    assert dispatched_operations[:5] == [
        wire.COMMENT_DISCUSSION_OPERATION,
        wire.UPDATE_DISCUSSION_OPERATION,
        wire.EDIT_DISCUSSION_COMMENT_OPERATION,
        wire.DELETE_DISCUSSION_OPERATION,
        wire.DELETE_DISCUSSION_COMMENT_OPERATION,
    ]
    assert dispatched_operations[5:] == [wire.COMMENT_DISCUSSION_OPERATION]
    assert len(set(dispatched_operations)) == 5


def test_non_finite_decoding_fails_typed_without_raw_body() -> None:
    nan_site = (
        b'{"version": "17.6.0", "id": "site", "title": "uData", "feed_size": 0, '
        b'"keywords": [], "metrics": {"widgets": NaN}}'
    )
    with pytest.raises(NativeCatalogError) as raised:
        with sync_client(
            sync_route_table(
                {
                    ("GET", f"{ORIGIN}/api/1/site/"): (200, nan_site),
                    ("GET", f"{ORIGIN}/api/1/activity/"): (200, nan_site),
                }
            ),
            None,
        ) as client:
            client.activity_discussions.activity(ActivityQuery())
    assert "NaN" not in repr(raised.value) + str(raised.value.__dict__)


def test_pre_dispatch_rejection_carries_a_redacted_receipt_and_dispatches_nothing() -> None:
    router = sync_route_table(with_site_route({}))
    with sync_client(router, UDATA_CREDENTIAL) as client:
        with pytest.raises(ForbiddenError) as rejected:
            client.activity_discussions.delete_discussion("discussion-1", PERMISSIONS)
    receipt = rejected.value.__dict__["mutation_receipt"]
    assert receipt.operation == wire.DELETE_DISCUSSION_OPERATION
    assert receipt.target.value == "discussion-1"
    assert receipt.outcome == "rejected"
    assert b"secret-key" not in json.dumps(receipt.to_dict()).encode()
    assert router.requests == []


def test_cancelled_discussion_mutation_records_a_cancelled_receipt(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cancellation stays visible as `cancelled` rather than collapsing to `failed`."""

    async def cancel_async(**kwargs: object) -> tuple[int, object, object]:
        raise asyncio.CancelledError

    async def run_async() -> None:
        async with async_client(async_route_table(with_site_route({})), UDATA_CREDENTIAL) as client:
            monkeypatch.setattr(client, "_dataset_call_async", cancel_async)
            with pytest.raises(asyncio.CancelledError) as cancelled:
                await client.activity_discussions.delete_discussion(
                    "discussion-1",
                    PERMISSIONS,
                    _policy(wire.DELETE_DISCUSSION_OPERATION, "discussion-1"),
                )
            receipt = cancelled.value.__dict__["mutation_receipt"]
            assert receipt.outcome == "cancelled"
            assert receipt.operation == wire.DELETE_DISCUSSION_OPERATION

    asyncio.run(run_async())


def test_interrupted_discussion_mutation_is_not_misreported_as_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    """KeyboardInterrupt keeps the interruption outcome instead of the generic `failed`."""

    def interrupt(**kwargs: object) -> tuple[int, object, object]:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt) as stopped:
        with sync_client(sync_route_table(with_site_route({})), UDATA_CREDENTIAL) as client:
            monkeypatch.setattr(client, "_dataset_call", interrupt)
            client.activity_discussions.delete_discussion(
                "discussion-1",
                PERMISSIONS,
                _policy(wire.DELETE_DISCUSSION_OPERATION, "discussion-1"),
            )
    assert stopped.value.__dict__["mutation_receipt"].outcome == "cancelled"
