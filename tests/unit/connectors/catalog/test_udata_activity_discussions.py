"""Exact wire and safety coverage for the uData activity and discussion family.

Expectations are transcribed from the pinned uData 17.6.0 source
(`udata.core.activity.api`, `udata.core.disccussions.api`, and
`udata.core.discussions.apiv2` at commit 0546582), not from the
production request builders.
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
from datasluice.connectors.catalog.udata.models.activity_discussions import (
    ActivityQuery,
    CommentInput,
    DiscussionCreateInput,
    DiscussionMutationResult,
    DiscussionSearchQuery,
    DiscussionUpdateInput,
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
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.errors.catalog import CatalogValidationError, ForbiddenError, NativeCatalogError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse

ORIGIN = "http://127.0.0.1:5640"
SITE = {"feed_size": 0, "id": "site", "keywords": [], "metrics": {}, "title": "uData", "version": "17.6.0"}
CREDENTIAL = UDataCredential(api_key="secret-key")
PERMISSIONS = EffectivePermissions.for_credential(CREDENTIAL, platform=CatalogPlatform.UDATA)
CREATE_ONLY_PERMISSIONS = EffectivePermissions.for_credential(
    CREDENTIAL,
    platform=CatalogPlatform.UDATA,
    scopes=frozenset({"create-discussion"}),
    operation_scopes={wire.CREATE_DISCUSSION_OPERATION: frozenset({"create-discussion"})},
)
DELETE_ONLY_PERMISSIONS = EffectivePermissions.for_credential(
    CREDENTIAL,
    platform=CatalogPlatform.UDATA,
    scopes=frozenset({"delete-discussion"}),
    operation_scopes={wire.DELETE_DISCUSSION_OPERATION: frozenset({"delete-discussion"})},
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
        return RuntimeResponse(status_code=status, headers={"Content-Type": "application/json"}, body=body)

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
        return RuntimeResponse(status_code=status, headers={"Content-Type": "application/json"}, body=body)

    async def aclose(self) -> None:
        return None


def _routes(routes: dict[tuple[str, str], tuple[int, object]]) -> dict[tuple[str, str], tuple[int, object]]:
    return {("GET", f"{ORIGIN}/api/1/site/"): (200, SITE), **routes}


def _policy(operation: str, target: str) -> MutationPolicy:
    """Only the two stock delete routes are destructive transitions."""

    return MutationPolicy(
        destructive=operation in {wire.DELETE_DISCUSSION_OPERATION, wire.DELETE_DISCUSSION_COMMENT_OPERATION},
        confirmation=ConfirmationPolicy(confirmed=True, operation=operation, target=target),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


def _thawed(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thawed(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_thawed(item) for item in value]
    return value


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
    with SyncUDataClient(_Router(_routes({})), declared_udata_profile(), origin=ORIGIN) as client:
        assert isinstance(client.activity_discussions, SyncUDataActivityDiscussionsService)

    async def run() -> None:
        async with AsyncUDataClient(_AsyncRouter(_routes({})), declared_udata_profile(), origin=ORIGIN) as client:
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
    router = _Router(_routes({("POST", f"{ORIGIN}/api/1/discussions/"): (201, {"id": "discussion-1", "title": "t"})}))
    with SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client:
        client.activity_discussions.create_discussion(
            DiscussionCreateInput(title="t", comment="c", subject={"id": "d", "class": "Dataset"}),
            CREATE_ONLY_PERMISSIONS,
            MutationPolicy(
                destructive=False,
                confirmation=ConfirmationPolicy(confirmed=True, operation=wire.CREATE_DISCUSSION_OPERATION, target="d"),
                concurrency=ConcurrencyPolicy(overwrite=True),
            ),
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
    routes: dict[tuple[str, str], tuple[int, object]] = {
        ("GET", f"{ORIGIN}/api/1/activity/"): (200, activity_page),
        ("GET", f"{ORIGIN}/api/1/activity/?user=u1"): (200, activity_page),
        ("GET", f"{ORIGIN}/api/1/discussions/"): (200, list_page),
        ("GET", f"{ORIGIN}/api/1/discussions/discussion-1/"): (200, discussion),
        ("GET", f"{ORIGIN}/api/2/discussions/search/?page=1&page_size=20"): (200, search_page),
    }
    with SyncUDataClient(_Router(_routes(routes)), declared_udata_profile(), origin=ORIGIN) as client:
        service = client.activity_discussions
        assert _thawed(service.activity(ActivityQuery()).payload) == _thawed(activity_page)
        assert _thawed(service.list_discussions().payload) == _thawed(list_page)
        assert _thawed(service.get_discussion("discussion-1").payload) == _thawed(discussion)
        assert _thawed(service.search_discussions(DiscussionSearchQuery()).payload) == _thawed(search_page)

    async def run() -> None:
        async with AsyncUDataClient(_AsyncRouter(_routes(routes)), declared_udata_profile(), origin=ORIGIN) as client:
            assert _thawed((await client.activity_discussions.get_discussion("discussion-1")).payload) == _thawed(
                discussion
            )
            assert _thawed((await client.activity_discussions.activity(ActivityQuery(user="u1"))).payload) is not None

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
    routes: dict[tuple[str, str], tuple[int, object]] = {
        ("POST", f"{ORIGIN}/api/1/discussions/discussion-1/"): (200, discussion),
        ("PUT", f"{ORIGIN}/api/1/discussions/discussion-1/"): (200, {**discussion, "title": "new"}),
        ("PUT", f"{ORIGIN}/api/1/discussions/discussion-1/comments/0/"): (200, discussion),
        ("DELETE", f"{ORIGIN}/api/1/discussions/discussion-1/"): (204, None),
        ("DELETE", f"{ORIGIN}/api/1/discussions/discussion-1/comments/0/"): (204, None),
    }
    with SyncUDataClient(
        _Router(_routes(routes)), declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL
    ) as client:
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
    routes = _routes(
        {
            ("POST", f"{ORIGIN}/api/1/discussions/"): (201, {"id": "discussion-1"}),
            ("DELETE", f"{ORIGIN}/api/1/discussions/discussion-1/"): (204, None),
        }
    )
    router = _Router(routes)
    create_input = DiscussionCreateInput(title="t", comment="c", subject={"id": "d", "class": "Dataset"})

    def run_sync() -> None:
        with SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client:
            with pytest.raises(ForbiddenError) as denied_create:
                client.activity_discussions.create_discussion(create_input, DELETE_ONLY_PERMISSIONS)
            with pytest.raises(ForbiddenError) as denied_delete:
                client.activity_discussions.delete_discussion("discussion-1", CREATE_ONLY_PERMISSIONS)
            assert denied_create.value.operation == wire.CREATE_DISCUSSION_OPERATION
            assert denied_delete.value.operation == wire.DELETE_DISCUSSION_OPERATION
            assert router.requests == []

    async def run_async() -> None:
        async with AsyncUDataClient(
            _AsyncRouter(routes), declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL
        ) as client:
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

    router = _Router(_routes({}))
    with SyncUDataClient(router, declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL) as client:
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
        async with AsyncUDataClient(
            _AsyncRouter(_routes({})), declared_udata_profile(), origin=ORIGIN, credentials=CREDENTIAL
        ) as client:
            monkeypatch.setattr(client, "_dataset_call_async", capture_async)
            await client.activity_discussions.comment_discussion(
                "discussion-1",
                CommentInput(comment="c"),
                PERMISSIONS,
                _policy(wire.COMMENT_DISCUSSION_OPERATION, "discussion-1"),
            )

    asyncio.run(run_async())
    sync_operations = [call["owning_operation"] for call in calls]
    assert sync_operations == [
        wire.COMMENT_DISCUSSION_OPERATION,
        wire.UPDATE_DISCUSSION_OPERATION,
        wire.EDIT_DISCUSSION_COMMENT_OPERATION,
        wire.DELETE_DISCUSSION_OPERATION,
        wire.DELETE_DISCUSSION_COMMENT_OPERATION,
        wire.COMMENT_DISCUSSION_OPERATION,
    ]
    assert len(set(sync_operations)) == 5


def test_non_finite_decoding_fails_typed_without_raw_body() -> None:
    nan_site = (
        b'{"version": "17.6.0", "id": "site", "title": "uData", "feed_size": 0, '
        b'"keywords": [], "metrics": {"widgets": NaN}}'
    )
    with pytest.raises(NativeCatalogError):
        with SyncUDataClient(
            _Router(
                {
                    ("GET", f"{ORIGIN}/api/1/site/"): (200, nan_site),
                    ("GET", f"{ORIGIN}/api/1/activity/"): (200, nan_site),
                }
            ),
            declared_udata_profile(),
            origin=ORIGIN,
        ) as client:
            client.activity_discussions.activity(ActivityQuery())
