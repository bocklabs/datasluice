"""Deterministic loopback coverage for the exhaustive CKAN relationships/follows core."""

from __future__ import annotations

import asyncio
import inspect
import json

import pytest

from datasluice.connectors.catalog.ckan.clients import AsyncCKANClient, SyncCKANClient, declared_ckan_profile
from datasluice.connectors.catalog.ckan.inventory import CKAN_ACTIONS
from datasluice.connectors.catalog.ckan.services.relationships_activity import (
    AsyncRelationshipsActivityService,
    SyncRelationshipsActivityService,
)
from datasluice.domain.catalog.models import MappingRecord, NativeRecord, ValueRecord
from datasluice.errors.catalog import UnauthenticatedError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse

LOOPBACK_ORIGIN = "http://127.0.0.1:9001"
RELATIONSHIPS_ID = "ckan/action-api-v3.relationships-follows"
EXPECTED_RELATIONSHIP_ACTIONS = frozenset(
    {
        "package_relationships_list",
        "package_relationship_create",
        "package_relationship_update",
        "package_relationship_delete",
        "follow_dataset",
        "unfollow_dataset",
        "am_following_dataset",
        "follow_group",
        "unfollow_group",
        "am_following_group",
        "follow_user",
        "unfollow_user",
        "am_following_user",
        "dataset_follower_count",
        "dataset_follower_list",
        "group_follower_count",
        "group_follower_list",
        "organization_follower_count",
        "organization_follower_list",
        "user_follower_count",
        "user_follower_list",
        "dataset_followee_count",
        "dataset_followee_list",
        "group_followee_count",
        "group_followee_list",
        "organization_followee_count",
        "organization_followee_list",
        "user_followee_count",
        "user_followee_list",
        "followee_count",
        "followee_list",
    }
)

USER_RESULT: dict[str, object] = {"id": "user-1", "name": "admin", "display_name": "Site Admin"}


def _success_body(result: object) -> bytes:
    return json.dumps({"success": True, "result": result}).encode("utf-8")


def _failure_body(error: dict[str, object]) -> bytes:
    return json.dumps({"success": False, "error": dict(error)}).encode("utf-8")


class SyncCaptureTransport:
    """A deterministic loopback capture transport recording every sent request."""

    def __init__(self, *, status_code: int = 200, body: bytes = b"{}") -> None:
        self.status_code = status_code
        self.body = body
        self.requests: list[RuntimeRequest] = []

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return RuntimeResponse(
            status_code=self.status_code,
            headers={"Content-Type": "application/json"},
            body=self.body,
        )

    def close(self) -> None:
        return None


class AsyncCaptureTransport:
    """A deterministic async loopback capture transport recording every sent request."""

    def __init__(self, *, status_code: int = 200, body: bytes = b"{}") -> None:
        self.status_code = status_code
        self.body = body
        self.requests: list[RuntimeRequest] = []

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return RuntimeResponse(
            status_code=self.status_code,
            headers={"Content-Type": "application/json"},
            body=self.body,
        )

    async def aclose(self) -> None:
        return None


def _client(transport: SyncCaptureTransport) -> SyncCKANClient:
    return SyncCKANClient(
        transport,
        declared_ckan_profile(),
        origin=LOOPBACK_ORIGIN,
        owns_transport=False,
    )


def _async_client(transport: AsyncCaptureTransport) -> AsyncCKANClient:
    return AsyncCKANClient(
        transport,
        declared_ckan_profile(),
        origin=LOOPBACK_ORIGIN,
        owns_transport=False,
    )


def _core_names() -> set[str]:
    return {entry.name for entry in CKAN_ACTIONS.entries if entry.owning_operation_id == RELATIONSHIPS_ID}


def test_every_core_relationship_action_exposes_a_typed_method_on_both_mode_services() -> None:
    """Each manifest-registered core action names a callable member on both projections."""
    sync_surface = {name for name in dir(SyncRelationshipsActivityService) if not name.startswith("_")}
    async_surface = {name for name in dir(AsyncRelationshipsActivityService) if not name.startswith("_")}
    names = _core_names()
    assert names == EXPECTED_RELATIONSHIP_ACTIONS
    for action in names:
        assert action in sync_surface, f"sync surface misses {action}"
        assert action in async_surface, f"async surface misses {action}"
        assert callable(getattr(SyncRelationshipsActivityService, action))


def test_relationship_surfaces_stay_in_structural_lockstep_across_modes() -> None:
    """Sync/async projections expose identical members with mode-correct dispatch."""
    sync_members = {name for name in dir(SyncRelationshipsActivityService) if not name.startswith("__")}
    async_members = {name for name in dir(AsyncRelationshipsActivityService) if not name.startswith("__")}
    assert sync_members == async_members
    public = {name for name in vars(SyncRelationshipsActivityService) if not name.startswith("_")}
    for name in public:
        assert inspect.iscoroutinefunction(getattr(AsyncRelationshipsActivityService, name)), name
        assert not inspect.iscoroutinefunction(getattr(SyncRelationshipsActivityService, name)), name


def test_relationship_dispatch_passes_documented_parameters_verbatim() -> None:
    """D-04 fidelity: relationship parameters cross the wire untranslated."""
    transport = SyncCaptureTransport(
        body=_success_body([{"subject": "dataset-a", "object": "dataset-b", "type": "depends_on"}])
    )
    client = _client(transport)

    envelope = client.relationships_activity.package_relationships_list(
        id="dataset-a", id2="dataset-b", rel="depends_on"
    )

    request = transport.requests[0]
    assert request.url == f"{LOOPBACK_ORIGIN}/api/3/action/package_relationships_list"
    assert json.loads(request.body or b"{}") == {"id": "dataset-a", "id2": "dataset-b", "rel": "depends_on"}
    assert isinstance(envelope.items[0], MappingRecord)

    create_transport = SyncCaptureTransport(body=_success_body({"subject": "dataset-a", "object": "dataset-b"}))
    create_client = _client(create_transport)
    created = create_client.relationships_activity.package_relationship_create(
        subject="dataset-a", object="dataset-b", type="depends_on"
    )
    assert json.loads(create_transport.requests[0].body or b"{}") == {
        "subject": "dataset-a",
        "object": "dataset-b",
        "type": "depends_on",
    }
    assert created.receipt.outcome == "succeeded"


def test_follow_routes_captured_across_all_three_entity_types() -> None:
    """Route captures span dataset, group, and user follow dispatches."""
    for action, entity in (("follow_dataset", "dataset"), ("follow_group", "group"), ("follow_user", "user")):
        transport = SyncCaptureTransport(body=_success_body({"id": f"{entity}-1"}))
        client = _client(transport)

        result = getattr(client.relationships_activity, action)(id=f"{entity}-1")

        request = transport.requests[0]
        assert request.url.endswith(f"/api/3/action/{action}")
        assert json.loads(request.body or b"{}") == {"id": f"{entity}-1"}
        assert result.receipt.outcome == "succeeded"
        assert result.receipt.target.value == f"{entity}-1"


def test_anonymous_counts_shape_to_integer_value_envelopes() -> None:
    """Public anonymous reads decode counts as integer ValueRecords with no credential."""
    transport = SyncCaptureTransport(body=_success_body(7))
    client = _client(transport)

    envelope = client.relationships_activity.dataset_follower_count(id="dataset-a")

    assert isinstance(envelope.items[0], ValueRecord)
    assert envelope.items[0].value == 7
    request = transport.requests[0]
    assert request.url.endswith("/api/3/action/dataset_follower_count")
    assert "Authorization" not in request.headers


def test_am_following_shape_to_boolean_value_envelopes() -> None:
    """am_following_* booleans decode to ValueRecord envelopes."""
    transport = SyncCaptureTransport(body=_success_body(True))
    client = _client(transport)

    envelope = client.relationships_activity.am_following_dataset(id="dataset-a")

    assert isinstance(envelope.items[0], ValueRecord)
    assert envelope.items[0].value is True

    group_transport = SyncCaptureTransport(body=_success_body(False))
    group_client = _client(group_transport)
    group_envelope = group_client.relationships_activity.am_following_group(id="group-1")
    assert isinstance(group_envelope.items[0], ValueRecord)
    assert group_envelope.items[0].value is False


def test_follower_and_followee_lists_decode_user_kind_records() -> None:
    """Follower/followee list families decode user records for their entity types."""
    transport = SyncCaptureTransport(body=_success_body([USER_RESULT, USER_RESULT]))
    client = _client(transport)

    envelope = client.relationships_activity.dataset_follower_list(id="dataset-a")

    assert len(envelope.items) == 2
    record = envelope.items[0]
    assert isinstance(record, NativeRecord)
    assert record.resource_kind.value == "user"
    assert record.id.value == "user-1"

    followee_transport = SyncCaptureTransport(body=_success_body([USER_RESULT]))
    followee_client = _client(followee_transport)
    followee_envelope = followee_client.relationships_activity.followee_list(id="user-1")
    assert isinstance(followee_envelope.items[0], NativeRecord)
    assert followee_envelope.items[0].resource_kind.value == "user"


def test_follow_without_credentials_maps_to_the_missing_credentials_error_class() -> None:
    """A server authorization envelope on follows raises the distinct unauthenticated error."""
    transport = SyncCaptureTransport(body=_failure_body({"__type": "Authorization Error", "message": "bad token"}))
    client = _client(transport)

    with pytest.raises(UnauthenticatedError) as excinfo:
        client.relationships_activity.follow_dataset(id="dataset-a")

    assert excinfo.value.safe_action
    assert len(transport.requests) == 1


def test_async_follow_and_count_mirror_the_sync_semantics() -> None:
    """The async twin keeps verbatim parameters, receipts, and scalar decoding."""
    follow_transport = AsyncCaptureTransport(body=_success_body({"id": "dataset-a"}))
    follow_client = _async_client(follow_transport)

    followed = asyncio.run(follow_client.relationships_activity.follow_dataset(id="dataset-a"))

    request = follow_transport.requests[0]
    assert request.url.endswith("/api/3/action/follow_dataset")
    assert json.loads(request.body or b"{}") == {"id": "dataset-a"}
    assert followed.receipt.outcome == "succeeded"

    count_transport = AsyncCaptureTransport(body=_success_body(3))
    count_client = _async_client(count_transport)
    counted = asyncio.run(count_client.relationships_activity.user_follower_count(id="user-1"))
    assert isinstance(counted.items[0], ValueRecord)
    assert counted.items[0].value == 3
