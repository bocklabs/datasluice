"""Both-mode CKAN relationship, follow, and activity projections across the v2 ids.

Relationships and follows (31 actions) ride the core
``relationships-follows`` id while the ckanext.activity tier (13 actions) owns
its optional ``activity`` id, so the two families resolve capability evidence
independently (the v2 profile correction from 03-02). Every typed method
declares its owning v2 OperationId from the checked-in manifest and passes
documented CKAN 2.11 parameters verbatim (D-04). Follower counts and
``am_following_*`` booleans shape to scalar ValueRecord envelopes while
follower/followee lists decode user-kind records — genuine scalar typing per
the Plan 08 review. Mutations return CKANMutationResult through the shared
receipt seam; anonymous count reads stay public per platform documentation.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, cast

from datasluice.connectors.catalog.ckan.clients import (
    _AsyncNativeService,
    _operation_id_from,
    _SyncNativeService,
)
from datasluice.connectors.catalog.ckan.inventory import ActionEntry
from datasluice.connectors.catalog.ckan.mapping import ACTIVITY, PLATFORM
from datasluice.connectors.catalog.ckan.results import CKANMutationResult, require_mutation_tier
from datasluice.contracts.catalog.native.ckan import CKANResultItem
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.ids import CatalogId, ResourceKind
from datasluice.domain.catalog.models import ResultEnvelope
from datasluice.domain.catalog.safety import MutationPolicy
from datasluice.errors.catalog import CatalogValidationError
from datasluice.runtime.mutation import build_mutation_receipt

if TYPE_CHECKING:
    from datasluice.connectors.catalog.ckan.clients import AsyncCKANClient, SyncCKANClient

type WireParams = dict[str, object]

_GROUP = "relationships_activity"

_ENTITY_KINDS: Mapping[str, ResourceKind] = {
    "dataset": ResourceKind.DATASET,
    "group": ResourceKind("group"),
    "user": ResourceKind.USER,
}


def _drop_unset(params: dict[str, object | None]) -> dict[str, object]:
    return {key: value for key, value in params.items() if value is not None}


_ACTIVITY_TARGETS: Mapping[str, str] = {
    "dashboard_mark_activities_old": "dashboard",
    "send_email_notifications": "activity-notifications",
}


def _mutation_target(action: str, params: Mapping[str, object]) -> CatalogId:
    if action == "activity_create":
        return CatalogId(PLATFORM, ACTIVITY, str(params["object_id"]))
    if action in _ACTIVITY_TARGETS:
        return CatalogId(PLATFORM, ACTIVITY, _ACTIVITY_TARGETS[action])
    if action.startswith("package_relationship"):
        return CatalogId(PLATFORM, ResourceKind.DATASET, str(params["subject"]))
    entity = action.rsplit("_", 1)[-1]
    kind = _ENTITY_KINDS[entity]
    return CatalogId(PLATFORM, kind, str(params["id"]))


class SyncRelationshipsActivityService(_SyncNativeService):
    """Synchronous relationship, follow, and activity projection with typed actions."""

    __slots__ = ()

    def __init__(self, client: SyncCKANClient) -> None:
        super().__init__(client, "relationships_activity")

    def package_relationships_list(self, *, id: str, id2: str, rel: str) -> ResultEnvelope[CKANResultItem]:
        """List relationships between two datasets filtered by relationship type."""
        return self._invoke_read("package_relationships_list", {"id": id, "id2": id2, "rel": rel})

    def package_relationship_create(
        self, *, subject: str, object: str, type: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Create a relationship between two datasets on the standard tier."""
        params: WireParams = {"subject": subject, "object": object, "type": type}
        return self._invoke_mutation("package_relationship_create", params, policy)

    def package_relationship_update(
        self, *, subject: str, object: str, type: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Update a relationship between two datasets on the standard tier."""
        params: WireParams = {"subject": subject, "object": object, "type": type}
        return self._invoke_mutation("package_relationship_update", params, policy)

    def package_relationship_delete(
        self, *, subject: str, object: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Delete a relationship between two datasets on the standard tier."""
        params: WireParams = {"subject": subject, "object": object}
        return self._invoke_mutation("package_relationship_delete", params, policy)

    def follow_dataset(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Follow one dataset from the authenticated user's context."""
        return self._invoke_mutation("follow_dataset", {"id": id}, policy)

    def unfollow_dataset(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Stop following one dataset from the authenticated user's context."""
        return self._invoke_mutation("unfollow_dataset", {"id": id}, policy)

    def am_following_dataset(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return whether the authenticated user follows one dataset."""
        return self._invoke_read("am_following_dataset", {"id": id})

    def follow_group(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Follow one group from the authenticated user's context."""
        return self._invoke_mutation("follow_group", {"id": id}, policy)

    def unfollow_group(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Stop following one group from the authenticated user's context."""
        return self._invoke_mutation("unfollow_group", {"id": id}, policy)

    def am_following_group(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return whether the authenticated user follows one group."""
        return self._invoke_read("am_following_group", {"id": id})

    def follow_user(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Follow one user from the authenticated user's context."""
        return self._invoke_mutation("follow_user", {"id": id}, policy)

    def unfollow_user(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Stop following one user from the authenticated user's context."""
        return self._invoke_mutation("unfollow_user", {"id": id}, policy)

    def am_following_user(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return whether the authenticated user follows one user."""
        return self._invoke_read("am_following_user", {"id": id})

    def dataset_follower_count(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return one dataset's follower count as a public anonymous read."""
        return self._invoke_read("dataset_follower_count", {"id": id})

    def dataset_follower_list(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """List the users following one dataset as user-kind records."""
        return self._invoke_read("dataset_follower_list", {"id": id})

    def group_follower_count(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return one group's follower count as a public anonymous read."""
        return self._invoke_read("group_follower_count", {"id": id})

    def group_follower_list(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """List the users following one group as user-kind records."""
        return self._invoke_read("group_follower_list", {"id": id})

    def organization_follower_count(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return one organization's follower count as a public anonymous read."""
        return self._invoke_read("organization_follower_count", {"id": id})

    def organization_follower_list(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """List the users following one organization as user-kind records."""
        return self._invoke_read("organization_follower_list", {"id": id})

    def user_follower_count(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return one user's follower count as a public anonymous read."""
        return self._invoke_read("user_follower_count", {"id": id})

    def user_follower_list(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """List the users following one user as user-kind records."""
        return self._invoke_read("user_follower_list", {"id": id})

    def dataset_followee_count(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return how many datasets one user follows."""
        return self._invoke_read("dataset_followee_count", {"id": id})

    def dataset_followee_list(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """List the datasets one user follows."""
        return self._invoke_read("dataset_followee_list", {"id": id})

    def group_followee_count(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return how many groups one user follows."""
        return self._invoke_read("group_followee_count", {"id": id})

    def group_followee_list(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """List the groups one user follows as user-kind records."""
        return self._invoke_read("group_followee_list", {"id": id})

    def organization_followee_count(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return how many organizations one user follows."""
        return self._invoke_read("organization_followee_count", {"id": id})

    def organization_followee_list(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """List the organizations one user follows."""
        return self._invoke_read("organization_followee_list", {"id": id})

    def user_followee_count(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return how many users one user follows."""
        return self._invoke_read("user_followee_count", {"id": id})

    def user_followee_list(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """List the users one user follows as user-kind records."""
        return self._invoke_read("user_followee_list", {"id": id})

    def followee_count(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return how many objects one user follows in total."""
        return self._invoke_read("followee_count", {"id": id})

    def followee_list(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """List every object one user follows as user-kind records."""
        return self._invoke_read("followee_list", {"id": id})

    def package_activity_list(
        self, *, id: str, offset: int | None = None, limit: int | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List one dataset's activity stream under the optional activity id."""
        return self._invoke_read("package_activity_list", _drop_unset({"id": id, "offset": offset, "limit": limit}))

    def group_activity_list(
        self, *, id: str, offset: int | None = None, limit: int | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List one group's activity stream under the optional activity id."""
        return self._invoke_read("group_activity_list", _drop_unset({"id": id, "offset": offset, "limit": limit}))

    def organization_activity_list(
        self, *, id: str, offset: int | None = None, limit: int | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List one organization's activity stream under the optional activity id."""
        return self._invoke_read(
            "organization_activity_list", _drop_unset({"id": id, "offset": offset, "limit": limit})
        )

    def user_activity_list(
        self, *, id: str, offset: int | None = None, limit: int | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List one user's activity stream under the optional activity id."""
        return self._invoke_read("user_activity_list", _drop_unset({"id": id, "offset": offset, "limit": limit}))

    def recently_changed_packages_activity_list(
        self, *, offset: int | None = None, limit: int | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List recently changed datasets' activity under the optional activity id."""
        return self._invoke_read(
            "recently_changed_packages_activity_list", _drop_unset({"offset": offset, "limit": limit})
        )

    def dashboard_activity_list(
        self, *, offset: int | None = None, limit: int | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List the authenticated user's dashboard activity under the optional activity id."""
        return self._invoke_read("dashboard_activity_list", _drop_unset({"offset": offset, "limit": limit}))

    def dashboard_new_activities_count(self) -> ResultEnvelope[CKANResultItem]:
        """Count the dashboard's new activities as an integer ValueRecord."""
        return self._invoke_read("dashboard_new_activities_count", {})

    def dashboard_mark_activities_old(self, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Mark every dashboard activity as seen on the standard tier."""
        return self._invoke_mutation("dashboard_mark_activities_old", {}, policy)

    def activity_show(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Show one activity detail as an activity-kind record."""
        return self._invoke_read("activity_show", {"id": id})

    def activity_data_show(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Show one activity's data payload as a lossless mapping."""
        return self._invoke_read("activity_data_show", {"id": id})

    def activity_diff(
        self, *, id: str, context: str | None = None, diff_type: str | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """Show one activity's structured diff as a lossless mapping."""
        return self._invoke_read("activity_diff", _drop_unset({"id": id, "context": context, "diff_type": diff_type}))

    def activity_create(
        self,
        *,
        user: str,
        object_id: str,
        activity_type: str,
        data: dict[str, object],
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Create one activity record explicitly on the standard tier."""
        params: dict[str, object] = {"user": user, "object_id": object_id, "activity_type": activity_type, "data": data}
        return self._invoke_mutation("activity_create", params, policy)

    def send_email_notifications(self, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Trigger the privileged notification job; server authorization stays the evidence."""
        return self._invoke_mutation("send_email_notifications", {}, policy)

    def _typed_entry(self, action: str) -> ActionEntry:
        entry = self._client._inventory.lookup(action)
        if entry.group != _GROUP:
            raise CatalogValidationError(
                f"The action {action!r} does not belong to the {_GROUP!r} group.",
                operation=entry.owning_operation_id,
                platform=PLATFORM.value,
                safe_action="Call the action through its owning native group projection.",
            )
        return entry

    def _invoke_read(self, action: str, params: dict[str, object]) -> ResultEnvelope[CKANResultItem]:
        entry = self._typed_entry(action)
        client: SyncCKANClient = self._client
        operation = CatalogOperationRequest(operation_id=_operation_id_from(entry.owning_operation_id), payload=params)
        guard = CatalogOperationGuard(operation_id=operation.operation_id, profile=client._profile)
        return cast(ResultEnvelope[CKANResultItem], client._dispatch(operation, guard, entry=entry))

    def _invoke_mutation(
        self, action: str, params: dict[str, object], policy: MutationPolicy | None
    ) -> CKANMutationResult:
        entry = self._typed_entry(action)
        client: SyncCKANClient = self._client
        owning_id = _operation_id_from(entry.owning_operation_id)
        effective = require_mutation_tier(entry.mutation_class, owning_id, policy)
        assert effective is not None
        operation = CatalogOperationRequest(operation_id=owning_id, payload=params, mutation_policy=effective)
        guard = CatalogOperationGuard(operation_id=owning_id, profile=client._profile)
        envelope = cast(ResultEnvelope[CKANResultItem], client._dispatch(operation, guard, entry=entry))
        receipt = build_mutation_receipt(
            owning_id, _mutation_target(entry.name, params), effective, "succeeded", {"action": entry.name}
        )
        return CKANMutationResult(result=envelope, receipt=receipt)


class AsyncRelationshipsActivityService(_AsyncNativeService):
    """Asynchronous relationship, follow, and activity projection with typed actions."""

    __slots__ = ()

    def __init__(self, client: AsyncCKANClient) -> None:
        super().__init__(client, "relationships_activity")

    async def package_relationships_list(self, *, id: str, id2: str, rel: str) -> ResultEnvelope[CKANResultItem]:
        """List relationships between two datasets filtered by relationship type."""
        return await self._invoke_read("package_relationships_list", {"id": id, "id2": id2, "rel": rel})

    async def package_relationship_create(
        self, *, subject: str, object: str, type: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Create a relationship between two datasets on the standard tier."""
        params: WireParams = {"subject": subject, "object": object, "type": type}
        return await self._invoke_mutation("package_relationship_create", params, policy)

    async def package_relationship_update(
        self, *, subject: str, object: str, type: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Update a relationship between two datasets on the standard tier."""
        params: WireParams = {"subject": subject, "object": object, "type": type}
        return await self._invoke_mutation("package_relationship_update", params, policy)

    async def package_relationship_delete(
        self, *, subject: str, object: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Delete a relationship between two datasets on the standard tier."""
        params: WireParams = {"subject": subject, "object": object}
        return await self._invoke_mutation("package_relationship_delete", params, policy)

    async def follow_dataset(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Follow one dataset from the authenticated user's context."""
        return await self._invoke_mutation("follow_dataset", {"id": id}, policy)

    async def unfollow_dataset(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Stop following one dataset from the authenticated user's context."""
        return await self._invoke_mutation("unfollow_dataset", {"id": id}, policy)

    async def am_following_dataset(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return whether the authenticated user follows one dataset."""
        return await self._invoke_read("am_following_dataset", {"id": id})

    async def follow_group(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Follow one group from the authenticated user's context."""
        return await self._invoke_mutation("follow_group", {"id": id}, policy)

    async def unfollow_group(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Stop following one group from the authenticated user's context."""
        return await self._invoke_mutation("unfollow_group", {"id": id}, policy)

    async def am_following_group(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return whether the authenticated user follows one group."""
        return await self._invoke_read("am_following_group", {"id": id})

    async def follow_user(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Follow one user from the authenticated user's context."""
        return await self._invoke_mutation("follow_user", {"id": id}, policy)

    async def unfollow_user(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Stop following one user from the authenticated user's context."""
        return await self._invoke_mutation("unfollow_user", {"id": id}, policy)

    async def am_following_user(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return whether the authenticated user follows one user."""
        return await self._invoke_read("am_following_user", {"id": id})

    async def dataset_follower_count(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return one dataset's follower count as a public anonymous read."""
        return await self._invoke_read("dataset_follower_count", {"id": id})

    async def dataset_follower_list(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """List the users following one dataset as user-kind records."""
        return await self._invoke_read("dataset_follower_list", {"id": id})

    async def group_follower_count(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return one group's follower count as a public anonymous read."""
        return await self._invoke_read("group_follower_count", {"id": id})

    async def group_follower_list(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """List the users following one group as user-kind records."""
        return await self._invoke_read("group_follower_list", {"id": id})

    async def organization_follower_count(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return one organization's follower count as a public anonymous read."""
        return await self._invoke_read("organization_follower_count", {"id": id})

    async def organization_follower_list(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """List the users following one organization as user-kind records."""
        return await self._invoke_read("organization_follower_list", {"id": id})

    async def user_follower_count(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return one user's follower count as a public anonymous read."""
        return await self._invoke_read("user_follower_count", {"id": id})

    async def user_follower_list(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """List the users following one user as user-kind records."""
        return await self._invoke_read("user_follower_list", {"id": id})

    async def dataset_followee_count(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return how many datasets one user follows."""
        return await self._invoke_read("dataset_followee_count", {"id": id})

    async def dataset_followee_list(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """List the datasets one user follows."""
        return await self._invoke_read("dataset_followee_list", {"id": id})

    async def group_followee_count(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return how many groups one user follows."""
        return await self._invoke_read("group_followee_count", {"id": id})

    async def group_followee_list(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """List the groups one user follows as user-kind records."""
        return await self._invoke_read("group_followee_list", {"id": id})

    async def organization_followee_count(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return how many organizations one user follows."""
        return await self._invoke_read("organization_followee_count", {"id": id})

    async def organization_followee_list(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """List the organizations one user follows."""
        return await self._invoke_read("organization_followee_list", {"id": id})

    async def user_followee_count(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return how many users one user follows."""
        return await self._invoke_read("user_followee_count", {"id": id})

    async def user_followee_list(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """List the users one user follows as user-kind records."""
        return await self._invoke_read("user_followee_list", {"id": id})

    async def followee_count(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Return how many objects one user follows in total."""
        return await self._invoke_read("followee_count", {"id": id})

    async def followee_list(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """List every object one user follows as user-kind records."""
        return await self._invoke_read("followee_list", {"id": id})

    async def package_activity_list(
        self, *, id: str, offset: int | None = None, limit: int | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List one dataset's activity stream under the optional activity id."""
        return await self._invoke_read(
            "package_activity_list", _drop_unset({"id": id, "offset": offset, "limit": limit})
        )

    async def group_activity_list(
        self, *, id: str, offset: int | None = None, limit: int | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List one group's activity stream under the optional activity id."""
        return await self._invoke_read("group_activity_list", _drop_unset({"id": id, "offset": offset, "limit": limit}))

    async def organization_activity_list(
        self, *, id: str, offset: int | None = None, limit: int | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List one organization's activity stream under the optional activity id."""
        return await self._invoke_read(
            "organization_activity_list", _drop_unset({"id": id, "offset": offset, "limit": limit})
        )

    async def user_activity_list(
        self, *, id: str, offset: int | None = None, limit: int | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List one user's activity stream under the optional activity id."""
        return await self._invoke_read("user_activity_list", _drop_unset({"id": id, "offset": offset, "limit": limit}))

    async def recently_changed_packages_activity_list(
        self, *, offset: int | None = None, limit: int | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List recently changed datasets' activity under the optional activity id."""
        return await self._invoke_read(
            "recently_changed_packages_activity_list", _drop_unset({"offset": offset, "limit": limit})
        )

    async def dashboard_activity_list(
        self, *, offset: int | None = None, limit: int | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List the authenticated user's dashboard activity under the optional activity id."""
        return await self._invoke_read("dashboard_activity_list", _drop_unset({"offset": offset, "limit": limit}))

    async def dashboard_new_activities_count(self) -> ResultEnvelope[CKANResultItem]:
        """Count the dashboard's new activities as an integer ValueRecord."""
        return await self._invoke_read("dashboard_new_activities_count", {})

    async def dashboard_mark_activities_old(self, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Mark every dashboard activity as seen on the standard tier."""
        return await self._invoke_mutation("dashboard_mark_activities_old", {}, policy)

    async def activity_show(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Show one activity detail as an activity-kind record."""
        return await self._invoke_read("activity_show", {"id": id})

    async def activity_data_show(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Show one activity's data payload as a lossless mapping."""
        return await self._invoke_read("activity_data_show", {"id": id})

    async def activity_diff(
        self, *, id: str, context: str | None = None, diff_type: str | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """Show one activity's structured diff as a lossless mapping."""
        return await self._invoke_read(
            "activity_diff", _drop_unset({"id": id, "context": context, "diff_type": diff_type})
        )

    async def activity_create(
        self,
        *,
        user: str,
        object_id: str,
        activity_type: str,
        data: dict[str, object],
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Create one activity record explicitly on the standard tier."""
        params: dict[str, object] = {"user": user, "object_id": object_id, "activity_type": activity_type, "data": data}
        return await self._invoke_mutation("activity_create", params, policy)

    async def send_email_notifications(self, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Trigger the privileged notification job; server authorization stays the evidence."""
        return await self._invoke_mutation("send_email_notifications", {}, policy)

    def _typed_entry(self, action: str) -> ActionEntry:
        entry = self._client._inventory.lookup(action)
        if entry.group != _GROUP:
            raise CatalogValidationError(
                f"The action {action!r} does not belong to the {_GROUP!r} group.",
                operation=entry.owning_operation_id,
                platform=PLATFORM.value,
                safe_action="Call the action through its owning native group projection.",
            )
        return entry

    async def _invoke_read(self, action: str, params: dict[str, object]) -> ResultEnvelope[CKANResultItem]:
        entry = self._typed_entry(action)
        client: AsyncCKANClient = self._client
        operation = CatalogOperationRequest(operation_id=_operation_id_from(entry.owning_operation_id), payload=params)
        guard = CatalogOperationGuard(operation_id=operation.operation_id, profile=client._profile)
        return cast(ResultEnvelope[CKANResultItem], await client._dispatch(operation, guard, entry=entry))

    async def _invoke_mutation(
        self, action: str, params: dict[str, object], policy: MutationPolicy | None
    ) -> CKANMutationResult:
        entry = self._typed_entry(action)
        client: AsyncCKANClient = self._client
        owning_id = _operation_id_from(entry.owning_operation_id)
        effective = require_mutation_tier(entry.mutation_class, owning_id, policy)
        assert effective is not None
        operation = CatalogOperationRequest(operation_id=owning_id, payload=params, mutation_policy=effective)
        guard = CatalogOperationGuard(operation_id=owning_id, profile=client._profile)
        envelope = cast(ResultEnvelope[CKANResultItem], await client._dispatch(operation, guard, entry=entry))
        receipt = build_mutation_receipt(
            owning_id, _mutation_target(entry.name, params), effective, "succeeded", {"action": entry.name}
        )
        return CKANMutationResult(result=envelope, receipt=receipt)
