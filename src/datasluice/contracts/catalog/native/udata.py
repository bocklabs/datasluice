from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.auth import EffectivePermissions
from datasluice.domain.catalog.models import MappingRecord, NativeRecord, ResultEnvelope
from datasluice.domain.catalog.safety import MutationPolicy
from datasluice.errors.catalog import NativeCatalogError

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.models.activity_discussions import (
        ActivityQuery,
        CommentInput,
        DiscussionCreateInput,
        DiscussionMutationResult,
        DiscussionSearchQuery,
        DiscussionUpdateInput,
    )
    from datasluice.connectors.catalog.udata.models.oauth import (
        OAuthAuthorizeDecision,
        OAuthClientRequest,
        OAuthConsentOutcome,
        OAuthConsentSummary,
        OAuthErrorDocument,
        OAuthRevokeRequest,
        OAuthTokenRequest,
        OAuthTokenResult,
    )
    from datasluice.connectors.catalog.udata.models.organizations import (
        MembershipRequestInput,
        MembershipRequestQuery,
        OrganizationCreateInput,
        OrganizationDatasetQuery,
        OrganizationInvitationInput,
        OrganizationListQuery,
        OrganizationLogoInput,
        OrganizationMemberInput,
        OrganizationMutationResult,
        OrganizationRefusalInput,
        OrganizationSuggestQuery,
        OrganizationUpdateInput,
    )
    from datasluice.connectors.catalog.udata.models.posts_reports import (
        NotificationQuery,
        PostCreateInput,
        PostListQuery,
        PostMutationResult,
        PostSearchQuery,
        PostUpdateInput,
        ReportCreateInput,
        ReportQuery,
        ReportUpdateInput,
    )
    from datasluice.connectors.catalog.udata.models.resources import (
        ResourceCreateInput,
        ResourceMutationResult,
        ResourceUpdateInput,
        ResourceUploadInput,
    )
    from datasluice.connectors.catalog.udata.models.reuses import (
        ReuseCreateInput,
        ReuseFollowersQuery,
        ReuseListQuery,
        ReuseMutationResult,
        ReuseSearchQuery,
        ReuseSuggestQuery,
        ReuseUpdateInput,
    )
    from datasluice.connectors.catalog.udata.models.spatial import (
        SpatialDatasetQuery,
        SpatialSuggestQuery,
    )
    from datasluice.connectors.catalog.udata.models.taxonomies import (
        BadgeCreateInput,
        SuggestQuery,
        TaxonomyMutationResult,
    )
    from datasluice.connectors.catalog.udata.models.users import (
        ApiTokenCreateInput,
        ApiTokenCreationResult,
        ApiTokenMetadata,
        UserAvatarInput,
        UserCreateInput,
        UserDeleteOptions,
        UserListQuery,
        UserMutationResult,
        UserSuggestQuery,
        UserUpdateInput,
    )
    from datasluice.domain.catalog.udata import (
        SiteCatalogQuery,
        SiteDataserviceCsvQuery,
        SiteDatasetCsvQuery,
        SiteDocument,
        SiteMutationResult,
        SiteOrganizationCsvQuery,
        SitePatchInput,
        SiteProfile,
        SiteReuseCsvQuery,
    )

type UDataResultItem = NativeRecord
type UDataResult = ResultEnvelope[UDataResultItem]


@runtime_checkable
class SyncUDataRootProfileService(Protocol):
    """Typed synchronous root-profile service."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    def get(self) -> SiteProfile: ...

    def set_site(
        self,
        client_input: SitePatchInput,
        *,
        permissions: EffectivePermissions | None,
        mutation_policy: MutationPolicy | None = None,
    ) -> SiteMutationResult: ...

    def data_portal(self, fmt: str) -> SiteDocument: ...

    def rdf_catalog(self, query: SiteCatalogQuery | None = None, *, accept: str | None = None) -> SiteDocument: ...

    def rdf_catalog_format(
        self, fmt: str, query: SiteCatalogQuery | None = None, *, sink: Callable[[bytes], None] | None = None
    ) -> SiteDocument: ...

    def datasets_csv(
        self, query: SiteDatasetCsvQuery | None = None, *, sink: Callable[[bytes], None] | None = None
    ) -> SiteDocument: ...

    def resources_csv(
        self, query: SiteDatasetCsvQuery | None = None, *, sink: Callable[[bytes], None] | None = None
    ) -> SiteDocument: ...

    def organizations_csv(
        self, query: SiteOrganizationCsvQuery | None = None, *, sink: Callable[[bytes], None] | None = None
    ) -> SiteDocument: ...

    def reuses_csv(
        self, query: SiteReuseCsvQuery | None = None, *, sink: Callable[[bytes], None] | None = None
    ) -> SiteDocument: ...

    def dataservices_csv(
        self, query: SiteDataserviceCsvQuery | None = None, *, sink: Callable[[bytes], None] | None = None
    ) -> SiteDocument: ...

    def harvests_csv(self, *, sink: Callable[[bytes], None] | None = None) -> SiteDocument: ...

    def tags_csv(self, *, sink: Callable[[bytes], None] | None = None) -> SiteDocument: ...

    def jsonld_context(self) -> SiteDocument: ...


@runtime_checkable
class AsyncUDataRootProfileService(Protocol):
    """Typed asynchronous root-profile service."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    async def get(self) -> SiteProfile: ...

    async def set_site(
        self,
        client_input: SitePatchInput,
        *,
        permissions: EffectivePermissions | None,
        mutation_policy: MutationPolicy | None = None,
    ) -> SiteMutationResult: ...

    async def data_portal(self, fmt: str) -> SiteDocument: ...

    async def rdf_catalog(
        self, query: SiteCatalogQuery | None = None, *, accept: str | None = None
    ) -> SiteDocument: ...

    async def rdf_catalog_format(
        self,
        fmt: str,
        query: SiteCatalogQuery | None = None,
        *,
        sink: Callable[[bytes], Awaitable[None] | None] | None = None,
    ) -> SiteDocument: ...

    async def datasets_csv(
        self, query: SiteDatasetCsvQuery | None = None, *, sink: Callable[[bytes], Awaitable[None] | None] | None = None
    ) -> SiteDocument: ...

    async def resources_csv(
        self, query: SiteDatasetCsvQuery | None = None, *, sink: Callable[[bytes], Awaitable[None] | None] | None = None
    ) -> SiteDocument: ...

    async def organizations_csv(
        self,
        query: SiteOrganizationCsvQuery | None = None,
        *,
        sink: Callable[[bytes], Awaitable[None] | None] | None = None,
    ) -> SiteDocument: ...

    async def reuses_csv(
        self, query: SiteReuseCsvQuery | None = None, *, sink: Callable[[bytes], Awaitable[None] | None] | None = None
    ) -> SiteDocument: ...

    async def dataservices_csv(
        self,
        query: SiteDataserviceCsvQuery | None = None,
        *,
        sink: Callable[[bytes], Awaitable[None] | None] | None = None,
    ) -> SiteDocument: ...

    async def harvests_csv(self, *, sink: Callable[[bytes], Awaitable[None] | None] | None = None) -> SiteDocument: ...

    async def tags_csv(self, *, sink: Callable[[bytes], Awaitable[None] | None] | None = None) -> SiteDocument: ...

    async def jsonld_context(self) -> SiteDocument: ...


@runtime_checkable
class SyncUDataResourcesService(Protocol):
    """Typed synchronous resource and bounded-upload service."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    def redirect(self, resource_id: str) -> str: ...
    def create(
        self,
        dataset_id: str,
        client_input: ResourceCreateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ResourceMutationResult: ...
    def reorder(
        self,
        dataset_id: str,
        values: tuple[ResourceUpdateInput, ...],
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ResourceMutationResult: ...
    def upload(
        self,
        dataset_id: str,
        client_input: ResourceUploadInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
        resource_id: str | None = None,
        community: bool = False,
    ) -> ResourceMutationResult: ...
    def upload_community(
        self,
        dataset_id: str,
        client_input: ResourceUploadInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ResourceMutationResult: ...
    def reupload_community(
        self,
        resource_id: str,
        client_input: ResourceUploadInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ResourceMutationResult: ...
    def get(self, dataset_id: str, resource_id: str) -> NativeRecord: ...
    def update(
        self,
        dataset_id: str,
        resource_id: str,
        client_input: ResourceUpdateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ResourceMutationResult: ...
    def delete(
        self,
        dataset_id: str,
        resource_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ResourceMutationResult: ...
    def list_community(self, params: Mapping[str, str | int] | None = None) -> UDataResult: ...
    def create_community(
        self,
        dataset_id: str,
        client_input: ResourceCreateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ResourceMutationResult: ...
    def get_community(self, resource_id: str) -> NativeRecord: ...
    def update_community(
        self,
        resource_id: str,
        client_input: ResourceUpdateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ResourceMutationResult: ...
    def delete_community(
        self, resource_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> ResourceMutationResult: ...
    def resource_types(self) -> tuple[Mapping[str, str], ...]: ...
    def get_dataset_v2(self, dataset_id: str) -> NativeRecord: ...
    def list_v2(self, dataset_id: str) -> UDataResult: ...
    def get_v2(self, resource_id: str) -> NativeRecord: ...
    def get_extras_v2(self, dataset_id: str, resource_id: str) -> Mapping[str, object]: ...
    def update_extras_v2(
        self,
        dataset_id: str,
        resource_id: str,
        values: Mapping[str, object],
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ResourceMutationResult: ...
    def delete_extras_v2(
        self,
        dataset_id: str,
        resource_id: str,
        keys: tuple[str, ...],
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ResourceMutationResult: ...


@runtime_checkable
class AsyncUDataResourcesService(Protocol):
    """Typed asynchronous resource and bounded-upload service."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    async def redirect(self, resource_id: str) -> str: ...
    async def create(
        self,
        dataset_id: str,
        client_input: ResourceCreateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ResourceMutationResult: ...
    async def reorder(
        self,
        dataset_id: str,
        values: tuple[ResourceUpdateInput, ...],
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ResourceMutationResult: ...
    async def upload(
        self,
        dataset_id: str,
        client_input: ResourceUploadInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
        resource_id: str | None = None,
        community: bool = False,
    ) -> ResourceMutationResult: ...
    async def upload_community(
        self,
        dataset_id: str,
        client_input: ResourceUploadInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ResourceMutationResult: ...
    async def reupload_community(
        self,
        resource_id: str,
        client_input: ResourceUploadInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ResourceMutationResult: ...
    async def get(self, dataset_id: str, resource_id: str) -> NativeRecord: ...
    async def update(
        self,
        dataset_id: str,
        resource_id: str,
        client_input: ResourceUpdateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ResourceMutationResult: ...
    async def delete(
        self,
        dataset_id: str,
        resource_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ResourceMutationResult: ...
    async def list_community(self, params: Mapping[str, str | int] | None = None) -> UDataResult: ...
    async def create_community(
        self,
        dataset_id: str,
        client_input: ResourceCreateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ResourceMutationResult: ...
    async def get_community(self, resource_id: str) -> NativeRecord: ...
    async def update_community(
        self,
        resource_id: str,
        client_input: ResourceUpdateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ResourceMutationResult: ...
    async def delete_community(
        self, resource_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> ResourceMutationResult: ...
    async def resource_types(self) -> tuple[Mapping[str, str], ...]: ...
    async def get_dataset_v2(self, dataset_id: str) -> NativeRecord: ...
    async def list_v2(self, dataset_id: str) -> UDataResult: ...
    async def get_v2(self, resource_id: str) -> NativeRecord: ...
    async def get_extras_v2(self, dataset_id: str, resource_id: str) -> Mapping[str, object]: ...
    async def update_extras_v2(
        self,
        dataset_id: str,
        resource_id: str,
        values: Mapping[str, object],
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ResourceMutationResult: ...
    async def delete_extras_v2(
        self,
        dataset_id: str,
        resource_id: str,
        keys: tuple[str, ...],
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ResourceMutationResult: ...


@runtime_checkable
class SyncUDataOrganizationsMembershipsService(Protocol):
    """Typed synchronous organization and membership service."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    def list_organizations(self, query: OrganizationListQuery | None = None) -> UDataResult: ...

    def create_organization(
        self,
        client_input: OrganizationCreateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    def get_organization(self, organization_id: str) -> NativeRecord: ...

    def update_organization(
        self,
        organization_id: str,
        client_input: OrganizationUpdateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    def delete_organization(
        self, organization_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> OrganizationMutationResult: ...

    def organization_datasets_csv(self, organization_id: str) -> SiteDocument: ...

    def organization_dataservices_csv(self, organization_id: str) -> SiteDocument: ...

    def organization_discussions_csv(self, organization_id: str) -> SiteDocument: ...

    def organization_datasets_resources_csv(self, organization_id: str) -> SiteDocument: ...

    def rdf_organization(self, organization_id: str) -> SiteDocument: ...

    def rdf_organization_format(self, organization_id: str, fmt: str) -> SiteDocument: ...

    def available_organization_badges(self) -> tuple[MappingRecord, ...]: ...

    def add_organization_badge(
        self,
        organization_id: str,
        badge_kind: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    def delete_organization_badge(
        self,
        organization_id: str,
        badge_kind: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    def get_organization_contact_point(
        self, organization_id: str, query: OrganizationListQuery | None = None
    ) -> tuple[MappingRecord, ...]: ...

    def suggest_org_contact_points(
        self, organization_id: str, query: OrganizationSuggestQuery
    ) -> tuple[MappingRecord, ...]: ...

    def list_membership_requests(
        self, organization_id: str, permissions: EffectivePermissions, query: MembershipRequestQuery | None = None
    ) -> tuple[MappingRecord, ...]: ...

    def membership_request(
        self,
        organization_id: str,
        client_input: MembershipRequestInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    def accept_membership(
        self,
        organization_id: str,
        request_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    def refuse_membership(
        self,
        organization_id: str,
        request_id: str,
        client_input: OrganizationRefusalInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    def cancel_membership(
        self,
        organization_id: str,
        request_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    def invite_organization_member(
        self,
        organization_id: str,
        client_input: OrganizationInvitationInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    def update_organization_member(
        self,
        organization_id: str,
        user_id: str,
        client_input: OrganizationMemberInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    def delete_organization_member(
        self,
        organization_id: str,
        user_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    def list_organization_assignments(
        self, organization_id: str, permissions: EffectivePermissions
    ) -> tuple[MappingRecord, ...]: ...

    def sync_member_assignments(
        self,
        organization_id: str,
        user_id: str,
        assignments: list[Mapping[str, object]],
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    def suggest_organizations(self, query: OrganizationSuggestQuery) -> tuple[MappingRecord, ...]: ...

    def organization_logo(
        self,
        organization_id: str,
        client_input: OrganizationLogoInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    def resize_organization_logo(
        self,
        organization_id: str,
        client_input: OrganizationLogoInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    def list_organization_datasets(
        self, organization_id: str, query: OrganizationDatasetQuery | None = None
    ) -> NativeRecord: ...

    def list_organization_reuses(self, organization_id: str) -> tuple[MappingRecord, ...]: ...

    def list_organization_discussions(self, organization_id: str) -> tuple[MappingRecord, ...]: ...

    def org_roles(self) -> tuple[MappingRecord, ...]: ...

    def search_organizations(self, query: OrganizationListQuery | None = None) -> UDataResult: ...

    def get_organization_extras(self, organization_id: str) -> Mapping[str, object]: ...

    def update_organization_extras(
        self,
        organization_id: str,
        values: Mapping[str, object],
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    def delete_organization_extras(
        self,
        organization_id: str,
        keys: tuple[str, ...],
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    def list_organization_followers(self, organization_id: str) -> tuple[MappingRecord, ...]: ...

    def follow_organization(
        self, organization_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> OrganizationMutationResult: ...

    def unfollow_organization(
        self, organization_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> OrganizationMutationResult: ...


@runtime_checkable
class AsyncUDataOrganizationsMembershipsService(Protocol):
    """Typed asynchronous organization and membership service."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    async def list_organizations(self, query: OrganizationListQuery | None = None) -> UDataResult: ...

    async def create_organization(
        self,
        client_input: OrganizationCreateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    async def get_organization(self, organization_id: str) -> NativeRecord: ...

    async def update_organization(
        self,
        organization_id: str,
        client_input: OrganizationUpdateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    async def delete_organization(
        self, organization_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> OrganizationMutationResult: ...

    async def organization_datasets_csv(self, organization_id: str) -> SiteDocument: ...

    async def organization_dataservices_csv(self, organization_id: str) -> SiteDocument: ...

    async def organization_discussions_csv(self, organization_id: str) -> SiteDocument: ...

    async def organization_datasets_resources_csv(self, organization_id: str) -> SiteDocument: ...

    async def rdf_organization(self, organization_id: str) -> SiteDocument: ...

    async def rdf_organization_format(self, organization_id: str, fmt: str) -> SiteDocument: ...

    async def available_organization_badges(self) -> tuple[MappingRecord, ...]: ...

    async def add_organization_badge(
        self,
        organization_id: str,
        badge_kind: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    async def delete_organization_badge(
        self,
        organization_id: str,
        badge_kind: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    async def get_organization_contact_point(
        self, organization_id: str, query: OrganizationListQuery | None = None
    ) -> tuple[MappingRecord, ...]: ...

    async def suggest_org_contact_points(
        self, organization_id: str, query: OrganizationSuggestQuery
    ) -> tuple[MappingRecord, ...]: ...

    async def list_membership_requests(
        self, organization_id: str, permissions: EffectivePermissions, query: MembershipRequestQuery | None = None
    ) -> tuple[MappingRecord, ...]: ...

    async def membership_request(
        self,
        organization_id: str,
        client_input: MembershipRequestInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    async def accept_membership(
        self,
        organization_id: str,
        request_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    async def refuse_membership(
        self,
        organization_id: str,
        request_id: str,
        client_input: OrganizationRefusalInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    async def cancel_membership(
        self,
        organization_id: str,
        request_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    async def invite_organization_member(
        self,
        organization_id: str,
        client_input: OrganizationInvitationInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    async def update_organization_member(
        self,
        organization_id: str,
        user_id: str,
        client_input: OrganizationMemberInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    async def delete_organization_member(
        self,
        organization_id: str,
        user_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    async def list_organization_assignments(
        self, organization_id: str, permissions: EffectivePermissions
    ) -> tuple[MappingRecord, ...]: ...

    async def sync_member_assignments(
        self,
        organization_id: str,
        user_id: str,
        assignments: list[Mapping[str, object]],
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    async def suggest_organizations(self, query: OrganizationSuggestQuery) -> tuple[MappingRecord, ...]: ...

    async def organization_logo(
        self,
        organization_id: str,
        client_input: OrganizationLogoInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    async def resize_organization_logo(
        self,
        organization_id: str,
        client_input: OrganizationLogoInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    async def list_organization_datasets(
        self, organization_id: str, query: OrganizationDatasetQuery | None = None
    ) -> NativeRecord: ...

    async def list_organization_reuses(self, organization_id: str) -> tuple[MappingRecord, ...]: ...

    async def list_organization_discussions(self, organization_id: str) -> tuple[MappingRecord, ...]: ...

    async def org_roles(self) -> tuple[MappingRecord, ...]: ...

    async def search_organizations(self, query: OrganizationListQuery | None = None) -> UDataResult: ...

    async def get_organization_extras(self, organization_id: str) -> Mapping[str, object]: ...

    async def update_organization_extras(
        self,
        organization_id: str,
        values: Mapping[str, object],
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    async def delete_organization_extras(
        self,
        organization_id: str,
        keys: tuple[str, ...],
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OrganizationMutationResult: ...

    async def list_organization_followers(self, organization_id: str) -> tuple[MappingRecord, ...]: ...

    async def follow_organization(
        self, organization_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> OrganizationMutationResult: ...

    async def unfollow_organization(
        self, organization_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> OrganizationMutationResult: ...


@runtime_checkable
class SyncUDataActivityDiscussionsService(Protocol):
    """Typed synchronous activity and discussion operations."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    def activity(self, query: ActivityQuery) -> MappingRecord: ...
    def list_discussions(self) -> MappingRecord: ...
    def get_discussion(self, discussion_id: str) -> MappingRecord: ...
    def search_discussions(self, query: DiscussionSearchQuery) -> MappingRecord: ...
    def create_discussion(
        self,
        client_input: DiscussionCreateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> DiscussionMutationResult: ...
    def comment_discussion(
        self,
        discussion_id: str,
        client_input: CommentInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> DiscussionMutationResult: ...
    def update_discussion(
        self,
        discussion_id: str,
        client_input: DiscussionUpdateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> DiscussionMutationResult: ...
    def delete_discussion(
        self,
        discussion_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> DiscussionMutationResult: ...
    def edit_discussion_comment(
        self,
        discussion_id: str,
        comment_id: str,
        client_input: CommentInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> DiscussionMutationResult: ...
    def delete_discussion_comment(
        self,
        discussion_id: str,
        comment_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> DiscussionMutationResult: ...


@runtime_checkable
class AsyncUDataActivityDiscussionsService(Protocol):
    """Typed asynchronous activity and discussion operations."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    async def activity(self, query: ActivityQuery) -> MappingRecord: ...
    async def list_discussions(self) -> MappingRecord: ...
    async def get_discussion(self, discussion_id: str) -> MappingRecord: ...
    async def search_discussions(self, query: DiscussionSearchQuery) -> MappingRecord: ...
    async def create_discussion(
        self,
        client_input: DiscussionCreateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> DiscussionMutationResult: ...
    async def comment_discussion(
        self,
        discussion_id: str,
        client_input: CommentInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> DiscussionMutationResult: ...
    async def update_discussion(
        self,
        discussion_id: str,
        client_input: DiscussionUpdateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> DiscussionMutationResult: ...
    async def delete_discussion(
        self,
        discussion_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> DiscussionMutationResult: ...
    async def edit_discussion_comment(
        self,
        discussion_id: str,
        comment_id: str,
        client_input: CommentInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> DiscussionMutationResult: ...
    async def delete_discussion_comment(
        self,
        discussion_id: str,
        comment_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> DiscussionMutationResult: ...


@runtime_checkable
class SyncUDataSpatialService(Protocol):
    """Typed synchronous spatial zone and coverage operations."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    def suggest_zones(self, query: SpatialSuggestQuery) -> tuple[MappingRecord, ...]: ...

    def spatial_zones(self, ids: tuple[str, ...]) -> MappingRecord: ...

    def spatial_zone_datasets(
        self, zone_id: str, query: SpatialDatasetQuery | None = None
    ) -> tuple[MappingRecord, ...]: ...

    def spatial_zone(self, zone_id: str) -> MappingRecord: ...

    def spatial_levels(self) -> tuple[MappingRecord, ...]: ...

    def spatial_granularities(self) -> tuple[MappingRecord, ...]: ...

    def spatial_coverage(self, level: str) -> MappingRecord: ...


@runtime_checkable
class AsyncUDataSpatialService(Protocol):
    """Typed asynchronous spatial zone and coverage operations."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    async def suggest_zones(self, query: SpatialSuggestQuery) -> tuple[MappingRecord, ...]: ...

    async def spatial_zones(self, ids: tuple[str, ...]) -> MappingRecord: ...

    async def spatial_zone_datasets(
        self, zone_id: str, query: SpatialDatasetQuery | None = None
    ) -> tuple[MappingRecord, ...]: ...

    async def spatial_zone(self, zone_id: str) -> MappingRecord: ...

    async def spatial_levels(self) -> tuple[MappingRecord, ...]: ...

    async def spatial_granularities(self) -> tuple[MappingRecord, ...]: ...

    async def spatial_coverage(self, level: str) -> MappingRecord: ...


@runtime_checkable
class SyncUDataReusesService(Protocol):
    """Typed synchronous reuse and reuse-follower operations."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    def list_reuses(self, query: ReuseListQuery | None = None) -> MappingRecord: ...
    def create_reuse(
        self,
        client_input: ReuseCreateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ReuseMutationResult: ...
    def recent_reuses_atom_feed(self, query: ReuseListQuery | None = None) -> MappingRecord: ...
    def get_reuse(self, reuse_id: str) -> MappingRecord: ...
    def update_reuse(
        self,
        reuse_id: str,
        client_input: ReuseUpdateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ReuseMutationResult: ...
    def delete_reuse(
        self, reuse_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> ReuseMutationResult: ...
    def reuse_add_dataset(
        self,
        reuse_id: str,
        dataset_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ReuseMutationResult: ...
    def reuse_add_dataservice(
        self,
        reuse_id: str,
        dataservice_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ReuseMutationResult: ...
    def available_reuse_badges(self) -> MappingRecord: ...
    def add_reuse_badge(
        self,
        reuse_id: str,
        badge_kind: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ReuseMutationResult: ...
    def delete_reuse_badge(
        self,
        reuse_id: str,
        badge_kind: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ReuseMutationResult: ...
    def feature_reuse(
        self, reuse_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> ReuseMutationResult: ...
    def unfeature_reuse(
        self, reuse_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> ReuseMutationResult: ...
    def suggest_reuses(self, query: ReuseSuggestQuery) -> tuple[MappingRecord, ...]: ...
    def reuse_image(
        self,
        reuse_id: str,
        data: bytes,
        content_type: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ReuseMutationResult: ...
    def reuse_types(self) -> tuple[MappingRecord, ...]: ...
    def reuse_topics(self) -> tuple[MappingRecord, ...]: ...
    def search_v2(self, query: ReuseSearchQuery | None = None) -> MappingRecord: ...
    def list_v2(self, query: ReuseListQuery | None = None) -> MappingRecord: ...
    def list_reuse_followers(self, reuse_id: str, query: ReuseFollowersQuery | None = None) -> MappingRecord: ...
    def follow_reuse(
        self, reuse_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> ReuseMutationResult: ...
    def unfollow_reuse(
        self, reuse_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> ReuseMutationResult: ...


@runtime_checkable
class AsyncUDataReusesService(Protocol):
    """Typed asynchronous reuse and reuse-follower operations."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    async def list_reuses(self, query: ReuseListQuery | None = None) -> MappingRecord: ...
    async def create_reuse(
        self,
        client_input: ReuseCreateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ReuseMutationResult: ...
    async def recent_reuses_atom_feed(self, query: ReuseListQuery | None = None) -> MappingRecord: ...
    async def get_reuse(self, reuse_id: str) -> MappingRecord: ...
    async def update_reuse(
        self,
        reuse_id: str,
        client_input: ReuseUpdateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ReuseMutationResult: ...
    async def delete_reuse(
        self, reuse_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> ReuseMutationResult: ...
    async def reuse_add_dataset(
        self,
        reuse_id: str,
        dataset_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ReuseMutationResult: ...
    async def reuse_add_dataservice(
        self,
        reuse_id: str,
        dataservice_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ReuseMutationResult: ...
    async def available_reuse_badges(self) -> MappingRecord: ...
    async def add_reuse_badge(
        self,
        reuse_id: str,
        badge_kind: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ReuseMutationResult: ...
    async def delete_reuse_badge(
        self,
        reuse_id: str,
        badge_kind: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ReuseMutationResult: ...
    async def feature_reuse(
        self, reuse_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> ReuseMutationResult: ...
    async def unfeature_reuse(
        self, reuse_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> ReuseMutationResult: ...
    async def suggest_reuses(self, query: ReuseSuggestQuery) -> tuple[MappingRecord, ...]: ...
    async def reuse_image(
        self,
        reuse_id: str,
        data: bytes,
        content_type: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ReuseMutationResult: ...
    async def reuse_types(self) -> tuple[MappingRecord, ...]: ...
    async def reuse_topics(self) -> tuple[MappingRecord, ...]: ...
    async def search_v2(self, query: ReuseSearchQuery | None = None) -> MappingRecord: ...
    async def list_v2(self, query: ReuseListQuery | None = None) -> MappingRecord: ...
    async def list_reuse_followers(self, reuse_id: str, query: ReuseFollowersQuery | None = None) -> MappingRecord: ...
    async def follow_reuse(
        self, reuse_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> ReuseMutationResult: ...
    async def unfollow_reuse(
        self, reuse_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> ReuseMutationResult: ...


@runtime_checkable
class SyncUDataPostsReportsService(Protocol):
    """Typed synchronous post, report, and notification operations."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    def list_reports(self, query: ReportQuery | None = None) -> MappingRecord: ...
    def create_report(
        self,
        client_input: ReportCreateInput,
        permissions: EffectivePermissions | None = None,
        mutation_policy: MutationPolicy | None = None,
    ) -> PostMutationResult: ...
    def get_report(self, report_id: str) -> MappingRecord: ...
    def update_report(
        self,
        report_id: str,
        client_input: ReportUpdateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> PostMutationResult: ...
    def list_reports_reasons(self) -> tuple[MappingRecord, ...]: ...
    def list_posts(self, query: PostListQuery | None = None) -> MappingRecord: ...
    def create_post(
        self,
        client_input: PostCreateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> PostMutationResult: ...
    def recent_posts_atom_feed(self) -> MappingRecord: ...
    def get_post(self, post_id: str) -> MappingRecord: ...
    def update_post(
        self,
        post_id: str,
        client_input: PostUpdateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> PostMutationResult: ...
    def delete_post(
        self,
        post_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> PostMutationResult: ...
    def publish_post(
        self,
        post_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> PostMutationResult: ...
    def unpublish_post(
        self,
        post_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> PostMutationResult: ...
    def post_image(
        self,
        post_id: str,
        data: bytes,
        content_type: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> PostMutationResult: ...
    def resize_post_image(
        self,
        post_id: str,
        data: bytes,
        content_type: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> PostMutationResult: ...
    def search_posts(self, query: PostSearchQuery | None = None) -> MappingRecord: ...
    def list_notifications(
        self,
        permissions: EffectivePermissions,
        query: NotificationQuery | None = None,
    ) -> MappingRecord: ...
    def read_notification(
        self,
        notification_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> PostMutationResult: ...


@runtime_checkable
class AsyncUDataPostsReportsService(Protocol):
    """Typed asynchronous post, report, and notification operations."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    async def list_reports(self, query: ReportQuery | None = None) -> MappingRecord: ...
    async def create_report(
        self,
        client_input: ReportCreateInput,
        permissions: EffectivePermissions | None = None,
        mutation_policy: MutationPolicy | None = None,
    ) -> PostMutationResult: ...
    async def get_report(self, report_id: str) -> MappingRecord: ...
    async def update_report(
        self,
        report_id: str,
        client_input: ReportUpdateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> PostMutationResult: ...
    async def list_reports_reasons(self) -> tuple[MappingRecord, ...]: ...
    async def list_posts(self, query: PostListQuery | None = None) -> MappingRecord: ...
    async def create_post(
        self,
        client_input: PostCreateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> PostMutationResult: ...
    async def recent_posts_atom_feed(self) -> MappingRecord: ...
    async def get_post(self, post_id: str) -> MappingRecord: ...
    async def update_post(
        self,
        post_id: str,
        client_input: PostUpdateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> PostMutationResult: ...
    async def delete_post(
        self,
        post_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> PostMutationResult: ...
    async def publish_post(
        self,
        post_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> PostMutationResult: ...
    async def unpublish_post(
        self,
        post_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> PostMutationResult: ...
    async def post_image(
        self,
        post_id: str,
        data: bytes,
        content_type: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> PostMutationResult: ...
    async def resize_post_image(
        self,
        post_id: str,
        data: bytes,
        content_type: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> PostMutationResult: ...
    async def search_posts(self, query: PostSearchQuery | None = None) -> MappingRecord: ...
    async def list_notifications(
        self,
        permissions: EffectivePermissions,
        query: NotificationQuery | None = None,
    ) -> MappingRecord: ...
    async def read_notification(
        self,
        notification_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> PostMutationResult: ...


@runtime_checkable
class SyncUDataTaxonomiesService(Protocol):
    """Typed synchronous taxonomy, schema, format, and badge operations."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    def available_badges(self) -> MappingRecord: ...
    def add_badge(
        self,
        dataset_id: str,
        client_input: BadgeCreateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> TaxonomyMutationResult: ...
    def delete_badge(
        self,
        dataset_id: str,
        kind: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> TaxonomyMutationResult: ...
    def suggest_formats(self, query: SuggestQuery) -> tuple[MappingRecord, ...]: ...
    def suggest_mime(self, query: SuggestQuery) -> tuple[MappingRecord, ...]: ...
    def licenses(self) -> tuple[MappingRecord, ...]: ...
    def frequencies(self) -> tuple[MappingRecord, ...]: ...
    def extensions(self) -> tuple[str, ...]: ...
    def schemas(self) -> tuple[MappingRecord, ...]: ...
    def dataset_schemas(self, dataset_id: str) -> tuple[MappingRecord, ...]: ...


@runtime_checkable
class AsyncUDataTaxonomiesService(Protocol):
    """Typed asynchronous taxonomy, schema, format, and badge operations."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    async def available_badges(self) -> MappingRecord: ...
    async def add_badge(
        self,
        dataset_id: str,
        client_input: BadgeCreateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> TaxonomyMutationResult: ...
    async def delete_badge(
        self,
        dataset_id: str,
        kind: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> TaxonomyMutationResult: ...
    async def suggest_formats(self, query: SuggestQuery) -> tuple[MappingRecord, ...]: ...
    async def suggest_mime(self, query: SuggestQuery) -> tuple[MappingRecord, ...]: ...
    async def licenses(self) -> tuple[MappingRecord, ...]: ...
    async def frequencies(self) -> tuple[MappingRecord, ...]: ...
    async def extensions(self) -> tuple[str, ...]: ...
    async def schemas(self) -> tuple[MappingRecord, ...]: ...
    async def dataset_schemas(self, dataset_id: str) -> tuple[MappingRecord, ...]: ...


@runtime_checkable
class SyncUDataUsersTokensService(Protocol):
    """Named synchronous stock user and token operations."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    def get_me(self, permissions: EffectivePermissions) -> NativeRecord: ...
    def update_me(
        self,
        client_input: UserUpdateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> UserMutationResult: ...
    def delete_me(
        self, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> UserMutationResult: ...
    def my_avatar(
        self,
        client_input: UserAvatarInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> UserMutationResult: ...
    def my_reuses(self, permissions: EffectivePermissions) -> tuple[MappingRecord, ...]: ...
    def my_datasets(self, permissions: EffectivePermissions) -> tuple[MappingRecord, ...]: ...
    def my_metrics(self, permissions: EffectivePermissions) -> MappingRecord: ...
    def my_org_datasets(self, permissions: EffectivePermissions, q: str | None = None) -> tuple[MappingRecord, ...]: ...
    def my_org_community_resources(
        self, permissions: EffectivePermissions, q: str | None = None
    ) -> tuple[MappingRecord, ...]: ...
    def my_org_reuses(self, permissions: EffectivePermissions, q: str | None = None) -> tuple[MappingRecord, ...]: ...
    def my_org_discussions(
        self, permissions: EffectivePermissions, q: str | None = None
    ) -> tuple[MappingRecord, ...]: ...
    def list_api_tokens(self, permissions: EffectivePermissions) -> tuple[ApiTokenMetadata, ...]: ...
    def create_api_token(
        self,
        client_input: ApiTokenCreateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ApiTokenCreationResult: ...
    def revoke_api_token(
        self, token_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> UserMutationResult: ...
    def list_org_invitations(self, permissions: EffectivePermissions) -> tuple[MappingRecord, ...]: ...
    def accept_org_invitation(
        self, invitation_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> UserMutationResult: ...
    def refuse_org_invitation(
        self, invitation_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> UserMutationResult: ...
    def list_users(self, permissions: EffectivePermissions, query: UserListQuery | None = None) -> UDataResult: ...
    def create_user(
        self,
        client_input: UserCreateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> UserMutationResult: ...
    def user_avatar(
        self,
        user_id: str,
        client_input: UserAvatarInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> UserMutationResult: ...
    def get_user(self, user_id: str) -> NativeRecord: ...
    def update_user(
        self,
        user_id: str,
        client_input: UserUpdateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> UserMutationResult: ...
    def delete_user(
        self,
        user_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
        options: UserDeleteOptions | None = None,
    ) -> UserMutationResult: ...
    def rotate_user_password(
        self, user_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> UserMutationResult: ...
    def get_user_contact_point(self, user_id: str, query: UserListQuery | None = None) -> UDataResult: ...
    def follow_user(
        self, user_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> UserMutationResult: ...
    def suggest_users(self, query: UserSuggestQuery) -> tuple[MappingRecord, ...]: ...
    def user_roles(self) -> tuple[MappingRecord, ...]: ...
    def my_org_topics(self, permissions: EffectivePermissions, query: UserListQuery | None = None) -> UDataResult: ...
    def list_user_followers(self, user_id: str, query: UserListQuery | None = None) -> UDataResult: ...
    def unfollow_user(
        self, user_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> UserMutationResult: ...


@runtime_checkable
class AsyncUDataUsersTokensService(Protocol):
    """Named asynchronous stock user and token operations."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    async def get_me(self, permissions: EffectivePermissions) -> NativeRecord: ...
    async def update_me(
        self,
        client_input: UserUpdateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> UserMutationResult: ...
    async def delete_me(
        self, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> UserMutationResult: ...
    async def my_avatar(
        self,
        client_input: UserAvatarInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> UserMutationResult: ...
    async def my_reuses(self, permissions: EffectivePermissions) -> tuple[MappingRecord, ...]: ...
    async def my_datasets(self, permissions: EffectivePermissions) -> tuple[MappingRecord, ...]: ...
    async def my_metrics(self, permissions: EffectivePermissions) -> MappingRecord: ...
    async def my_org_datasets(
        self, permissions: EffectivePermissions, q: str | None = None
    ) -> tuple[MappingRecord, ...]: ...
    async def my_org_community_resources(
        self, permissions: EffectivePermissions, q: str | None = None
    ) -> tuple[MappingRecord, ...]: ...
    async def my_org_reuses(
        self, permissions: EffectivePermissions, q: str | None = None
    ) -> tuple[MappingRecord, ...]: ...
    async def my_org_discussions(
        self, permissions: EffectivePermissions, q: str | None = None
    ) -> tuple[MappingRecord, ...]: ...
    async def list_api_tokens(self, permissions: EffectivePermissions) -> tuple[ApiTokenMetadata, ...]: ...
    async def create_api_token(
        self,
        client_input: ApiTokenCreateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> ApiTokenCreationResult: ...
    async def revoke_api_token(
        self, token_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> UserMutationResult: ...
    async def list_org_invitations(self, permissions: EffectivePermissions) -> tuple[MappingRecord, ...]: ...
    async def accept_org_invitation(
        self, invitation_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> UserMutationResult: ...
    async def refuse_org_invitation(
        self, invitation_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> UserMutationResult: ...
    async def list_users(
        self, permissions: EffectivePermissions, query: UserListQuery | None = None
    ) -> UDataResult: ...
    async def create_user(
        self,
        client_input: UserCreateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> UserMutationResult: ...
    async def user_avatar(
        self,
        user_id: str,
        client_input: UserAvatarInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> UserMutationResult: ...
    async def get_user(self, user_id: str) -> NativeRecord: ...
    async def update_user(
        self,
        user_id: str,
        client_input: UserUpdateInput,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> UserMutationResult: ...
    async def delete_user(
        self,
        user_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
        options: UserDeleteOptions | None = None,
    ) -> UserMutationResult: ...
    async def rotate_user_password(
        self, user_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> UserMutationResult: ...
    async def get_user_contact_point(self, user_id: str, query: UserListQuery | None = None) -> UDataResult: ...
    async def follow_user(
        self, user_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> UserMutationResult: ...
    async def suggest_users(self, query: UserSuggestQuery) -> tuple[MappingRecord, ...]: ...
    async def user_roles(self) -> tuple[MappingRecord, ...]: ...
    async def my_org_topics(
        self, permissions: EffectivePermissions, query: UserListQuery | None = None
    ) -> UDataResult: ...
    async def list_user_followers(self, user_id: str, query: UserListQuery | None = None) -> UDataResult: ...
    async def unfollow_user(
        self, user_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> UserMutationResult: ...


@runtime_checkable
class SyncUDataAuthOAuthService(Protocol):
    """Synchronous stock uData /oauth routes."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    def access_token(self, body: OAuthTokenRequest, permissions: EffectivePermissions) -> OAuthTokenResult: ...
    def revoke_token(
        self,
        body: OAuthRevokeRequest,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OAuthTokenResult: ...
    def client_info(self, query: OAuthClientRequest, permissions: EffectivePermissions) -> OAuthConsentSummary: ...
    def authorize(self, query: OAuthClientRequest, permissions: EffectivePermissions) -> OAuthConsentSummary: ...
    def authorize_post(
        self,
        body: OAuthAuthorizeDecision,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OAuthConsentOutcome: ...
    def oauth_error(self) -> OAuthErrorDocument: ...


@runtime_checkable
class AsyncUDataAuthOAuthService(Protocol):
    """Asynchronous stock uData /oauth routes."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    async def access_token(self, body: OAuthTokenRequest, permissions: EffectivePermissions) -> OAuthTokenResult: ...
    async def revoke_token(
        self,
        body: OAuthRevokeRequest,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OAuthTokenResult: ...
    async def client_info(
        self, query: OAuthClientRequest, permissions: EffectivePermissions
    ) -> OAuthConsentSummary: ...
    async def authorize(self, query: OAuthClientRequest, permissions: EffectivePermissions) -> OAuthConsentSummary: ...
    async def authorize_post(
        self,
        body: OAuthAuthorizeDecision,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> OAuthConsentOutcome: ...
    async def oauth_error(self) -> OAuthErrorDocument: ...


@runtime_checkable
class SyncUDataService(Protocol):
    """Synchronous uData operation group."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    def execute(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> UDataResult: ...


@runtime_checkable
class AsyncUDataService(Protocol):
    """Asynchronous uData operation group."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    async def execute(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> UDataResult: ...


@runtime_checkable
class SyncUDataServices(Protocol):
    """Complete synchronous uData service projection."""

    @property
    def root_profile(self) -> SyncUDataRootProfileService: ...

    @property
    def datasets(self) -> SyncUDataService: ...

    @property
    def resources(self) -> SyncUDataResourcesService: ...

    @property
    def reuses(self) -> SyncUDataReusesService: ...

    @property
    def posts_reports(self) -> SyncUDataPostsReportsService: ...

    @property
    def organizations_memberships(self) -> SyncUDataOrganizationsMembershipsService: ...

    @property
    def users_tokens(self) -> SyncUDataUsersTokensService: ...

    @property
    def auth_oauth(self) -> SyncUDataAuthOAuthService: ...

    @property
    def spatial(self) -> SyncUDataSpatialService: ...

    @property
    def taxonomies(self) -> SyncUDataTaxonomiesService: ...

    @property
    def activity_discussions(self) -> SyncUDataActivityDiscussionsService: ...

    @property
    def social(self) -> SyncUDataService: ...

    @property
    def geography(self) -> SyncUDataService: ...

    @property
    def harvest_moderation_admin(self) -> SyncUDataService: ...

    @property
    def extensions(self) -> SyncUDataService: ...


@runtime_checkable
class AsyncUDataServices(Protocol):
    """Complete asynchronous uData service projection."""

    @property
    def root_profile(self) -> AsyncUDataRootProfileService: ...

    @property
    def datasets(self) -> AsyncUDataService: ...

    @property
    def resources(self) -> AsyncUDataResourcesService: ...

    @property
    def reuses(self) -> AsyncUDataReusesService: ...

    @property
    def posts_reports(self) -> AsyncUDataPostsReportsService: ...

    @property
    def organizations_memberships(self) -> AsyncUDataOrganizationsMembershipsService: ...

    @property
    def users_tokens(self) -> AsyncUDataUsersTokensService: ...

    @property
    def auth_oauth(self) -> AsyncUDataAuthOAuthService: ...

    @property
    def spatial(self) -> AsyncUDataSpatialService: ...

    @property
    def taxonomies(self) -> AsyncUDataTaxonomiesService: ...

    @property
    def activity_discussions(self) -> AsyncUDataActivityDiscussionsService: ...

    @property
    def social(self) -> AsyncUDataService: ...

    @property
    def geography(self) -> AsyncUDataService: ...

    @property
    def harvest_moderation_admin(self) -> AsyncUDataService: ...

    @property
    def extensions(self) -> AsyncUDataService: ...
