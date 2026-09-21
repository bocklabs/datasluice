"""Typed uData native Protocol groups."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.auth import EffectivePermissions
from datasluice.domain.catalog.models import NativeRecord, ResultEnvelope
from datasluice.domain.catalog.safety import MutationPolicy
from datasluice.errors.catalog import NativeCatalogError

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.mapping import UDataPageEnvelope
    from datasluice.connectors.catalog.udata.models.resources import (
        ResourceCreateInput,
        ResourceMutationResult,
        ResourceUpdateInput,
        ResourceUploadInput,
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
    def list_community(self, params: Mapping[str, str | int] | None = None) -> UDataPageEnvelope: ...
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
    def list_v2(self, dataset_id: str) -> UDataPageEnvelope: ...
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
    async def list_community(self, params: Mapping[str, str | int] | None = None) -> UDataPageEnvelope: ...
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
    async def list_v2(self, dataset_id: str) -> UDataPageEnvelope: ...
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

    def list_organizations(self, query: object | None = None) -> object: ...
    def create_organization(
        self, client_input: object, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> object: ...
    def get_organization(self, organization_id: str) -> object: ...
    def update_organization(
        self,
        organization_id: str,
        client_input: object,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    def delete_organization(
        self, organization_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> object: ...
    def organization_datasets_csv(self, organization_id: str) -> object: ...
    def organization_dataservices_csv(self, organization_id: str) -> object: ...
    def organization_discussions_csv(self, organization_id: str) -> object: ...
    def organization_datasets_resources_csv(self, organization_id: str) -> object: ...
    def rdf_organization(self, organization_id: str) -> object: ...
    def rdf_organization_format(self, organization_id: str, fmt: str) -> object: ...
    def available_organization_badges(self) -> object: ...
    def add_organization_badge(
        self,
        organization_id: str,
        badge_kind: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    def delete_organization_badge(
        self,
        organization_id: str,
        badge_kind: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    def get_organization_contact_point(self, organization_id: str, query: object | None = None) -> object: ...
    def suggest_org_contact_points(self, organization_id: str, query: object) -> object: ...
    def list_membership_requests(
        self, organization_id: str, permissions: EffectivePermissions, query: object | None = None
    ) -> object: ...
    def membership_request(
        self,
        organization_id: str,
        client_input: object,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    def accept_membership(
        self,
        organization_id: str,
        request_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    def refuse_membership(
        self,
        organization_id: str,
        request_id: str,
        client_input: object,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    def cancel_membership(
        self,
        organization_id: str,
        request_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    def invite_organization_member(
        self,
        organization_id: str,
        client_input: object,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    def update_organization_member(
        self,
        organization_id: str,
        user_id: str,
        client_input: object,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    def delete_organization_member(
        self,
        organization_id: str,
        user_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    def list_organization_assignments(self, organization_id: str, permissions: EffectivePermissions) -> object: ...
    def sync_member_assignments(
        self,
        organization_id: str,
        user_id: str,
        assignments: list[Mapping[str, object]],
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    def suggest_organizations(self, query: object) -> object: ...
    def organization_logo(
        self,
        organization_id: str,
        client_input: object,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    def resize_organization_logo(
        self,
        organization_id: str,
        client_input: object,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    def list_organization_datasets(self, organization_id: str, query: object | None = None) -> object: ...
    def list_organization_reuses(self, organization_id: str) -> object: ...
    def list_organization_discussions(self, organization_id: str) -> object: ...
    def org_roles(self) -> object: ...
    def search_organizations(self, query: object | None = None) -> object: ...
    def get_organization_extras(self, organization_id: str) -> object: ...
    def update_organization_extras(
        self,
        organization_id: str,
        values: Mapping[str, object],
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    def delete_organization_extras(
        self,
        organization_id: str,
        keys: tuple[str, ...],
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    def list_organization_followers(self, organization_id: str) -> object: ...
    def follow_organization(
        self, organization_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> object: ...
    def unfollow_organization(
        self, organization_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> object: ...


@runtime_checkable
class AsyncUDataOrganizationsMembershipsService(Protocol):
    """Typed asynchronous organization and membership service."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    async def list_organizations(self, query: object | None = None) -> object: ...
    async def create_organization(
        self, client_input: object, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> object: ...
    async def get_organization(self, organization_id: str) -> object: ...
    async def update_organization(
        self,
        organization_id: str,
        client_input: object,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    async def delete_organization(
        self, organization_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> object: ...
    async def organization_datasets_csv(self, organization_id: str) -> object: ...
    async def organization_dataservices_csv(self, organization_id: str) -> object: ...
    async def organization_discussions_csv(self, organization_id: str) -> object: ...
    async def organization_datasets_resources_csv(self, organization_id: str) -> object: ...
    async def rdf_organization(self, organization_id: str) -> object: ...
    async def rdf_organization_format(self, organization_id: str, fmt: str) -> object: ...
    async def available_organization_badges(self) -> object: ...
    async def add_organization_badge(
        self,
        organization_id: str,
        badge_kind: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    async def delete_organization_badge(
        self,
        organization_id: str,
        badge_kind: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    async def get_organization_contact_point(self, organization_id: str, query: object | None = None) -> object: ...
    async def suggest_org_contact_points(self, organization_id: str, query: object) -> object: ...
    async def list_membership_requests(
        self, organization_id: str, permissions: EffectivePermissions, query: object | None = None
    ) -> object: ...
    async def membership_request(
        self,
        organization_id: str,
        client_input: object,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    async def accept_membership(
        self,
        organization_id: str,
        request_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    async def refuse_membership(
        self,
        organization_id: str,
        request_id: str,
        client_input: object,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    async def cancel_membership(
        self,
        organization_id: str,
        request_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    async def invite_organization_member(
        self,
        organization_id: str,
        client_input: object,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    async def update_organization_member(
        self,
        organization_id: str,
        user_id: str,
        client_input: object,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    async def delete_organization_member(
        self,
        organization_id: str,
        user_id: str,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    async def list_organization_assignments(
        self, organization_id: str, permissions: EffectivePermissions
    ) -> object: ...
    async def sync_member_assignments(
        self,
        organization_id: str,
        user_id: str,
        assignments: list[Mapping[str, object]],
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    async def suggest_organizations(self, query: object) -> object: ...
    async def organization_logo(
        self,
        organization_id: str,
        client_input: object,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    async def resize_organization_logo(
        self,
        organization_id: str,
        client_input: object,
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    async def list_organization_datasets(self, organization_id: str, query: object | None = None) -> object: ...
    async def list_organization_reuses(self, organization_id: str) -> object: ...
    async def list_organization_discussions(self, organization_id: str) -> object: ...
    async def org_roles(self) -> object: ...
    async def search_organizations(self, query: object | None = None) -> object: ...
    async def get_organization_extras(self, organization_id: str) -> object: ...
    async def update_organization_extras(
        self,
        organization_id: str,
        values: Mapping[str, object],
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    async def delete_organization_extras(
        self,
        organization_id: str,
        keys: tuple[str, ...],
        permissions: EffectivePermissions,
        mutation_policy: MutationPolicy | None = None,
    ) -> object: ...
    async def list_organization_followers(self, organization_id: str) -> object: ...
    async def follow_organization(
        self, organization_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> object: ...
    async def unfollow_organization(
        self, organization_id: str, permissions: EffectivePermissions, mutation_policy: MutationPolicy | None = None
    ) -> object: ...


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
    def organizations_memberships(self) -> SyncUDataOrganizationsMembershipsService: ...

    @property
    def users_tokens(self) -> SyncUDataService: ...

    @property
    def auth_oauth(self) -> SyncUDataService: ...

    @property
    def taxonomies(self) -> SyncUDataService: ...

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
    def organizations_memberships(self) -> AsyncUDataOrganizationsMembershipsService: ...

    @property
    def users_tokens(self) -> AsyncUDataService: ...

    @property
    def auth_oauth(self) -> AsyncUDataService: ...

    @property
    def taxonomies(self) -> AsyncUDataService: ...

    @property
    def social(self) -> AsyncUDataService: ...

    @property
    def geography(self) -> AsyncUDataService: ...

    @property
    def harvest_moderation_admin(self) -> AsyncUDataService: ...

    @property
    def extensions(self) -> AsyncUDataService: ...
