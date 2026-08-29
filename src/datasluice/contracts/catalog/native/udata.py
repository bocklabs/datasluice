"""Typed uData native Protocol groups."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.auth import EffectivePermissions
from datasluice.domain.catalog.models import NativeRecord, ResultEnvelope
from datasluice.domain.catalog.safety import MutationPolicy
from datasluice.errors.catalog import NativeCatalogError

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.models.root_profile import (
        SiteCatalogQuery,
        SiteDocument,
        SiteMutationResult,
        SitePatchInput,
        SiteProfile,
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

    def rdf_catalog_format(self, fmt: str, query: SiteCatalogQuery | None = None) -> SiteDocument: ...

    def datasets_csv(self, query: SiteCatalogQuery | None = None) -> SiteDocument: ...

    def resources_csv(self, query: SiteCatalogQuery | None = None) -> SiteDocument: ...

    def organizations_csv(self, query: SiteCatalogQuery | None = None) -> SiteDocument: ...

    def reuses_csv(self, query: SiteCatalogQuery | None = None) -> SiteDocument: ...

    def dataservices_csv(self, query: SiteCatalogQuery | None = None) -> SiteDocument: ...

    def harvests_csv(self) -> SiteDocument: ...

    def tags_csv(self) -> SiteDocument: ...

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

    async def rdf_catalog_format(self, fmt: str, query: SiteCatalogQuery | None = None) -> SiteDocument: ...

    async def datasets_csv(self, query: SiteCatalogQuery | None = None) -> SiteDocument: ...

    async def resources_csv(self, query: SiteCatalogQuery | None = None) -> SiteDocument: ...

    async def organizations_csv(self, query: SiteCatalogQuery | None = None) -> SiteDocument: ...

    async def reuses_csv(self, query: SiteCatalogQuery | None = None) -> SiteDocument: ...

    async def dataservices_csv(self, query: SiteCatalogQuery | None = None) -> SiteDocument: ...

    async def harvests_csv(self) -> SiteDocument: ...

    async def tags_csv(self) -> SiteDocument: ...

    async def jsonld_context(self) -> SiteDocument: ...


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
    def resources(self) -> SyncUDataService: ...

    @property
    def organizations_memberships(self) -> SyncUDataService: ...

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
    def resources(self) -> AsyncUDataService: ...

    @property
    def organizations_memberships(self) -> AsyncUDataService: ...

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
