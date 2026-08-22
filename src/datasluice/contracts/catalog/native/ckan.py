"""Typed CKAN Action API Protocol groups."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.models import MappingRecord, NativeRecord, ResultEnvelope, ValueRecord
from datasluice.errors.catalog import NativeCatalogError

type CKANResultItem = NativeRecord | ValueRecord | MappingRecord
type CKANResult = ResultEnvelope[CKANResultItem]


@runtime_checkable
class SyncCKANService(Protocol):
    """Common synchronous CKAN native service contract."""

    @property
    def error_type(self) -> type[NativeCatalogError]:
        """Return the native CKAN error type."""


@runtime_checkable
class AsyncCKANService(Protocol):
    """Common asynchronous CKAN native service contract."""

    @property
    def error_type(self) -> type[NativeCatalogError]:
        """Return the native CKAN error type."""


@runtime_checkable
class SyncCKANActionDiscoveryService(SyncCKANService, Protocol):
    """Synchronous CKAN Action API discovery operations."""

    def discovery_help_and_status(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> CKANResult:
        """Probe Action API help and status."""


@runtime_checkable
class AsyncCKANActionDiscoveryService(AsyncCKANService, Protocol):
    """Asynchronous CKAN Action API discovery operations."""

    async def discovery_help_and_status(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> CKANResult:
        """Probe Action API help and status."""


@runtime_checkable
class SyncCKANDatasetService(SyncCKANService, Protocol):
    """Synchronous CKAN dataset operations."""

    def list_show_search(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> CKANResult:
        """List, show, or search datasets."""

    def create_update_patch_delete_purge(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> CKANResult:
        """Mutate or purge a dataset."""


@runtime_checkable
class AsyncCKANDatasetService(AsyncCKANService, Protocol):
    """Asynchronous CKAN dataset operations."""

    async def list_show_search(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> CKANResult:
        """List, show, or search datasets."""

    async def create_update_patch_delete_purge(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> CKANResult:
        """Mutate or purge a dataset."""


@runtime_checkable
class SyncCKANResourceService(SyncCKANService, Protocol):
    """Synchronous CKAN resource operations."""

    def list_show_create_update_patch_delete_upload(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> CKANResult:
        """List, read, mutate, or upload a resource."""


@runtime_checkable
class AsyncCKANResourceService(AsyncCKANService, Protocol):
    """Asynchronous CKAN resource operations."""

    async def list_show_create_update_patch_delete_upload(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> CKANResult:
        """List, read, mutate, or upload a resource."""


@runtime_checkable
class SyncCKANOrganizationService(SyncCKANService, Protocol):
    """Synchronous CKAN organization operations."""

    def list_show_create_update_delete_members(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> CKANResult:
        """Manage organizations and membership."""


@runtime_checkable
class AsyncCKANOrganizationService(AsyncCKANService, Protocol):
    """Asynchronous CKAN organization operations."""

    async def list_show_create_update_delete_members(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> CKANResult:
        """Manage organizations and membership."""


@runtime_checkable
class SyncCKANGroupService(SyncCKANService, Protocol):
    """Synchronous CKAN group operations."""

    def list_show_create_update_delete_members(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> CKANResult:
        """Manage groups and membership."""


@runtime_checkable
class AsyncCKANGroupService(AsyncCKANService, Protocol):
    """Asynchronous CKAN group operations."""

    async def list_show_create_update_delete_members(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> CKANResult:
        """Manage groups and membership."""


@runtime_checkable
class SyncCKANUserService(SyncCKANService, Protocol):
    """Synchronous CKAN user operations."""

    def list_show_create_update_delete_token_management(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> CKANResult:
        """Manage users and API tokens."""


@runtime_checkable
class AsyncCKANUserService(AsyncCKANService, Protocol):
    """Asynchronous CKAN user operations."""

    async def list_show_create_update_delete_token_management(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> CKANResult:
        """Manage users and API tokens."""


@runtime_checkable
class SyncCKANVocabularyLicenseService(SyncCKANService, Protocol):
    """Synchronous CKAN vocabulary and license operations."""

    def tags_vocabularies_and_licenses(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> CKANResult:
        """Read or manage tags, vocabularies, and licenses."""


@runtime_checkable
class AsyncCKANVocabularyLicenseService(AsyncCKANService, Protocol):
    """Asynchronous CKAN vocabulary and license operations."""

    async def tags_vocabularies_and_licenses(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> CKANResult:
        """Read or manage tags, vocabularies, and licenses."""


@runtime_checkable
class SyncCKANRelationshipActivityService(SyncCKANService, Protocol):
    """Synchronous CKAN relationship and activity operations."""

    def relationships_followers_and_activity(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> CKANResult:
        """Manage relationships, followers, and activity."""


@runtime_checkable
class AsyncCKANRelationshipActivityService(AsyncCKANService, Protocol):
    """Asynchronous CKAN relationship and activity operations."""

    async def relationships_followers_and_activity(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> CKANResult:
        """Manage relationships, followers, and activity."""


@runtime_checkable
class SyncCKANViewService(SyncCKANService, Protocol):
    """Synchronous CKAN resource-view operations."""

    def resource_views(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> CKANResult:
        """Manage resource views."""


@runtime_checkable
class AsyncCKANViewService(AsyncCKANService, Protocol):
    """Asynchronous CKAN resource-view operations."""

    async def resource_views(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> CKANResult:
        """Manage resource views."""


@runtime_checkable
class SyncCKANDatastoreService(SyncCKANService, Protocol):
    """Synchronous CKAN datastore operations."""

    def query_and_record_crud(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> CKANResult:
        """Query or mutate datastore records."""


@runtime_checkable
class AsyncCKANDatastoreService(AsyncCKANService, Protocol):
    """Asynchronous CKAN datastore operations."""

    async def query_and_record_crud(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> CKANResult:
        """Query or mutate datastore records."""


@runtime_checkable
class SyncCKANFilestoreService(SyncCKANService, Protocol):
    """Synchronous CKAN filestore operations."""

    def upload_and_resource_file_replacement(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> CKANResult:
        """Upload or replace resource files."""


@runtime_checkable
class AsyncCKANFilestoreService(AsyncCKANService, Protocol):
    """Asynchronous CKAN filestore operations."""

    async def upload_and_resource_file_replacement(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> CKANResult:
        """Upload or replace resource files."""


@runtime_checkable
class SyncCKANExtensionService(SyncCKANService, Protocol):
    """Synchronous CKAN extension probe operations."""

    def extension_probes(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> CKANResult:
        """Probe deployment-provided actions and extensions."""


@runtime_checkable
class AsyncCKANExtensionService(AsyncCKANService, Protocol):
    """Asynchronous CKAN extension probe operations."""

    async def extension_probes(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> CKANResult:
        """Probe deployment-provided actions and extensions."""


@runtime_checkable
class SyncCKANServices(Protocol):
    """Complete synchronous CKAN Action API service projection."""

    @property
    def action_discovery(self) -> SyncCKANActionDiscoveryService: ...

    @property
    def datasets(self) -> SyncCKANDatasetService: ...

    @property
    def resources(self) -> SyncCKANResourceService: ...

    @property
    def organizations(self) -> SyncCKANOrganizationService: ...

    @property
    def groups(self) -> SyncCKANGroupService: ...

    @property
    def users(self) -> SyncCKANUserService: ...

    @property
    def vocabularies_licenses(self) -> SyncCKANVocabularyLicenseService: ...

    @property
    def relationships_activity(self) -> SyncCKANRelationshipActivityService: ...

    @property
    def views(self) -> SyncCKANViewService: ...

    @property
    def datastore(self) -> SyncCKANDatastoreService: ...

    @property
    def filestore(self) -> SyncCKANFilestoreService: ...

    @property
    def extensions(self) -> SyncCKANExtensionService: ...


@runtime_checkable
class AsyncCKANServices(Protocol):
    """Complete asynchronous CKAN Action API service projection."""

    @property
    def action_discovery(self) -> AsyncCKANActionDiscoveryService: ...

    @property
    def datasets(self) -> AsyncCKANDatasetService: ...

    @property
    def resources(self) -> AsyncCKANResourceService: ...

    @property
    def organizations(self) -> AsyncCKANOrganizationService: ...

    @property
    def groups(self) -> AsyncCKANGroupService: ...

    @property
    def users(self) -> AsyncCKANUserService: ...

    @property
    def vocabularies_licenses(self) -> AsyncCKANVocabularyLicenseService: ...

    @property
    def relationships_activity(self) -> AsyncCKANRelationshipActivityService: ...

    @property
    def views(self) -> AsyncCKANViewService: ...

    @property
    def datastore(self) -> AsyncCKANDatastoreService: ...

    @property
    def filestore(self) -> AsyncCKANFilestoreService: ...

    @property
    def extensions(self) -> AsyncCKANExtensionService: ...
