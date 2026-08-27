"""Typed uData native Protocol groups."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.models import NativeRecord, ResultEnvelope
from datasluice.errors.catalog import NativeCatalogError

type UDataResultItem = NativeRecord
type UDataResult = ResultEnvelope[UDataResultItem]


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
    def root_profile(self) -> SyncUDataService: ...

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
    def root_profile(self) -> AsyncUDataService: ...

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
