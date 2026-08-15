"""Typed Socrata SODA 3 Protocol groups."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.models import NativeRecord, ResultEnvelope
from datasluice.errors.catalog import NativeCatalogError

type SocrataResult = ResultEnvelope[NativeRecord]


@runtime_checkable
class SyncSocrataService(Protocol):
    """Synchronous Socrata operation group."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    def execute(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> SocrataResult: ...


@runtime_checkable
class AsyncSocrataService(Protocol):
    """Asynchronous Socrata operation group."""

    @property
    def error_type(self) -> type[NativeCatalogError]: ...

    async def execute(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> SocrataResult: ...


@runtime_checkable
class SyncSocrataServices(Protocol):
    """Complete synchronous Socrata SODA 3 service projection."""

    @property
    def soda(self) -> SyncSocrataService: ...

    @property
    def catalog(self) -> SyncSocrataService: ...

    @property
    def assets_permissions(self) -> SyncSocrataService: ...

    @property
    def identity_permissions(self) -> SyncSocrataService: ...

    @property
    def auth(self) -> SyncSocrataService: ...

    @property
    def async_status(self) -> SyncSocrataService: ...


@runtime_checkable
class AsyncSocrataServices(Protocol):
    """Complete asynchronous Socrata SODA 3 service projection."""

    @property
    def soda(self) -> AsyncSocrataService: ...

    @property
    def catalog(self) -> AsyncSocrataService: ...

    @property
    def assets_permissions(self) -> AsyncSocrataService: ...

    @property
    def identity_permissions(self) -> AsyncSocrataService: ...

    @property
    def auth(self) -> AsyncSocrataService: ...

    @property
    def async_status(self) -> AsyncSocrataService: ...
