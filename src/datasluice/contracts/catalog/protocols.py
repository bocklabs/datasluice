"""Typed normalized catalog Protocols and injected executor seams."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator, Mapping
from dataclasses import dataclass, field
from types import TracebackType
from typing import Literal, Protocol, Self, runtime_checkable

from datasluice.domain.catalog.auth import CatalogCredential, EffectivePermissions
from datasluice.domain.catalog.models import DatasetRecord, OrganizationRecord, ResourceRecord, ResultEnvelope
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.profiles import EffectiveCapabilityProfile
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.safety import MutationPolicy

CapabilityState = Literal["available", "unavailable"]


@dataclass(frozen=True, slots=True)
class CatalogOperationRequest:
    """An immutable request routed through one declared catalog operation."""

    operation_id: OperationId
    payload: Mapping[str, object] = field(default_factory=dict)
    mutation_policy: MutationPolicy | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.operation_id, OperationId):
            raise ValueError("Catalog operation requests require an OperationId.")
        if not isinstance(self.payload, Mapping) or not all(isinstance(key, str) and key for key in self.payload):
            raise ValueError("Catalog operation request payloads require non-empty string keys.")
        if self.mutation_policy is not None and not isinstance(self.mutation_policy, MutationPolicy):
            raise ValueError("Catalog operation requests require a typed mutation policy.")


@dataclass(frozen=True, slots=True)
class CatalogOperationGuard:
    """Effective capability and permission evidence required before dispatch."""

    operation_id: OperationId
    profile: EffectiveCapabilityProfile | None = None
    permissions: EffectivePermissions | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.operation_id, OperationId):
            raise ValueError("Catalog operation guards require an OperationId.")
        if self.profile is not None and not isinstance(self.profile, EffectiveCapabilityProfile):
            raise ValueError("Catalog operation guards require an effective capability profile.")
        if self.permissions is not None and not isinstance(self.permissions, EffectivePermissions):
            raise ValueError("Catalog operation guards require effective permissions.")

    def require_allowed(self) -> None:
        """Reject known unavailable or unauthorized work before executor dispatch."""
        if self.profile is not None:
            decision = self.profile.guard(self.operation_id)
            if not decision.allowed:
                from datasluice.errors.catalog import UnsupportedCapabilityError

                raise UnsupportedCapabilityError(
                    "The deployment cannot perform this catalog operation.",
                    operation=str(self.operation_id),
                    platform=self.operation_id.platform,
                    capability_state=decision.state.value,
                    safe_action=decision.remedy or "Inspect the deployment capability profile before retrying.",
                )
        if self.permissions is not None:
            self.permissions.require(str(self.operation_id))


@runtime_checkable
class SyncCatalogStream[T](Protocol):
    """A synchronous stream with an explicit resource-release operation."""

    def __iter__(self) -> Iterator[T]:
        """Return this stream's iterator."""

    def __next__(self) -> T:
        """Return the next stream item."""

    def close(self) -> None:
        """Release resources owned by the stream."""


@runtime_checkable
class AsyncCatalogStream[T](Protocol):
    """An asynchronous stream with explicit cancellation-safe release."""

    def __aiter__(self) -> AsyncIterator[T]:
        """Return this stream's asynchronous iterator."""

    async def __anext__(self) -> T:
        """Return the next stream item."""

    async def aclose(self) -> None:
        """Release resources owned by the asynchronous stream."""


@runtime_checkable
class SyncCatalogOperationExecutor(Protocol):
    """Synchronous runtime dispatch sealed behind typed contract values."""

    def execute(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> ResultEnvelope[object]:
        """Dispatch one validated catalog operation."""

    def close(self) -> None:
        """Release resources owned by this executor."""


@runtime_checkable
class AsyncCatalogOperationExecutor(Protocol):
    """Asynchronous runtime dispatch sealed behind typed contract values."""

    async def execute(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> ResultEnvelope[object]:
        """Dispatch one validated catalog operation."""

    async def aclose(self) -> None:
        """Release resources owned by this executor."""


@dataclass(frozen=True, slots=True)
class CatalogConnectorContext:
    """Explicit dependencies supplied to a connector factory."""

    sync_executor: SyncCatalogOperationExecutor
    async_executor: AsyncCatalogOperationExecutor
    credentials: CatalogCredential | None = None
    manages_sync_executor: bool = True
    manages_async_executor: bool = True

    def __post_init__(self) -> None:
        if type(self.manages_sync_executor) is not bool or type(self.manages_async_executor) is not bool:
            raise ValueError("Catalog executor ownership flags must be booleans.")


@runtime_checkable
class SyncDatasetService(Protocol):
    """Portable synchronous dataset behavior."""

    def get(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> ResultEnvelope[DatasetRecord]:
        """Return one normalized dataset."""

    def list(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> ResultEnvelope[DatasetRecord]:
        """Return normalized datasets."""

    def create(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> tuple[ResultEnvelope[DatasetRecord], MutationReceipt]:
        """Create one normalized dataset."""

    def update(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> tuple[ResultEnvelope[DatasetRecord], MutationReceipt]:
        """Update one normalized dataset."""

    def delete(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> MutationReceipt:
        """Delete one normalized dataset."""


@runtime_checkable
class AsyncDatasetService(Protocol):
    """Portable asynchronous dataset behavior."""

    async def get(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[DatasetRecord]:
        """Return one normalized dataset."""

    async def list(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[DatasetRecord]:
        """Return normalized datasets."""

    async def create(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> tuple[ResultEnvelope[DatasetRecord], MutationReceipt]:
        """Create one normalized dataset."""

    async def update(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> tuple[ResultEnvelope[DatasetRecord], MutationReceipt]:
        """Update one normalized dataset."""

    async def delete(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> MutationReceipt:
        """Delete one normalized dataset."""


@runtime_checkable
class SyncResourceService(Protocol):
    """Portable synchronous resource behavior."""

    def get(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> ResultEnvelope[ResourceRecord]:
        """Return one normalized resource."""

    def list(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> ResultEnvelope[ResourceRecord]:
        """Return normalized resources."""


@runtime_checkable
class AsyncResourceService(Protocol):
    """Portable asynchronous resource behavior."""

    async def get(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[ResourceRecord]:
        """Return one normalized resource."""

    async def list(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[ResourceRecord]:
        """Return normalized resources."""


@runtime_checkable
class SyncOrganizationService(Protocol):
    """Portable synchronous organization behavior."""

    def get(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[OrganizationRecord]:
        """Return one normalized organization."""

    def list(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[OrganizationRecord]:
        """Return normalized organizations."""


@runtime_checkable
class AsyncOrganizationService(Protocol):
    """Portable asynchronous organization behavior."""

    async def get(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[OrganizationRecord]:
        """Return one normalized organization."""

    async def list(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[OrganizationRecord]:
        """Return normalized organizations."""


@runtime_checkable
class SyncCatalogClient(Protocol):
    """Synchronous normalized catalog client surface."""

    @property
    def datasets(self) -> SyncDatasetService:
        """Return normalized dataset operations."""

    @property
    def resources(self) -> SyncResourceService:
        """Return normalized resource operations."""

    @property
    def organizations(self) -> SyncOrganizationService:
        """Return normalized organization operations."""

    def capability(self, operation_id: str) -> CapabilityState:
        """Return the effective non-dispatching capability classification."""

    def platform_metadata(self) -> Mapping[str, object]:
        """Return safe platform metadata."""

    def close(self) -> None:
        """Release owned synchronous resources."""

    def __enter__(self) -> Self:
        """Enter a managed synchronous client context."""

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close resources on context exit."""


@runtime_checkable
class AsyncCatalogClient(Protocol):
    """Asynchronous normalized catalog client surface."""

    @property
    def datasets(self) -> AsyncDatasetService:
        """Return normalized dataset operations."""

    @property
    def resources(self) -> AsyncResourceService:
        """Return normalized resource operations."""

    @property
    def organizations(self) -> AsyncOrganizationService:
        """Return normalized organization operations."""

    def capability(self, operation_id: str) -> CapabilityState:
        """Return the effective non-dispatching capability classification."""

    def platform_metadata(self) -> Mapping[str, object]:
        """Return safe platform metadata."""

    async def aclose(self) -> None:
        """Release owned asynchronous resources."""

    async def __aenter__(self) -> Self:
        """Enter a managed asynchronous client context."""

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close resources on context exit."""
