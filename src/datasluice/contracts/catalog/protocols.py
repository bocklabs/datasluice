"""Narrow normalized catalog Protocols used by the contract tracer."""

from __future__ import annotations

from collections.abc import Mapping
from types import TracebackType
from typing import Literal, Protocol, Self, runtime_checkable

from datasluice.domain import Dataset

CapabilityState = Literal["available", "unavailable"]


@runtime_checkable
class SyncDatasetService(Protocol):
    """Synchronous normalized dataset reads."""

    def get(self, dataset_id: str) -> Dataset:
        """Return one normalized dataset by identifier."""


@runtime_checkable
class AsyncDatasetService(Protocol):
    """Asynchronous normalized dataset reads."""

    async def get(self, dataset_id: str) -> Dataset:
        """Return one normalized dataset by identifier."""


@runtime_checkable
class SyncCatalogClient(Protocol):
    """Synchronous catalog client surface exercised by the tracer."""

    @property
    def datasets(self) -> SyncDatasetService:
        """Return the normalized dataset service."""

    def capability(self, operation_id: str) -> CapabilityState:
        """Return the effective capability state for an operation."""

    def platform_metadata(self) -> Mapping[str, object]:
        """Return report-safe candidate platform metadata."""

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
    """Asynchronous catalog client surface exercised by the tracer."""

    @property
    def datasets(self) -> AsyncDatasetService:
        """Return the normalized dataset service."""

    def capability(self, operation_id: str) -> CapabilityState:
        """Return the effective capability state for an operation."""

    def platform_metadata(self) -> Mapping[str, object]:
        """Return report-safe candidate platform metadata."""

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
