"""Deterministic reference clients for the catalog contract tracer."""

from __future__ import annotations

from collections.abc import Mapping
from types import TracebackType

from datasluice.contracts.catalog.protocols import CapabilityState
from datasluice.domain import Dataset

_FIXTURE_DATASETS = {"fixture-dataset": {"id": "fixture-dataset", "title": "Fixture dataset"}}


class SyncReferenceConnector:
    """Independent synchronous fixture-backed catalog reference client."""

    def __init__(self, *, capability: CapabilityState = "available") -> None:
        self._capability = capability
        self._datasets = {dataset_id: dict(value) for dataset_id, value in _FIXTURE_DATASETS.items()}
        self.dispatches: list[str] = []
        self.closed = False

    @property
    def datasets(self) -> SyncReferenceConnector:
        """Return the synchronous dataset service."""
        return self

    def capability(self, operation_id: str) -> CapabilityState:
        """Return the deterministic capability classification."""
        if operation_id != "datasets.get":
            return "unavailable"
        return self._capability

    def platform_metadata(self) -> Mapping[str, object]:
        """Return metadata that proves report sanitization."""
        return {
            "platform": "reference",
            "fixture": "dataset-v1",
            "environment": "deterministic",
            "access_token": "never-report-this",
            "raw_body": "never-report-this",
        }

    def get(self, dataset_id: str) -> Dataset:
        """Return one deterministic fixture dataset."""
        self.dispatches.append("datasets.get")
        payload = self._datasets[dataset_id]
        return Dataset(id=payload["id"], title=payload["title"])

    def close(self) -> None:
        """Close the synchronous reference client exactly once."""
        self.closed = True

    def __enter__(self) -> SyncReferenceConnector:
        """Enter the synchronous reference client context."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the synchronous reference client context."""
        self.close()


class AsyncReferenceConnector:
    """Independent asynchronous fixture-backed catalog reference client."""

    def __init__(self, *, capability: CapabilityState = "available") -> None:
        self._capability = capability
        self._datasets = {dataset_id: dict(value) for dataset_id, value in _FIXTURE_DATASETS.items()}
        self.dispatches: list[str] = []
        self.closed = False

    @property
    def datasets(self) -> AsyncReferenceConnector:
        """Return the asynchronous dataset service."""
        return self

    def capability(self, operation_id: str) -> CapabilityState:
        """Return the deterministic capability classification."""
        if operation_id != "datasets.get":
            return "unavailable"
        return self._capability

    def platform_metadata(self) -> Mapping[str, object]:
        """Return metadata that proves report sanitization."""
        return {
            "platform": "reference",
            "fixture": "dataset-v1",
            "environment": "deterministic",
            "access_token": "never-report-this",
            "raw_body": "never-report-this",
        }

    async def get(self, dataset_id: str) -> Dataset:
        """Return one deterministic fixture dataset without sync delegation."""
        self.dispatches.append("datasets.get")
        payload = self._datasets[dataset_id]
        return Dataset(id=payload["id"], title=payload["title"])

    async def aclose(self) -> None:
        """Close the asynchronous reference client exactly once."""
        self.closed = True

    async def __aenter__(self) -> AsyncReferenceConnector:
        """Enter the asynchronous reference client context."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the asynchronous reference client context."""
        await self.aclose()
