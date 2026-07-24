"""State store port Protocol for incremental sync state (SYNC-01)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from datasluice.domain import SyncState


@runtime_checkable
class StateStore(Protocol):
    """Boundary protocol for persisting incremental sync state."""

    def get(self, key: str) -> SyncState | None: ...

    def put(self, key: str, state: SyncState) -> None: ...

    def delete(self, key: str) -> None: ...
