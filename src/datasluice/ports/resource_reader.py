"""Resource reader port Protocol (defined-only, no implementation yet — D-16)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from datasluice.domain import Resource


@runtime_checkable
class ResourceReader(Protocol):
    """Boundary protocol for opening a resource for streaming reads.

    The return type is ``Any`` in Phase 2 because no ``BatchStream`` exists yet;
    Phase 4 narrows it to an Arrow ``RecordBatchReader``.
    """

    def open(self, resource: Resource) -> Any: ...
