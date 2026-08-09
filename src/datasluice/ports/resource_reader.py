"""Resource reader port Protocol."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from datasluice.data.batch_stream import BatchCursor, BatchStream
    from datasluice.domain import Resource


@runtime_checkable
class ResourceReader(Protocol):
    """Boundary protocol for opening a resource for streaming reads.

        The ``open`` method returns a :class:`BatchStream` — a context-managed
        Arrow ``RecordBatch`` stream. The
        ``batch_size`` keyword controls the row count per yielded batch
    .
    """

    def open(self, resource: Resource, *, batch_size: int = 65536) -> BatchStream: ...


@runtime_checkable
class CheckpointableResourceReader(Protocol):
    """Additive capability for opening a resource from a logical cursor."""

    def open_from_cursor(
        self,
        resource: Resource,
        cursor: BatchCursor,
        *,
        batch_size: int = 65536,
    ) -> BatchStream: ...


@runtime_checkable
class ResponseAwareReader(Protocol):
    """Additive capability for consuming an already-fetched response stream."""

    def open_response(
        self,
        resource: Resource,
        stream_cm: object,
        *,
        headers: object,
        batch_size: int | None = None,
    ) -> BatchStream: ...
