"""Resource reader port Protocol (defined-only, no implementation yet — D-16)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from datasluice.data.batch_stream import BatchStream
    from datasluice.domain import Resource


@runtime_checkable
class ResourceReader(Protocol):
    """Boundary protocol for opening a resource for streaming reads.

    The ``open`` method returns a :class:`BatchStream` — a context-managed
    Arrow ``RecordBatch`` stream (DATA-01, DATA-02, D-P4-17). The
    ``batch_size`` keyword controls the row count per yielded batch
    (D-P4-14, default 65,536 — pyarrow-native).
    """

    def open(self, resource: Resource, *, batch_size: int = 65536) -> BatchStream: ...
