"""BatchStream — context-managed Arrow RecordBatch stream (DATA-01, DATA-02, D-P4-17).

A concrete wrapper around ``pa.RecordBatchReader`` or a bare
``Iterator[RecordBatch]``. Exposes ``.schema`` and ``.iter_batches()`` with
context-manager discipline (idempotent close, ``StreamClosedError`` on
use-after-close). Composition over inheritance: ``pa.RecordBatchReader`` is a
C++ extension type — we wrap it, never subclass it (RESEARCH Anti-Patterns).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from datasluice.exceptions import DataSluiceError, StreamClosedError

if TYPE_CHECKING:
    from collections.abc import Iterator


@dataclass(frozen=True)
class ParquetRowGroupPosition:
    """Logical position at the next unread Parquet row group."""

    row_group_index: int

    def __post_init__(self) -> None:
        if type(self.row_group_index) is not int or self.row_group_index < 0:
            raise DataSluiceError("Parquet row-group index must be a non-negative integer")


@dataclass(frozen=True)
class BatchCursor:
    """Closed continuation cursor for the next unread batch."""

    next_batch_index: int
    position: ParquetRowGroupPosition

    def __post_init__(self) -> None:
        if type(self.next_batch_index) is not int or self.next_batch_index < 0:
            raise DataSluiceError("Next batch index must be a non-negative integer")
        if self.next_batch_index != self.position.row_group_index:
            raise DataSluiceError("Batch and Parquet row-group indexes must match")


class BatchStream:
    """Context-managed Arrow RecordBatch stream.

    Wraps a ``pa.RecordBatchReader`` (CSV/Parquet path) or a bare
    ``Iterator[RecordBatch]`` (XLSX/GeoJSON path) and exposes a uniform
    ``.schema`` property + ``.iter_batches()`` generator with
    context-manager cleanup.

    Attributes:
        _source: The wrapped reader or iterator.
        _schema: The ``pa.Schema`` for batches yielded by this stream.
        _closed: Whether :meth:`close` has been called.
    """

    def __init__(
        self,
        source: Any,
        schema: Any,
        *,
        start_batch_index: int = 0,
        closeables: tuple[Any, ...] = (),
    ) -> None:
        if type(start_batch_index) is not int or start_batch_index < 0:
            raise DataSluiceError("Batch stream start index must be a non-negative integer")
        self._source = source
        self._schema = schema
        self._closed = False
        self._start_batch_index = start_batch_index
        self._closeables = closeables

    @property
    def schema(self) -> Any:
        """The pa.Schema for batches yielded by this stream."""
        return self._schema

    def iter_batches(self) -> Iterator[Any]:
        """Yield Arrow ``RecordBatch`` objects from the wrapped source.

        Raises:
            StreamClosedError: If called after :meth:`close` or ``__exit__``.
        """
        if self._closed:
            raise StreamClosedError("BatchStream is closed; cannot iterate batches")
        if hasattr(self._source, "read_next_batch"):
            while True:
                try:
                    batch = self._source.read_next_batch()
                except StopIteration:
                    return
                if batch is None:
                    return
                yield batch
        else:
            yield from self._source

    def iter_batches_with_cursors(self) -> Iterator[tuple[Any, BatchCursor]]:
        """Yield batches with the closed cursor for the next unread row group."""
        previous = self._start_batch_index
        for batch_index, batch in enumerate(self.iter_batches(), start=self._start_batch_index):
            next_batch_index = batch_index + 1
            if next_batch_index <= previous:
                raise DataSluiceError("Batch cursor indexes must increase monotonically")
            cursor = BatchCursor(next_batch_index, ParquetRowGroupPosition(next_batch_index))
            yield batch, cursor
            previous = next_batch_index

    def close(self) -> None:
        """Release the underlying reader; idempotent (safe to call multiple times)."""
        if self._closed:
            return
        self._closed = True
        if hasattr(self._source, "close"):
            self._source.close()
        for closeable in self._closeables:
            if hasattr(closeable, "close"):
                closeable.close()

    def __enter__(self) -> BatchStream:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        """Return a PyCapsule for zero-copy Arrow interop (Phase 6 terminals).

        Delegates to the wrapped reader's ``__arrow_c_stream__`` when present
        (``pa.RecordBatchReader`` implements it). For bare iterators,
        materializes batches via ``pa.RecordBatchReader.from_batches``.
        """
        import pyarrow as pa

        if hasattr(self._source, "__arrow_c_stream__"):
            return self._source.__arrow_c_stream__(requested_schema)
        return pa.RecordBatchReader.from_batches(self._schema, list(self.iter_batches())).__arrow_c_stream__(
            requested_schema
        )
