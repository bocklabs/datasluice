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
    """Closed continuation cursor for the next unread batch.

    ``next_batch_index`` tracks the shard count (for shard naming) and
    ``position.row_group_index`` tracks the physical Parquet row-group
    position. These MAY diverge when empty row groups exist between non-empty
    ones (CR-06): the physical index advances past empty groups while the
    shard count only increments for yielded batches.
    """

    next_batch_index: int
    position: ParquetRowGroupPosition

    def __post_init__(self) -> None:
        if type(self.next_batch_index) is not int or self.next_batch_index < 0:
            raise DataSluiceError("Next batch index must be a non-negative integer")


class BatchStream:
    """Context-managed Arrow RecordBatch stream.

    Wraps a ``pa.RecordBatchReader`` (CSV/Parquet path) or a bare
    ``Iterator[RecordBatch]`` (XLSX/GeoJSON path) and exposes a uniform
    ``.schema`` property + ``.iter_batches()`` generator with
    context-manager cleanup. Callers transfer ownership of acquired byte
    sources through ``closeables`` so :meth:`close` releases them with the
    wrapped reader.

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
        start_row_group_index: int | None = None,
        closeables: tuple[Any, ...] = (),
        indexed: bool = False,
    ) -> None:
        if type(start_batch_index) is not int or start_batch_index < 0:
            raise DataSluiceError("Batch stream start index must be a non-negative integer")
        if start_row_group_index is not None and (type(start_row_group_index) is not int or start_row_group_index < 0):
            raise DataSluiceError("Batch stream row-group start index must be a non-negative integer")
        self._source = source
        self._schema = schema
        self._closed = False
        self._start_batch_index = start_batch_index
        self._start_row_group_index = start_batch_index if start_row_group_index is None else start_row_group_index
        self._closeables = closeables
        self._indexed = indexed

    @property
    def schema(self) -> Any:
        """The pa.Schema for batches yielded by this stream."""
        return self._schema

    def iter_batches(self) -> Iterator[Any]:
        """Yield Arrow ``RecordBatch`` objects from the wrapped source.

        When ``indexed`` mode is active (Parquet row-group path), the source
        yields ``(physical_index, batch)`` tuples and this method unwraps to
        just the batch.

        Raises:
            StreamClosedError: If called after :meth:`close` or ``__exit__``.
        """
        if self._closed:
            raise StreamClosedError("BatchStream is closed; cannot iterate batches")
        if self._indexed:
            for _index, batch in self._source:
                yield batch
        elif hasattr(self._source, "read_next_batch"):
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
        """Yield batches with the closed cursor for the next unread row group.

        In ``indexed`` mode (Parquet row-group path), the physical row-group
        index comes from the source tuples, not from ``enumerate`` — so empty
        row groups that are skipped internally do not corrupt the cursor
        position (CR-06). ``next_batch_index`` (shard count) and
        ``position.row_group_index`` (physical position) may diverge.

        Raises:
            StreamClosedError: If called after :meth:`close` or ``__exit__``
                (WR-01: previously the indexed path bypassed the closed-stream
                guard that :meth:`iter_batches` enforces).
        """
        if self._closed:
            raise StreamClosedError("BatchStream is closed; cannot iterate batches")
        next_batch_index = self._start_batch_index
        if self._indexed:
            last_physical = self._start_row_group_index - 1
            for physical_index, batch in self._source:
                if physical_index <= last_physical:
                    raise DataSluiceError("Physical row-group indexes must increase monotonically")
                cursor = BatchCursor(
                    next_batch_index + 1,
                    ParquetRowGroupPosition(physical_index + 1),
                )
                yield batch, cursor
                next_batch_index += 1
                last_physical = physical_index
        else:
            previous = self._start_batch_index
            for batch_index, batch in enumerate(self.iter_batches(), start=self._start_batch_index):
                next_batch = batch_index + 1
                if next_batch <= previous:
                    raise DataSluiceError("Batch cursor indexes must increase monotonically")
                cursor = BatchCursor(next_batch, ParquetRowGroupPosition(next_batch))
                yield batch, cursor
                previous = next_batch

    def close(self) -> None:
        """Release the underlying reader and any owned closeables; idempotent (WR-02).

        Every owned resource is attempted even when an earlier close raises,
        so one failing closeable cannot prevent later ones from releasing
        (WR-02). The first close exception is re-raised after all closeables
        have been attempted so the caller still sees the original failure.
        """
        if self._closed:
            return
        self._closed = True
        first_exc: BaseException | None = None
        if hasattr(self._source, "close"):
            try:
                self._source.close()
            except BaseException as exc:
                first_exc = exc
        for closeable in self._closeables:
            if not hasattr(closeable, "close"):
                continue
            try:
                closeable.close()
            except BaseException as exc:
                if first_exc is None:
                    first_exc = exc
        if first_exc is not None:
            raise first_exc

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
