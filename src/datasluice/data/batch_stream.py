"""BatchStream — context-managed Arrow RecordBatch stream (DATA-01, DATA-02, D-P4-17).

A concrete wrapper around ``pa.RecordBatchReader`` or a bare
``Iterator[RecordBatch]``. Exposes ``.schema`` and ``.iter_batches()`` with
context-manager discipline (idempotent close, ``StreamClosedError`` on
use-after-close). Composition over inheritance: ``pa.RecordBatchReader`` is a
C++ extension type — we wrap it, never subclass it (RESEARCH Anti-Patterns).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from datasluice.exceptions import StreamClosedError

if TYPE_CHECKING:
    from collections.abc import Iterator


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

    def __init__(self, source: Any, schema: Any) -> None:
        self._source = source
        self._schema = schema
        self._closed = False

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

    def close(self) -> None:
        """Release the underlying reader; idempotent (safe to call multiple times)."""
        if self._closed:
            return
        self._closed = True
        if hasattr(self._source, "close"):
            self._source.close()

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
