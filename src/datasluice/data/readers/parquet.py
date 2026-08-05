"""Streaming Parquet reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-10).

Migrated from ``datasluice.formats.parquet``. Streams row groups via
``pq.ParquetFile.iter_batches`` on seekable sources. The Parquet footer
lives at end-of-file and is read via ``seek()``, so a non-seekable source
(``HttpDownload`` over ``IterableBytesIO``) MUST be spooled to
``io.BytesIO`` before reading. This is the unavoidable compromise
documented in RESEARCH Pitfall 1: the spool is bounded by total file
size, not ``batch_size``. The proper fix (HTTP Range requests) is out of
Phase 4 scope.
"""

from __future__ import annotations

import io
from collections.abc import Iterator
from typing import Any

from datasluice.data.readers.base import BaseFormatReader
from datasluice.exceptions import FormatError


class ParquetReader(BaseFormatReader):
    """Stream a Parquet ``BinaryIO`` source into Arrow ``RecordBatch`` objects.

    On a seekable source (``LocalFile``, ``ObjectStorage``, ``BytesIO``)
    row groups are streamed via ``ParquetFile.iter_batches``. On a
    non-seekable source (``HttpDownload`` over ``IterableBytesIO``) the
    entire body is spooled into ``io.BytesIO`` first — this is required
    because Parquet footers live at end-of-file and ``ParquetFile``
    issues a ``seek()`` to read them. See RESEARCH Pitfall 1.
    """

    format_name = "PARQUET"

    def read_batches(self, source: Any, *, batch_size: int = 65536) -> Iterator[Any]:
        """Yield ``RecordBatch`` objects by streaming Parquet row groups.

        Args:
            source: A binary file-like Parquet source.
            batch_size: Target rows per yielded batch (re-chunked from
                row-group boundaries).

        Raises:
            FormatError: If ``pyarrow`` is missing or the Parquet is malformed.
        """
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise FormatError(
                "Streaming reads require 'pyarrow'. Install with: pip install datasluice[streaming]"
            ) from exc

        seekable = _safe_seekable(source)
        if not seekable:
            source = io.BytesIO(source.read())

        try:
            parquet_file = pq.ParquetFile(source)
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
            raise FormatError(f"Invalid Parquet: {exc}") from exc
        try:
            for batch in parquet_file.iter_batches(batch_size=batch_size):
                if batch.num_rows > 0:
                    yield batch
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
            raise FormatError(f"Invalid Parquet: {exc}") from exc

    def read_batches_from_row_group(self, source: Any, *, start_row_group_index: int) -> Iterator[tuple[int, Any]]:
        """Yield ``(row_group_index, batch)`` tuples for each non-empty row group.

        Each tuple carries the physical row-group index alongside its batch so
        the caller tracks the physical position even when empty groups are
        skipped (CR-06). Empty row groups advance the ``range`` cursor without
        yielding a tuple, keeping the physical index accurate for the next
        non-empty group.

        The caller owns *source* lifetime: this method does NOT close *source*
        so the caller can still read the Parquet footer (e.g. for an empty
        file's schema, CR-08) after iteration completes. Pass *source* to a
        ``BatchStream`` ``closeables`` tuple (or otherwise close it) to release
        the underlying file handle.

        Args:
            source: A seekable binary Parquet source.
            start_row_group_index: Physical row-group index to resume from.

        Raises:
            FormatError: If ``pyarrow`` is missing, the Parquet is malformed, or
                the source is non-seekable.
        """
        if type(start_row_group_index) is not int or start_row_group_index < 0:
            raise FormatError("Parquet start row-group index must be a non-negative integer")
        if not _safe_seekable(source):
            raise FormatError("Parquet continuation requires a seekable local-file or object-storage source")
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise FormatError("Streaming reads require 'pyarrow'. Install with: uv sync --all-extras") from exc
        try:
            parquet_file = pq.ParquetFile(source)
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
            raise FormatError(f"Invalid Parquet: {exc}") from exc
        if start_row_group_index > parquet_file.num_row_groups:
            raise FormatError(
                f"Parquet continuation row group {start_row_group_index} exceeds footer count "
                f"{parquet_file.num_row_groups}"
            )
        try:
            for row_group_index in range(start_row_group_index, parquet_file.num_row_groups):
                batch = self._read_row_group(parquet_file, row_group_index)
                if batch.num_rows > 0:
                    yield row_group_index, batch
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
            raise FormatError(f"Invalid Parquet row group: {exc}") from exc

    def _read_row_group(self, parquet_file: Any, row_group_index: int) -> Any:
        """Read one complete Parquet row group as one RecordBatch."""
        table = parquet_file.read_row_group(row_group_index).combine_chunks()
        batches = table.to_batches(max_chunksize=max(table.num_rows, 1))
        if batches:
            return batches[0]
        import pyarrow as pa

        return pa.RecordBatch.from_arrays(
            [pa.array([], type=field.type) for field in table.schema],
            schema=table.schema,
        )


def _safe_seekable(source: Any) -> bool:
    """Return ``source.seekable()`` if available; ``False`` on any error."""
    try:
        return bool(source.seekable())
    except Exception:
        return False
