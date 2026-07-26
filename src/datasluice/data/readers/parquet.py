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
        except pa.ArrowInvalid as exc:
            raise FormatError(f"Invalid Parquet: {exc}") from exc

        try:
            for batch in parquet_file.iter_batches(batch_size=batch_size):
                if batch.num_rows > 0:
                    yield batch
        except pa.ArrowInvalid as exc:
            raise FormatError(f"Invalid Parquet: {exc}") from exc


def _safe_seekable(source: Any) -> bool:
    """Return ``source.seekable()`` if available; ``False`` on any error."""
    try:
        return bool(source.seekable())
    except Exception:
        return False
