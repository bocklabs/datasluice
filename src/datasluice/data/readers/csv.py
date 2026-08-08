"""Streaming CSV reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-10).

Migrated from ``datasluice.formats.csv`` (v0.1.0 ``list[dict]`` contract) to
the streaming RecordBatch contract. Delegates decoding to
``pyarrow.csv.open_csv`` which returns a ``RecordBatchReader`` that streams
batches without buffering the whole file. The reader honours the
``batch_size`` row-count hint by re-chunking the upstream batches (pyarrow's
CSV reader only exposes a byte-level ``block_size`` knob). Verified against
pyarrow 24.0.0 on both seekable (``io.BytesIO``) and non-seekable
(``IterableBytesIO``) sources (RESEARCH Pattern 1).
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from datasluice.data.readers.base import BaseFormatReader
from datasluice.exceptions import FormatError


class CSVReader(BaseFormatReader):
    """Stream a CSV ``BinaryIO`` source into Arrow ``RecordBatch`` objects.

    Args:
        encoding: Text encoding (default ``"utf-8"``).
        delimiter: Column delimiter (default ``","``).
    """

    format_name = "CSV"

    def __init__(self, *, encoding: str = "utf-8", delimiter: str = ",") -> None:
        self.encoding = encoding
        self.delimiter = delimiter

    def read_batches(self, source: Any, *, batch_size: int = 65536) -> Iterator[Any]:
        """Yield ``RecordBatch`` objects by delegating to ``pyarrow.csv.open_csv``.

        Args:
            source: A binary file-like CSV source.
            batch_size: Target rows per yielded batch.

        Raises:
            FormatError: If ``pyarrow`` is missing or the CSV is malformed.
        """
        try:
            import pyarrow as pa
            import pyarrow.csv as pacsv
        except ImportError as exc:
            raise FormatError(
                "Streaming reads require 'pyarrow'. Install with: pip install datasluice[streaming]"
            ) from exc

        read_options = pacsv.ReadOptions(block_size=1 << 20, encoding=self.encoding)
        parse_options = pacsv.ParseOptions(delimiter=self.delimiter)
        try:
            reader = pacsv.open_csv(source, read_options=read_options, parse_options=parse_options)
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
            raise FormatError(f"Invalid CSV: {exc}") from exc

        try:
            yield from _rechunk_reader(reader, batch_size, pa)
        finally:
            reader.close()


def _rechunk_reader(reader: Any, batch_size: int, pa: Any) -> Iterator[Any]:
    """Drain a ``pa.RecordBatchReader`` and re-chunk into ``batch_size``-row batches."""
    if type(batch_size) is not int or batch_size <= 0:
        raise FormatError("CSV batch_size must be a positive integer")
    pending: Any = None
    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            break
        if batch is None:
            break
        if batch.num_rows == 0:
            continue
        current = pa.Table.from_batches([batch])
        if pending is not None:
            current = pa.concat_tables([pending, current])
            pending = None
        while current.num_rows >= batch_size:
            head = current.slice(0, batch_size)
            yield head.to_batches()[0]
            current = current.slice(batch_size)
        if current.num_rows > 0:
            pending = current
    if pending is not None and pending.num_rows > 0:
        yield from pending.to_batches()
