"""Incremental JSONL reads and size-limited JSON arrays of objects."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from datasluice.data.compression import PeekableReader
from datasluice.data.readers.base import BaseFormatReader, read_document
from datasluice.exceptions import FormatError

if TYPE_CHECKING:
    from collections.abc import Iterator


class JSONReader(BaseFormatReader):
    """Stream a JSON / JSONL ``BinaryIO`` source into Arrow ``RecordBatch`` objects."""

    format_name = "JSON"

    def read_batches(self, source: Any, *, batch_size: int = 65536) -> Iterator[Any]:
        """Yield ``RecordBatch`` objects from a JSON array or JSONL source.

        Args:
            source: A binary file-like JSON or JSONL source.
            batch_size: Target rows per yielded batch.

        Raises:
            FormatError: If ``pyarrow`` is missing or the JSON is malformed.
        """
        try:
            import pyarrow as pa
        except ImportError as exc:
            raise FormatError(
                "Streaming reads require 'pyarrow'. Install with: pip install datasluice[parquet]"
            ) from exc

        source = PeekableReader(source)
        if source.peek(3) == b"\xef\xbb\xbf":
            source.read(3)
        while source.peek(1) and source.peek(1) in b" \t\r\n":
            source.read(1)
        first = source.peek(1)
        if not first:
            return
        if first == b"[":
            table = self._read_array(read_document(source), pa)
            yield from table.to_batches(max_chunksize=batch_size)
        elif first == b"{":
            yield from self._read_jsonl(source, pa, batch_size)
        else:
            raise FormatError(f"Unexpected JSON leading byte: {first!r}")

    @staticmethod
    def _read_jsonl(source: Any, pa: Any, batch_size: int) -> Iterator[Any]:
        import pyarrow.json as paj

        read_options = paj.ReadOptions(block_size=65536, use_threads=False)
        try:
            with paj.open_json(source, read_options=read_options) as reader:
                for batch in reader:
                    for offset in range(0, batch.num_rows, batch_size):
                        yield batch.slice(offset, batch_size)
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
            raise FormatError(f"Invalid JSONL: {exc}") from exc

    @staticmethod
    def _read_array(data: bytes, pa: Any) -> Any:
        import json

        try:
            parsed = json.loads(data)
        except json.JSONDecodeError as exc:
            raise FormatError(f"Invalid JSON array: {exc}") from exc
        if not isinstance(parsed, list):
            raise FormatError(f"Expected JSON array, got {type(parsed).__name__}")
        if not all(isinstance(record, dict) for record in parsed):
            raise FormatError("JSON array elements must be objects")
        if not parsed:
            return pa.table({})
        try:
            return pa.Table.from_pylist(parsed)
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
            raise FormatError(f"Could not coerce JSON array to Arrow: {exc}") from exc
