"""Streaming JSON reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-10).

Migrated from ``datasluice.formats.json``. The v0.1.0 ``list[dict]`` contract
is replaced by the streaming RecordBatch contract.

pyarrow's ``pyarrow.json.read_json`` only handles newline-delimited JSONL
natively; JSON arrays of objects (``[{...}, {...}]``) trip a column-type
mismatch error. The migrated reader therefore peeks the first non-whitespace
byte and dispatches: ``{`` -> JSONL via ``pyarrow.json.read_json``; ``[`` ->
JSON array via ``json.loads`` plus ``pyarrow.Table.from_pylist``. Both paths
yield ``RecordBatch`` objects in roughly ``batch_size`` chunks.

pyarrow's JSON reader materialises a full ``Table`` internally (it is not a
true row-group streamer like the CSV or Parquet readers), so this reader
buffers the source into ``io.BytesIO`` before handing it to pyarrow. This
matches the underlying library's actual behaviour and keeps the public
contract uniform with the other readers.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from datasluice.data.readers.base import BaseFormatReader
from datasluice.exceptions import FormatError


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
                "Streaming reads require 'pyarrow'. Install with: pip install datasluice[streaming]"
            ) from exc

        data = source.read()
        first = _first_non_whitespace_byte(data)
        if first is None:
            return

        if first == 91:  # ord('[') == 91
            table = self._read_array(data, pa)
        elif first == 123:  # ord('{') == 123
            table = self._read_jsonl(data, pa)
        else:
            raise FormatError(f"Unexpected JSON leading byte: {bytes([first])!r}")

        if table.num_rows == 0:
            return
        for batch in table.to_batches(max_chunksize=batch_size):
            if batch.num_rows > 0:
                yield batch

    @staticmethod
    def _read_jsonl(data: bytes, pa: Any) -> Any:
        import io

        import pyarrow.json as paj

        read_options = paj.ReadOptions(block_size=1 << 20)
        try:
            return paj.read_json(io.BytesIO(data), read_options=read_options)
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
        records = [r for r in parsed if isinstance(r, dict)]
        if not records:
            return pa.table({})
        try:
            return pa.Table.from_pylist(records)
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
            raise FormatError(f"Could not coerce JSON array to Arrow: {exc}") from exc


def _first_non_whitespace_byte(data: bytes) -> int | None:
    for byte in data:
        if byte not in b" \t\r\n":
            return byte
    return None
