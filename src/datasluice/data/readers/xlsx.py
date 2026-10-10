from __future__ import annotations

from typing import TYPE_CHECKING, Any

from datasluice.data.readers.base import BaseFormatReader
from datasluice.exceptions import FormatError

if TYPE_CHECKING:
    from collections.abc import Iterator


def _xlsx_dependencies() -> tuple[Any, Any]:
    """Return the openpyxl workbook loader and the pyarrow module, or raise a FormatError naming the extra."""
    try:
        from openpyxl import load_workbook
    except ImportError as exc:
        raise FormatError("XLSX reads require 'openpyxl'. Install with: pip install datasluice[xlsx]") from exc
    try:
        import pyarrow as pa
    except ImportError as exc:
        raise FormatError("Streaming reads require 'pyarrow'. Install with: pip install datasluice[parquet]") from exc
    return load_workbook, pa


def _deduplicated_headers(header_row: tuple[Any, ...]) -> list[str]:
    """Return the column names of one XLSX row with duplicates and blanks suffixed by occurrence.

    openpyxl can emit duplicate column headers in messy real-world workbooks, so
    later cells would otherwise overwrite earlier ones inside the zipped row
    dicts.
    """
    seen: dict[str, int] = {}
    headers: list[str] = []
    for header in (str(value) if value is not None else "" for value in header_row):
        count = seen.get(header, 0)
        seen[header] = count + 1
        headers.append(header if count == 0 else f"{header}_{count + 1}")
    return headers


def _row_chunks(rows: Iterator[Any], headers: list[str], batch_size: int) -> Iterator[list[dict[str, Any]]]:
    """Yield the rows of one worksheet as chunks of at most *batch_size* dicts keyed by *headers*."""
    buffer: list[dict[str, Any]] = []
    for row in rows:
        buffer.append(dict(zip(headers, row, strict=False)))
        if len(buffer) >= batch_size:
            yield buffer
            buffer = []
    if buffer:
        yield buffer


class XLSXReader(BaseFormatReader):
    """Stream an XLSX ``BinaryIO`` source into Arrow ``RecordBatch`` objects."""

    format_name = "XLSX"

    def read_batches(self, source: Any, *, batch_size: int = 65536) -> Iterator[Any]:
        """Yield ``RecordBatch`` objects by chunking openpyxl ``iter_rows``.

        Args:
            source: A binary file-like XLSX source.
            batch_size: Target rows per yielded batch.

        Raises:
            FormatError: If ``openpyxl`` / ``pyarrow`` is missing or the
                workbook is corrupt.
        """
        load_workbook, pa = _xlsx_dependencies()

        try:
            wb = load_workbook(source, read_only=True, data_only=True)
        except Exception as exc:
            raise FormatError(f"Invalid XLSX: {exc}") from exc

        try:
            ws = wb.active
            if ws is None:
                raise FormatError("Invalid XLSX: workbook has no active worksheet")
            rows = ws.iter_rows(values_only=True)
            try:
                header_row = next(rows)
            except StopIteration:
                return
            headers = _deduplicated_headers(header_row)
            for chunk in _row_chunks(rows, headers, batch_size):
                yield _batch_from_rows(chunk, pa)
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
            raise FormatError(f"Could not coerce XLSX rows to Arrow: {exc}") from exc
        finally:
            wb.close()


def _batch_from_rows(rows: list[dict[str, Any]], pa: Any) -> Any:
    """Build a single ``RecordBatch`` from a chunk of row dicts."""
    table = pa.Table.from_pylist(rows)
    batches = table.to_batches()
    if not batches:
        return pa.RecordBatch.from_pylist(rows)
    return batches[0]
