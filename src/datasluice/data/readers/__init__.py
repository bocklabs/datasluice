"""Streaming format readers yielding Arrow ``RecordBatch`` (DATA-03, D-P4-10).

Registry and ``get_reader`` factory migrated verbatim from
``datasluice.formats`` (v0.1.0); only the reader classes change (from
``list[dict]``-returning to ``Iterator[RecordBatch]``-returning).

Each reader lazy-imports its heavy optional dependency (``pyarrow`` for CSV,
JSON, Parquet, GeoJSON; ``openpyxl`` for XLSX) inside ``read_batches`` so
that importing this package does not require the streaming extra.
"""

from __future__ import annotations

from datasluice.data.readers.base import BaseFormatReader
from datasluice.data.readers.csv import CSVReader
from datasluice.data.readers.json import JSONReader

READERS: dict[str, type[BaseFormatReader]] = {
    "CSV": CSVReader,
    "JSON": JSONReader,
    "JSONL": JSONReader,
    "NDJSON": JSONReader,
}


def get_reader(format_name: str) -> BaseFormatReader:
    """Return a format reader instance for *format_name*.

    Args:
        format_name: Format key (case-insensitive). Known keys: CSV, JSON,
            JSONL, NDJSON, XLSX, XLS, PARQUET, GEOJSON.

    Returns:
        A :class:`BaseFormatReader` instance configured with defaults.

    Raises:
        KeyError: If the format is not supported.
    """
    cls = READERS.get(format_name.upper())
    if cls is None:
        raise KeyError(f"Unsupported format: {format_name!r}. Known: {', '.join(sorted(READERS))}")
    return cls()


__all__ = [
    "BaseFormatReader",
    "CSVReader",
    "JSONReader",
    "READERS",
    "get_reader",
]
