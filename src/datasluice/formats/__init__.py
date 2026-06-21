"""Format readers for normalising tabular and geospatial data."""

from datasluice.formats.base import BaseFormatReader
from datasluice.formats.csv import CSVReader
from datasluice.formats.geojson import GeoJSONReader
from datasluice.formats.json import JSONReader
from datasluice.formats.parquet import ParquetReader
from datasluice.formats.xlsx import XLSXReader

READERS: dict[str, type[BaseFormatReader]] = {
    "CSV": CSVReader,
    "JSON": JSONReader,
    "JSONL": JSONReader,
    "NDJSON": JSONReader,
    "XLSX": XLSXReader,
    "XLS": XLSXReader,
    "PARQUET": ParquetReader,
    "GEOJSON": GeoJSONReader,
}


def get_reader(format_name: str) -> BaseFormatReader:
    """Return a format reader instance for *format_name*.

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
    "XLSXReader",
    "ParquetReader",
    "GeoJSONReader",
    "READERS",
    "get_reader",
]
