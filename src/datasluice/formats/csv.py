"""CSV format reader."""

from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import Any

from datasluice.formats.base import BaseFormatReader


class CSVReader(BaseFormatReader):
    """Read CSV files into a list of dictionaries.

    Args:
        encoding: File encoding (default ``"utf-8"``).
        delimiter: Column delimiter (default ``","``).
    """

    format_name = "CSV"

    def __init__(self, *, encoding: str = "utf-8", delimiter: str = ",") -> None:
        self.encoding = encoding
        self.delimiter = delimiter

    def _to_text(self, source: str | Path | bytes) -> str:
        if isinstance(source, bytes):
            return source.decode(self.encoding)
        return Path(source).read_text(encoding=self.encoding)

    def read(self, source: str | Path | bytes) -> list[dict[str, Any]]:
        text = self._to_text(source)
        reader = csv.DictReader(io.StringIO(text), delimiter=self.delimiter)
        return list(reader)
