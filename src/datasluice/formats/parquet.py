"""Parquet format reader (requires the ``pyarrow`` optional dependency)."""

from __future__ import annotations

import io
from pathlib import Path
from typing import Any

from datasluice.exceptions import FormatError
from datasluice.formats.base import BaseFormatReader


class ParquetReader(BaseFormatReader):
    """Read Parquet files into a list of dictionaries.

    Requires ``pyarrow``: install with ``pip install datasluice[parquet]``.
    """

    format_name = "PARQUET"

    def read(self, source: str | Path | bytes) -> list[dict[str, Any]]:
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise FormatError(
                "Parquet support requires 'pyarrow'. Install with: pip install datasluice[parquet]"
            ) from exc

        if isinstance(source, bytes):
            table = pq.read_table(io.BytesIO(source))
        else:
            table = pq.read_table(source)

        return table.to_pylist()
