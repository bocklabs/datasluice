"""JSON format reader.

Supports both line-delimited JSON (JSONL/NDJSON) and JSON arrays.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from datasluice.exceptions import FormatError
from datasluice.formats.base import BaseFormatReader


class JSONReader(BaseFormatReader):
    """Read JSON or JSONL files into a list of dictionaries."""

    format_name = "JSON"

    def _to_text(self, source: str | Path | bytes) -> str:
        if isinstance(source, bytes):
            return source.decode("utf-8")
        return Path(source).read_text(encoding="utf-8")

    def read(self, source: str | Path | bytes) -> list[dict[str, Any]]:
        text = self._to_text(source).strip()
        if not text:
            return []

        # Try JSONL first (one object per line).
        if "\n" in text and not text.lstrip().startswith("["):
            records: list[dict[str, Any]] = []
            for line in text.splitlines():
                line = line.strip()
                if line:
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(obj, dict):
                        records.append(obj)
            if records:
                return records

        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise FormatError(f"Invalid JSON: {exc}") from exc

        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]
        if isinstance(data, dict):
            return [data]
        raise FormatError(f"Unexpected JSON structure: {type(data).__name__}")
