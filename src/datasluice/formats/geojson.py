"""GeoJSON format reader."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from datasluice.exceptions import FormatError
from datasluice.formats.base import BaseFormatReader


class GeoJSONReader(BaseFormatReader):
    """Read GeoJSON FeatureCollections into a list of feature dictionaries."""

    format_name = "GEOJSON"

    def _to_text(self, source: str | Path | bytes) -> str:
        if isinstance(source, bytes):
            return source.decode("utf-8")
        return Path(source).read_text(encoding="utf-8")

    def read(self, source: str | Path | bytes) -> list[dict[str, Any]]:
        text = self._to_text(source).strip()
        if not text:
            return []
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise FormatError(f"Invalid GeoJSON: {exc}") from exc

        if data.get("type") == "FeatureCollection":
            return list(data.get("features", []))
        if data.get("type") == "Feature":
            return [data]
        raise FormatError(f"Unexpected GeoJSON root type: {data.get('type')!r}")
