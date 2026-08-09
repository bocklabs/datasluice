"""Streaming GeoJSON reader yielding Arrow ``RecordBatch``.

Migrated from ``datasluice.formats.geojson``. GeoJSON is nested (not
tabular), so it cannot use ``pyarrow.json`` directly. Each Feature is
flattened to ONE row: ``properties`` becomes typed columns and
``geometry`` becomes a single ``geometry_wkt`` string column.

WKT encoding is handled by a minimal hand-rolled encoder for
``Point``, ``LineString`` and ``Polygon`` geometries. Complex geometries
(``MultiPoint``, ``MultiLineString``, ``MultiPolygon``,
``GeometryCollection``) are stored as the raw JSON string of the
geometry object in the ``geometry_wkt`` column with a documented caveat
(RESEARCH A3): open-data geometries are overwhelmingly simple, and
shapely is not installed to avoid bloating the dep tree. + can
swap in shapely if richer WKT coverage is needed.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

from datasluice.data.readers.base import BaseFormatReader
from datasluice.exceptions import FormatError


class GeoJSONReader(BaseFormatReader):
    """Stream a GeoJSON ``BinaryIO`` source into Arrow ``RecordBatch`` objects.

    Each Feature is flattened to a row whose columns are the Feature's
    ``properties`` plus a ``geometry_wkt`` string column carrying the
    WKT-encoded geometry (or raw JSON for complex geometry types).
    """

    format_name = "GEOJSON"

    def read_batches(self, source: Any, *, batch_size: int = 65536) -> Iterator[Any]:
        """Yield ``RecordBatch`` objects by flattening GeoJSON Features.

        Args:
            source: A binary file-like GeoJSON source.
            batch_size: Target rows per yielded batch.

        Raises:
            FormatError: If ``pyarrow`` is missing, the JSON is malformed,
                or the root object is not a Feature / FeatureCollection.
        """
        try:
            import pyarrow as pa
        except ImportError as exc:
            raise FormatError(
                "Streaming reads require 'pyarrow'. Install with: pip install datasluice[streaming]"
            ) from exc

        data = source.read()
        if not data.strip():
            return
        try:
            parsed = json.loads(data)
        except json.JSONDecodeError as exc:
            raise FormatError(f"Invalid GeoJSON: {exc}") from exc

        if not isinstance(parsed, dict):
            raise FormatError(f"Unexpected GeoJSON root type: {type(parsed).__name__}")

        root_type = parsed.get("type")
        if root_type == "FeatureCollection":
            features = parsed.get("features", [])
        elif root_type == "Feature":
            features = [parsed]
        else:
            raise FormatError(f"Unexpected GeoJSON root type: {root_type!r}")

        buffer: list[dict[str, Any]] = []
        for feature in features:
            row = dict(feature.get("properties") or {})
            row["geometry_wkt"] = _to_wkt(feature.get("geometry"))
            buffer.append(row)
            if len(buffer) >= batch_size:
                yield _batch_from_rows(buffer, pa)
                buffer = []
        if buffer:
            yield _batch_from_rows(buffer, pa)


def _batch_from_rows(rows: list[dict[str, Any]], pa: Any) -> Any:
    """Build a single ``RecordBatch`` from a chunk of feature rows."""
    table = pa.Table.from_pylist(rows)
    batches = table.to_batches()
    if not batches:
        return pa.RecordBatch.from_pylist(rows)
    return batches[0]


def _to_wkt(geometry: Any) -> str:
    """Encode a GeoJSON ``geometry`` object as WKT (or fall back to raw JSON).

    Handles ``Point``, ``LineString`` and ``Polygon``. Complex geometries
    (``MultiPoint``, ``MultiLineString``, ``MultiPolygon``,
    ``GeometryCollection``) and unknown types are stored as the raw JSON
    string of the geometry object so no information is lost.
    """
    if geometry is None:
        return ""
    geom_type = geometry.get("type")
    coords = geometry.get("coordinates")
    if geom_type == "Point" and isinstance(coords, list) and len(coords) >= 2:
        return f"POINT ({_fmt_coord(coords[0])} {_fmt_coord(coords[1])})"
    if geom_type == "LineString" and isinstance(coords, list):
        inside = ", ".join(f"{_fmt_coord(p[0])} {_fmt_coord(p[1])}" for p in coords if len(p) >= 2)
        return f"LINESTRING ({inside})"
    if geom_type == "Polygon" and isinstance(coords, list):
        rings = [
            "(" + ", ".join(f"{_fmt_coord(p[0])} {_fmt_coord(p[1])}" for p in ring if len(p) >= 2) + ")"
            for ring in coords
            if isinstance(ring, list)
        ]
        return f"POLYGON ({', '.join(rings)})"
    return json.dumps(geometry, separators=(",", ":"))


def _fmt_coord(value: Any) -> str:
    """Format a coordinate value: integers without trailing ``.0``; floats as-is."""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)
