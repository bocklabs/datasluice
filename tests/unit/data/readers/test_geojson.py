"""Unit tests for the streaming GeoJSON reader.

Covers the Feature-flatten + geometry_wkt column contract:
- FeatureCollection flattens to one row per Feature
- properties dict becomes typed columns
- geometry becomes a ``geometry_wkt`` string column
- minimal WKT encoder handles Point, LineString, Polygon
"""

from __future__ import annotations

import importlib
import io
import json

import pytest

pytest.importorskip("pyarrow")
import pyarrow as pa  # noqa: E402

try:
    _readers_mod = importlib.import_module("datasluice.data.readers")
    _geojson_mod = importlib.import_module("datasluice.data.readers.geojson")
except ImportError:
    pytest.skip("datasluice.data.readers.geojson not importable", allow_module_level=True)

READERS = _readers_mod.READERS
GeoJSONReader = _geojson_mod.GeoJSONReader


def test_read_batches_feature_collection_point() -> None:
    gj = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"id": 1, "name": "a"},
                "geometry": {"type": "Point", "coordinates": [1.0, 2.0]},
            },
            {
                "type": "Feature",
                "properties": {"id": 2, "name": "b"},
                "geometry": {"type": "Point", "coordinates": [3.0, 4.0]},
            },
        ],
    }
    src = io.BytesIO(json.dumps(gj).encode("utf-8"))
    reader = GeoJSONReader()
    batches = list(reader.read_batches(src))
    table = pa.Table.from_batches(batches)
    assert table.num_rows == 2
    assert "geometry_wkt" in table.schema.names
    assert table.schema.field("geometry_wkt").type == pa.string()
    wkts = table.column("geometry_wkt").to_pylist()
    assert wkts[0] == "POINT (1 2)"
    assert wkts[1] == "POINT (3 4)"
    assert table.column("name").to_pylist() == ["a", "b"]


def test_read_batches_single_feature() -> None:
    feature = {
        "type": "Feature",
        "properties": {"id": 7, "label": "solo"},
        "geometry": {"type": "Point", "coordinates": [10.0, 20.0]},
    }
    src = io.BytesIO(json.dumps(feature).encode("utf-8"))
    reader = GeoJSONReader()
    batches = list(reader.read_batches(src))
    table = pa.Table.from_batches(batches)
    assert table.num_rows == 1
    assert table.column("geometry_wkt").to_pylist() == ["POINT (10 20)"]


def test_read_batches_linestring_and_polygon() -> None:
    gj = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"id": 1},
                "geometry": {"type": "LineString", "coordinates": [[0.0, 0.0], [1.0, 1.0]]},
            },
            {
                "type": "Feature",
                "properties": {"id": 2},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]],
                },
            },
        ],
    }
    src = io.BytesIO(json.dumps(gj).encode("utf-8"))
    reader = GeoJSONReader()
    batches = list(reader.read_batches(src))
    table = pa.Table.from_batches(batches)
    wkts = table.column("geometry_wkt").to_pylist()
    assert wkts[0] == "LINESTRING (0 0, 1 1)"
    assert wkts[1] == "POLYGON ((0 0, 1 0, 1 1, 0 0))"


def test_read_batches_complex_geometry_falls_back_to_json() -> None:
    gj = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"id": 1},
                "geometry": {
                    "type": "MultiPolygon",
                    "coordinates": [[[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]]],
                },
            },
        ],
    }
    src = io.BytesIO(json.dumps(gj).encode("utf-8"))
    reader = GeoJSONReader()
    batches = list(reader.read_batches(src))
    table = pa.Table.from_batches(batches)
    wkt = table.column("geometry_wkt").to_pylist()[0]
    assert wkt.startswith("{")
    assert "MultiPolygon" in wkt


def test_geojson_reader_in_registry() -> None:
    assert READERS["GEOJSON"] is GeoJSONReader
