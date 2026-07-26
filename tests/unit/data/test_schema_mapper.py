"""Unit tests for :func:`to_arrow_schema` — domain Schema to pa.Schema mapper (DATA-07).

Covers portal type-string → Arrow type mapping, unknown-type defaulting,
nullable propagation, and empty-schema handling. Follows the Phase 03
RED→GREEN TDD pattern: the module skips cleanly at collection time while
``datasluice.data.schema`` does not exist, then runs and passes once Task 2
GREEN lands the mapper.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from datasluice.domain import Schema

pytest.importorskip("pyarrow")

try:
    _schema_module = importlib.import_module("datasluice.data.schema")
    to_arrow_schema = _schema_module.to_arrow_schema
except ImportError:
    pytest.skip("datasluice.data.schema not yet implemented (RED → GREEN within task 04-01)", allow_module_level=True)


def _make_schema(columns: list[dict]) -> Schema:
    from datasluice.domain.schema import Schema as _Schema

    return _Schema(name="test", columns=columns)


def test_known_type_mappings() -> None:
    """Known portal type strings map to expected pa types."""
    import pyarrow as pa

    schema = _make_schema(
        [
            {"name": "a", "type": "integer"},
            {"name": "b", "type": "number"},
            {"name": "c", "type": "string"},
            {"name": "d", "type": "boolean"},
            {"name": "e", "type": "date"},
            {"name": "f", "type": "datetime"},
        ]
    )
    result = to_arrow_schema(schema)
    assert result.field("a").type == pa.int64()
    assert result.field("b").type == pa.float64()
    assert result.field("c").type == pa.string()
    assert result.field("d").type == pa.bool_()
    assert result.field("e").type == pa.date32()
    assert result.field("f").type == pa.timestamp("us")


def test_type_aliases() -> None:
    """Type aliases (int, float, text, bool, timestamp) map correctly."""
    import pyarrow as pa

    schema = _make_schema(
        [
            {"name": "a", "type": "int"},
            {"name": "b", "type": "float"},
            {"name": "c", "type": "text"},
            {"name": "d", "type": "bool"},
            {"name": "e", "type": "timestamp"},
        ]
    )
    result = to_arrow_schema(schema)
    assert result.field("a").type == pa.int64()
    assert result.field("b").type == pa.float64()
    assert result.field("c").type == pa.string()
    assert result.field("d").type == pa.bool_()
    assert result.field("e").type == pa.timestamp("us")


def test_unknown_type_defaults_to_string() -> None:
    """An unrecognized type string maps to pa.string()."""
    import pyarrow as pa

    schema = _make_schema([{"name": "weird", "type": "geometry"}])
    result = to_arrow_schema(schema)
    assert result.field("weird").type == pa.string()


def test_nullable_propagation() -> None:
    """Columns with nullable=False produce non-nullable pa fields."""

    schema = _make_schema(
        [
            {"name": "req", "type": "integer", "nullable": False},
            {"name": "opt", "type": "integer", "nullable": True},
            {"name": "default", "type": "integer"},
        ]
    )
    result = to_arrow_schema(schema)
    assert result.field("req").nullable is False
    assert result.field("opt").nullable is True
    assert result.field("default").nullable is True


def test_empty_columns() -> None:
    """An empty columns list produces an empty pa.Schema."""
    import pyarrow as pa

    schema = _make_schema([])
    result = to_arrow_schema(schema)
    assert len(result) == 0
    assert isinstance(result, pa.Schema)
