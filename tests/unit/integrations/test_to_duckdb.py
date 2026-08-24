"""Unit tests for :func:`to_duckdb`.

Follows the importorskip + inline-import test pattern. Verifies the
relation-API registration path (``conn.register`` + ``conn.table`` — no SQL
string interpolation, ) and the preserved
:func:`_validate_table_name` injection guard at the to_duckdb boundary.
"""

from __future__ import annotations

import importlib
from typing import Any

import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("duckdb")

to_duckdb: Any
try:
    _duckdb_module = importlib.import_module("datasluice.integrations.duckdb")
    to_duckdb = getattr(_duckdb_module, "to_duckdb", None)
except ImportError:  # RED phase: module not yet importable
    to_duckdb = None

if to_duckdb is None:
    pytest.skip("to_duckdb not yet implemented (RED -> GREEN)", allow_module_level=True)

from datasluice.data.batch_stream import BatchStream
from datasluice.integrations.duckdb import _validate_table_name


def test_to_duckdb_returns_named_relation() -> None:
    """to_duckdb registers under table_name and returns a relation whose rows match the stream."""
    import pyarrow as pa

    schema = pa.schema([("id", pa.int64())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2])], schema=schema)
    stream = BatchStream(iter([batch]), schema)

    rel = to_duckdb(stream, table_name="my_table")

    assert rel.fetchall() == [(1,), (2,)]


def test_to_duckdb_default_table_name() -> None:
    """to_duckdb(stream) registers under the default 'datasluice' name."""
    import pyarrow as pa

    schema = pa.schema([("id", pa.int64())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2])], schema=schema)
    stream = BatchStream(iter([batch]), schema)

    rel = to_duckdb(stream)

    assert rel.fetchall() == [(1,), (2,)]


def test_to_duckdb_injected_conn_reused() -> None:
    """to_duckdb(stream, conn=existing_conn) reuses the caller-injected connection."""
    import duckdb
    import pyarrow as pa

    schema = pa.schema([("id", pa.int64())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2])], schema=schema)
    stream = BatchStream(iter([batch]), schema)

    conn = duckdb.connect()
    to_duckdb(stream, conn=conn)

    assert conn.table("datasluice").fetchall() == [(1,), (2,)]


@pytest.mark.parametrize("bad_name", ["x; DROP", "bad name", "1lead", "", "'); DROP TABLE x;--"])
def test_to_duckdb_rejects_bad_table_name(bad_name: str) -> None:
    """to_duckdb rejects injection payloads via _validate_table_name."""
    import pyarrow as pa

    schema = pa.schema([("id", pa.int64())])
    batch = pa.RecordBatch.from_arrays([pa.array([1])], schema=schema)
    stream = BatchStream(iter([batch]), schema)

    with pytest.raises(ValueError):
        to_duckdb(stream, table_name=bad_name)


def test_to_duckdb_preserves_nulls() -> None:
    """to_duckdb preserves nulls."""
    import pyarrow as pa

    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2]), pa.array(["a", None])], schema=schema)
    stream = BatchStream(iter([batch]), schema)

    table = to_duckdb(stream).to_arrow_table()

    assert table.column("name").null_count == 1
    assert len(table) == 2


def test_validate_table_name_still_present() -> None:
    """The guard _validate_table_name is preserved alongside to_duckdb."""
    assert _validate_table_name("good_name") == "good_name"
    with pytest.raises(ValueError):
        _validate_table_name("bad name")
