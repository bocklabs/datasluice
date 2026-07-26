"""Unit tests for :func:`unify_batches` — schema unification via ``pa.concat_tables`` (DATA-08).

Covers the documented promotion lattice: int→float widening, missing-column
null-fill, string+binary→binary (the lossy direction per RESEARCH Pitfall 6),
and the tz-aware vs tz-naive timestamp hard-fail case (RESEARCH Pitfall 4).

Follows the established Phase 03/04 RED→GREEN TDD pattern: the module skips
cleanly at collection time while ``unify_batches`` is not yet exported from
``datasluice.data.schema`` (RED), then runs and passes once Task 1 GREEN lands
the implementation.
"""

from __future__ import annotations

import importlib
from typing import Any

import pytest

pytest.importorskip("pyarrow")

unify_batches: Any
try:
    _schema_module = importlib.import_module("datasluice.data.schema")
    unify_batches = getattr(_schema_module, "unify_batches", None)
except ImportError:
    unify_batches = None

if unify_batches is None:
    pytest.skip(
        "datasluice.data.schema.unify_batches not yet implemented (RED → GREEN within task 04-04)",
        allow_module_level=True,
    )


def test_int_to_float_widening() -> None:
    """int64 + float64 columns unify to float64 with values preserved."""
    import pyarrow as pa

    b1 = pa.RecordBatch.from_arrays([pa.array([1, 2, 3], type=pa.int64())], names=["v"])
    b2 = pa.RecordBatch.from_arrays([pa.array([4.5, 5.5], type=pa.float64())], names=["v"])
    table = unify_batches([b1, b2])
    assert table.column("v").type == pa.float64()
    assert table.column("v").to_pylist() == [1.0, 2.0, 3.0, 4.5, 5.5]


def test_missing_column_null_fills() -> None:
    """Disjoint column sets unify; absent data is null-filled."""
    import pyarrow as pa

    b1 = pa.RecordBatch.from_arrays(
        [pa.array([1, 2], type=pa.int64()), pa.array(["a", "b"])],
        names=["x", "y"],
    )
    b2 = pa.RecordBatch.from_arrays(
        [pa.array([3], type=pa.int64()), pa.array([True])],
        names=["x", "z"],
    )
    table = unify_batches([b1, b2])
    assert set(table.column_names) == {"x", "y", "z"}
    assert table.column("x").to_pylist() == [1, 2, 3]
    assert table.column("y").to_pylist() == ["a", "b", None]
    assert table.column("z").to_pylist() == [None, None, True]


def test_string_plus_binary_unifies_to_binary() -> None:
    """string + binary columns unify to binary (the lossy direction per RESEARCH Pitfall 6)."""
    import pyarrow as pa

    b1 = pa.RecordBatch.from_arrays([pa.array(["hello"], type=pa.string())], names=["s"])
    b2 = pa.RecordBatch.from_arrays([pa.array([b"world"], type=pa.binary())], names=["s"])
    table = unify_batches([b1, b2])
    assert table.column("s").type == pa.binary()
    assert table.column("s").to_pylist() == [b"hello", b"world"]


def test_tz_mismatch_raises() -> None:
    """tz-aware vs tz-naive timestamp columns cannot unify — SchemaUnificationError fires."""
    import pyarrow as pa

    from datasluice.exceptions import SchemaUnificationError

    b1 = pa.RecordBatch.from_arrays(
        [pa.array([1], type=pa.timestamp("us", tz="UTC"))],
        names=["ts"],
    )
    b2 = pa.RecordBatch.from_arrays(
        [pa.array([2], type=pa.timestamp("us"))],
        names=["ts"],
    )
    with pytest.raises(SchemaUnificationError) as exc_info:
        unify_batches([b1, b2])
    msg = str(exc_info.value).lower()
    assert "timezone" in msg or "tz" in msg, f"expected timezone mention in: {exc_info.value}"
    assert "normalizetimestamps" in msg, f"expected NormalizeTimestamps suggestion in: {exc_info.value}"


def test_single_batch_passthrough() -> None:
    """A single batch unifies to itself with no change."""
    import pyarrow as pa

    b1 = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3], type=pa.int64())],
        names=["v"],
    )
    table = unify_batches([b1])
    assert table.num_rows == 3
    assert table.column("v").to_pylist() == [1, 2, 3]


def test_homogeneous_batches_unify_cleanly() -> None:
    """Multiple batches with identical schemas unify without error."""
    import pyarrow as pa

    b1 = pa.RecordBatch.from_arrays(
        [pa.array([1, 2], type=pa.int64()), pa.array(["a", "b"])],
        names=["id", "name"],
    )
    b2 = pa.RecordBatch.from_arrays(
        [pa.array([3, 4], type=pa.int64()), pa.array(["c", "d"])],
        names=["id", "name"],
    )
    b3 = pa.RecordBatch.from_arrays(
        [pa.array([5], type=pa.int64()), pa.array(["e"])],
        names=["id", "name"],
    )
    table = unify_batches([b1, b2, b3])
    assert table.num_rows == 5
    assert table.column("id").to_pylist() == [1, 2, 3, 4, 5]
    assert table.column("name").to_pylist() == ["a", "b", "c", "d", "e"]
