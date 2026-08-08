"""Unit tests for the SelectColumns transform (TRANS-02, D-P6-11).

Covers subset projection, re-ordering, the actionable missing-column error
(naming the missing column AND the available ones), and the fail-fast empty
tuple guard at construction.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("pyarrow")

from datasluice.transforms.protocol import TransformContext  # noqa: E402
from datasluice.transforms.steps import SelectColumns  # noqa: E402


def _ctx(schema: Any) -> TransformContext:
    return TransformContext(arrow_schema=schema)


def test_select_subset() -> None:
    """Selecting a subset narrows the schema to the selected columns."""
    import pyarrow as pa

    schema = pa.schema([("id", pa.int64()), ("name", pa.string()), ("value", pa.float64())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2]), pa.array(["a", "b"]), pa.array([1.5, 2.5])], schema=schema)
    out = list(SelectColumns(("id", "name")).apply(iter([batch]), _ctx(schema)))
    assert out[0].schema.names == ["id", "name"]


def test_select_reorder() -> None:
    """Selecting in a different order re-orders the columns."""
    import pyarrow as pa

    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2]), pa.array(["a", "b"])], schema=schema)
    out = list(SelectColumns(("name", "id")).apply(iter([batch]), _ctx(schema)))
    assert out[0].schema.names == ["name", "id"]


def test_select_missing_raises() -> None:
    """A missing column raises TransformError naming it and the available ones (D-P6-11)."""
    import pyarrow as pa

    from datasluice.exceptions import TransformError

    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    with pytest.raises(TransformError) as exc_info:
        list(SelectColumns(("adress",)).apply(iter([]), _ctx(schema)))
    msg = str(exc_info.value)
    assert "adress" in msg
    assert "id" in msg and "name" in msg


def test_select_empty_tuple_raises() -> None:
    """An empty column tuple is rejected at construction (fail-fast, D-P6-07)."""
    with pytest.raises(ValueError):
        SelectColumns(())


def test_select_preserves_values() -> None:
    """Selected columns retain their original data values and ordering."""
    import pyarrow as pa

    schema = pa.schema([("id", pa.int64()), ("name", pa.string()), ("value", pa.float64())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2]), pa.array(["a", "b"]), pa.array([1.5, 2.5])], schema=schema)
    out = list(SelectColumns(("value", "id")).apply(iter([batch]), _ctx(schema)))
    assert out[0].column("value").to_pylist() == [1.5, 2.5]
    assert out[0].column("id").to_pylist() == [1, 2]


def test_select_multi_batch_preserves_order() -> None:
    """Column selection is stable across multiple input batches."""
    import pyarrow as pa

    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    b1 = pa.RecordBatch.from_arrays([pa.array([1]), pa.array(["a"])], schema=schema)
    b2 = pa.RecordBatch.from_arrays([pa.array([2]), pa.array(["b"])], schema=schema)
    out = list(SelectColumns(("name",)).apply(iter([b1, b2]), _ctx(schema)))
    assert len(out) == 2
    assert out[0].column("name").to_pylist() == ["a"]
    assert out[1].column("name").to_pylist() == ["b"]
