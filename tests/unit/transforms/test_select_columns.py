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
