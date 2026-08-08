"""Unit tests for the Flatten transform (TRANS-07, D-P6-13).

Covers one-level struct flattening (dotted names), ``max_depth=2`` recursion
into a nested struct (RESEARCH Pitfall 6), the list-column-untouched policy,
and the no-struct no-op. The ``RecordBatch`` path goes through ``pa.Table``
(RESEARCH Pitfall 4 — ``RecordBatch`` has no ``.flatten()``).
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("pyarrow")

from datasluice.transforms.protocol import TransformContext  # noqa: E402
from datasluice.transforms.steps import Flatten  # noqa: E402


def _ctx(schema: Any) -> TransformContext:
    return TransformContext(arrow_schema=schema)


def test_flatten_one_level() -> None:
    """A struct column flattens into dotted child columns at max_depth=1."""
    import pyarrow as pa

    struct_type = pa.struct([pa.field("city", pa.string()), pa.field("zip", pa.string())])
    schema = pa.schema([("address", struct_type)])
    batch = pa.RecordBatch.from_arrays([pa.array([{"city": "NYC", "zip": "10001"}], type=struct_type)], schema=schema)
    out = list(Flatten(max_depth=1).apply(iter([batch]), _ctx(schema)))
    assert out[0].schema.names == ["address.city", "address.zip"]
    assert out[0].column("address.city").to_pylist() == ["NYC"]


def test_flatten_max_depth_two() -> None:
    """A two-level nested struct fully flattens at max_depth=2."""
    import pyarrow as pa

    inner = pa.struct([pa.field("deep", pa.string())])
    outer = pa.struct([pa.field("inner", inner)])
    schema = pa.schema([("outer", outer)])
    batch = pa.RecordBatch.from_arrays([pa.array([{"inner": {"deep": "x"}}], type=outer)], schema=schema)
    out = list(Flatten(max_depth=2).apply(iter([batch]), _ctx(schema)))
    assert out[0].schema.names == ["outer.inner.deep"]
    assert out[0].column("outer.inner.deep").to_pylist() == ["x"]


def test_flatten_list_untouched() -> None:
    """A list column alongside a struct is left untouched by flattening."""
    import pyarrow as pa

    struct_type = pa.struct([pa.field("city", pa.string())])
    list_type = pa.list_(pa.int64())
    schema = pa.schema([("address", struct_type), ("tags", list_type)])
    batch = pa.RecordBatch.from_arrays(
        [pa.array([{"city": "NYC"}], type=struct_type), pa.array([[1, 2, 3]], type=list_type)],
        schema=schema,
    )
    out = list(Flatten(max_depth=1).apply(iter([batch]), _ctx(schema)))
    assert out[0].schema.names == ["address.city", "tags"]
    assert out[0].column("tags").to_pylist() == [[1, 2, 3]]


def test_flatten_no_struct_is_noop() -> None:
    """A batch with only scalar columns is yielded unchanged."""
    import pyarrow as pa

    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2]), pa.array(["a", "b"])], schema=schema)
    out = list(Flatten(max_depth=1).apply(iter([batch]), _ctx(schema)))
    assert out[0].schema.names == ["id", "name"]


def test_flatten_max_depth_zero_raises() -> None:
    """Flatten rejects max_depth < 1 at construction."""
    with pytest.raises(ValueError):
        Flatten(max_depth=0)
