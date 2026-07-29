"""Unit tests for the CastSchema transform (TRANS-04, D-P6-10).

Covers a safe widening cast, the strict truncating-cast failure (raises
``TransformError`` wrapping ``ArrowInvalid`` — no silent data loss, mitigates
T-06-03), and an identity safe cast.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("pyarrow")

from datasluice.transforms.protocol import TransformContext  # noqa: E402
from datasluice.transforms.steps import CastSchema  # noqa: E402


def _ctx(schema: Any) -> TransformContext:
    return TransformContext(arrow_schema=schema)


def test_cast_widening_succeeds() -> None:
    """An int32→int64 widening cast yields int64 batches."""
    import pyarrow as pa

    src_schema = pa.schema([("v", pa.int32())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2], type=pa.int32())], schema=src_schema)
    out = list(CastSchema(pa.schema([("v", pa.int64())])).apply(iter([batch]), _ctx(src_schema)))
    assert out[0].schema.field("v").type == pa.int64()
    assert out[0].column("v").to_pylist() == [1, 2]


def test_cast_truncating_raises() -> None:
    """A truncating int64→int32 cast raises TransformError (wraps ArrowInvalid)."""
    import pyarrow as pa

    from datasluice.exceptions import TransformError

    src_schema = pa.schema([("v", pa.int64())])
    batch = pa.RecordBatch.from_arrays([pa.array([99999999999], type=pa.int64())], schema=src_schema)
    with pytest.raises(TransformError):
        list(CastSchema(pa.schema([("v", pa.int32())])).apply(iter([batch]), _ctx(src_schema)))


def test_cast_safe_identity() -> None:
    """A string→string safe cast is an identity."""
    import pyarrow as pa

    src_schema = pa.schema([("s", pa.string())])
    batch = pa.RecordBatch.from_arrays([pa.array(["a", "b"])], schema=src_schema)
    out = list(CastSchema(pa.schema([("s", pa.string())])).apply(iter([batch]), _ctx(src_schema)))
    assert out[0].schema == src_schema
    assert out[0].column("s").to_pylist() == ["a", "b"]
