"""Unit tests for the Filter transform.

Verifies ``Filter`` delegates row filtering to ``RecordBatch.filter`` via a
pyarrow compute ``Expression``: single-expression, compound (``&``), and the
zero-row edge case.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

pytest.importorskip("pyarrow")

from datasluice.transforms.protocol import TransformContext  # noqa: E402
from datasluice.transforms.steps import Filter  # noqa: E402

if TYPE_CHECKING:
    from collections.abc import Iterator


def _ctx(schema: Any) -> TransformContext:
    return TransformContext(arrow_schema=schema)


def test_filter_expression_filters_rows() -> None:
    """A single comparison expression keeps only the matching rows."""
    import pyarrow as pa
    import pyarrow.compute as pc

    schema = pa.schema([("id", pa.int64())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3, 4], type=pa.int64())], schema=schema)
    out: list[Any] = list(Filter(pc.field("id") > 2).apply(iter([batch]), _ctx(schema)))
    assert len(out) == 1
    assert out[0].column("id").to_pylist() == [3, 4]


def test_filter_compound_expression() -> None:
    """A compound (AND) expression narrows the result set."""
    import pyarrow as pa
    import pyarrow.compute as pc

    schema = pa.schema([("id", pa.int64())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3, 4], type=pa.int64())], schema=schema)
    expr = (pc.field("id") > 1) & (pc.field("id") < 4)
    out: list[Any] = list(Filter(expr).apply(iter([batch]), _ctx(schema)))
    assert out[0].column("id").to_pylist() == [2, 3]


def test_filter_zero_results() -> None:
    """An expression matching no rows yields an empty batch (not no batches)."""
    import pyarrow as pa
    import pyarrow.compute as pc

    schema = pa.schema([("id", pa.int64())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3], type=pa.int64())], schema=schema)
    out: Iterator[Any] = Filter(pc.field("id") > 100).apply(iter([batch]), _ctx(schema))
    result = list(out)
    assert len(result) == 1
    assert result[0].num_rows == 0
