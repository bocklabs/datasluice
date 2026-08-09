"""Unit tests for the NormalizeTimestamps transform.

Covers the three-way branch: tz-naive → assume_timezone then cast (NEVER a
direct naive→aware cast, ), tz-aware non-UTC → target tz,
same-tz unit change, and the non-timestamp pass-through. Each test asserts the
OUTPUT TYPE (not internal calls) to confirm the branch was taken correctly.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("pyarrow")

from datasluice.transforms.protocol import TransformContext  # noqa: E402
from datasluice.transforms.steps import NormalizeTimestamps  # noqa: E402


def _ctx(schema: Any) -> TransformContext:
    return TransformContext(arrow_schema=schema)


def test_naive_to_aware_utc() -> None:
    """A tz-naive column becomes tz-aware UTC at the target unit."""
    import pyarrow as pa

    schema = pa.schema([("ts", pa.timestamp("us"))])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2], type=pa.timestamp("us"))], schema=schema)
    out = list(NormalizeTimestamps().apply(iter([batch]), _ctx(schema)))
    assert out[0].schema.field("ts").type == pa.timestamp("us", tz="UTC")


def test_aware_nonutc_to_utc() -> None:
    """A tz-aware non-UTC column is converted to the target tz (UTC)."""
    import pyarrow as pa

    schema = pa.schema([("ts", pa.timestamp("us", tz="America/New_York"))])
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2], type=pa.timestamp("us", tz="America/New_York"))], schema=schema
    )
    out = list(NormalizeTimestamps().apply(iter([batch]), _ctx(schema)))
    assert out[0].schema.field("ts").type == pa.timestamp("us", tz="UTC")


def test_same_tz_unit_change() -> None:
    """A same-tz, different-unit timestamp is cast to the target unit."""
    import pyarrow as pa

    schema = pa.schema([("ts", pa.timestamp("ns", tz="UTC"))])
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1_000_000, 2_000_000], type=pa.timestamp("ns", tz="UTC"))], schema=schema
    )
    out = list(NormalizeTimestamps().apply(iter([batch]), _ctx(schema)))
    assert out[0].schema.field("ts").type == pa.timestamp("us", tz="UTC")


def test_non_timestamp_columns_untouched() -> None:
    """Non-timestamp columns pass through with their original type."""
    import pyarrow as pa

    schema = pa.schema([("ts", pa.timestamp("us")), ("id", pa.int64())])
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2], type=pa.timestamp("us")), pa.array([5, 6], type=pa.int64())],
        schema=schema,
    )
    out = list(NormalizeTimestamps().apply(iter([batch]), _ctx(schema)))
    assert out[0].schema.field("id").type == pa.int64()
    assert out[0].column("id").to_pylist() == [5, 6]
    assert out[0].schema.field("ts").type == pa.timestamp("us", tz="UTC")


def test_dst_fold_raises_transform_error() -> None:
    """A timestamp that falls in a DST fold/gap raises TransformError, not a raw Arrow exception."""
    import pyarrow as pa

    from datasluice.exceptions import TransformError

    schema = pa.schema([("ts", pa.timestamp("us"))])
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1604194200000000], type=pa.timestamp("us"))],
        schema=schema,
    )
    with pytest.raises(TransformError):
        list(
            NormalizeTimestamps(target_tz="America/New_York", assume_naive_tz="America/New_York").apply(
                iter([batch]), _ctx(schema)
            )
        )
