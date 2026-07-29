"""Unit tests for :func:`to_polars` (INTG-03, D-P6-01).

Follows the Phase 04/06 importorskip + inline-import test pattern. Verifies
delegation through the shared :func:`to_arrow` substrate (QUAL-10).
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("polars")

from datasluice.data.batch_stream import BatchStream  # noqa: E402
from datasluice.integrations.polars import to_polars  # noqa: E402


def test_to_polars_returns_dataframe() -> None:
    """to_polars returns a pl.DataFrame with matching columns and height."""
    import polars as pl
    import pyarrow as pa

    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2]), pa.array(["a", "b"])], schema=schema)
    stream = BatchStream(iter([batch]), schema)

    df = to_polars(stream)

    assert isinstance(df, pl.DataFrame)
    assert df.columns == ["id", "name"]
    assert df.height == 2


def test_to_polars_preserves_row_count() -> None:
    """to_polars preserves the row count of a 3-row stream."""
    import pyarrow as pa

    schema = pa.schema([("id", pa.int64()), ("name", pa.string()), ("value", pa.float64())])
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array(["a", "b", "c"]), pa.array([1.5, 2.5, 3.5])],
        schema=schema,
    )
    stream = BatchStream(iter([batch]), schema)

    df = to_polars(stream)

    assert df.height == 3


def test_to_polars_delegates_through_to_arrow() -> None:
    """to_polars DataFrame columns match the stream schema names (substrate path)."""
    import pyarrow as pa

    schema = pa.schema([("year", pa.int64()), ("city", pa.string())])
    batch = pa.RecordBatch.from_arrays([pa.array([2024, 2025]), pa.array(["Luxembourg", "Paris"])], schema=schema)
    stream = BatchStream(iter([batch]), schema)

    df = to_polars(stream)

    assert df.columns == list(schema.names)
