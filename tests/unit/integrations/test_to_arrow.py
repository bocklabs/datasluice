"""Unit tests for :func:`to_arrow`.

Follows the importorskip + inline-import test pattern (matches
``tests/unit/data/test_batch_stream.py``).
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyarrow")

from datasluice.data.batch_stream import BatchStream
from datasluice.integrations.arrow import to_arrow


def test_to_arrow_returns_table() -> None:
    """to_arrow returns a pa.Table with matching schema and row count."""
    import pyarrow as pa

    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2]), pa.array(["a", "b"])], schema=schema)
    stream = BatchStream(iter([batch]), schema)

    table = to_arrow(stream)

    assert isinstance(table, pa.Table)
    assert table.schema == schema
    assert len(table) == 2


def test_to_arrow_preserves_nulls() -> None:
    """to_arrow preserves nulls (a None input is None in the output Table, not dropped)."""
    import pyarrow as pa

    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2]), pa.array(["a", None])], schema=schema)
    stream = BatchStream(iter([batch]), schema)

    table = to_arrow(stream)

    assert table.column("name").is_null().to_pylist()[1] is True
    assert len(table) == 2


def test_to_arrow_concatenates_batches() -> None:
    """to_arrow concatenates multiple batches into one Table."""
    import pyarrow as pa

    schema = pa.schema([("id", pa.int64())])
    batch1 = pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], schema=schema)
    batch2 = pa.RecordBatch.from_arrays([pa.array([4, 5])], schema=schema)
    stream = BatchStream(iter([batch1, batch2]), schema)

    table = to_arrow(stream)

    assert len(table) == 5
    assert table.column("id").to_pylist() == [1, 2, 3, 4, 5]


def test_to_arrow_empty_stream() -> None:
    """to_arrow on an empty stream returns a Table with the stream's (empty) schema and 0 rows."""
    import pyarrow as pa

    schema = pa.schema([])
    stream = BatchStream(iter(()), schema)

    table = to_arrow(stream)

    assert isinstance(table, pa.Table)
    assert len(table) == 0
    assert table.schema == schema
