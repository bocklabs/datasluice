"""Unit tests for the streaming Parquet reader (DATA-03, 04-02 Task 2).

Covers both seekable (BytesIO) and non-seekable (IterableBytesIO) paths —
the latter exercises the spool-to-BytesIO landmine mitigation (RESEARCH
Pitfall 1: Parquet footers live at EOF and require a seek).
"""

from __future__ import annotations

import importlib
import io

import pytest

pytest.importorskip("pyarrow")
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

try:
    _readers_mod = importlib.import_module("datasluice.data.readers")
    _parquet_mod = importlib.import_module("datasluice.data.readers.parquet")
except ImportError:
    pytest.skip("datasluice.data.readers.parquet not importable", allow_module_level=True)

from datasluice.data._byte_source import IterableBytesIO  # noqa: E402

READERS = _readers_mod.READERS
ParquetReader = _parquet_mod.ParquetReader


def _sample_table() -> pa.Table:
    return pa.table({"id": pa.array([1, 2, 3], type=pa.int64()), "name": pa.array(["a", "b", "c"])})


def test_read_batches_seekable() -> None:
    buf = io.BytesIO()
    pq.write_table(_sample_table(), buf)
    buf.seek(0)
    reader = ParquetReader()
    batches = list(reader.read_batches(buf))
    table = pa.Table.from_batches(batches)
    assert table.num_rows == 3
    assert table.schema.field("id").type == pa.int64()
    assert table.column("name").to_pylist() == ["a", "b", "c"]


def test_read_batches_non_seekable_spools() -> None:
    buf = io.BytesIO()
    pq.write_table(_sample_table(), buf)
    raw = buf.getvalue()
    src = IterableBytesIO([raw[:10], raw[10:50], raw[50:]])
    assert src.seekable() is False
    reader = ParquetReader()
    batches = list(reader.read_batches(src))
    table = pa.Table.from_batches(batches)
    assert table.num_rows == 3
    assert table.column("name").to_pylist() == ["a", "b", "c"]


def test_parquet_reader_in_registry() -> None:
    assert READERS["PARQUET"] is ParquetReader


def test_read_batches_accepts_batch_size_kwarg() -> None:
    table = pa.table({"id": pa.array(list(range(50)), type=pa.int64())})
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    reader = ParquetReader()
    batches = list(reader.read_batches(buf, batch_size=16))
    result = pa.Table.from_batches(batches)
    assert result.num_rows == 50
    assert len(batches) > 1
    assert all(b.num_rows <= 16 for b in batches)
    assert result.column("id").to_pylist() == list(range(50))
