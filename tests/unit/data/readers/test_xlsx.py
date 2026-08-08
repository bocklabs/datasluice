"""Unit tests for the streaming XLSX reader (DATA-03, 04-02 Task 2)."""

from __future__ import annotations

import importlib
import io

import pytest

pytest.importorskip("pyarrow")
import pyarrow as pa  # noqa: E402

try:
    _readers_mod = importlib.import_module("datasluice.data.readers")
    _xlsx_mod = importlib.import_module("datasluice.data.readers.xlsx")
except ImportError:
    pytest.skip("datasluice.data.readers.xlsx not importable", allow_module_level=True)

READERS = _readers_mod.READERS
XLSXReader = _xlsx_mod.XLSXReader


def _make_xlsx_bytes() -> bytes:
    openpyxl = pytest.importorskip("openpyxl")
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["id", "name"])
    ws.append([1, "alice"])
    ws.append([2, "bob"])
    ws.append([3, "carol"])
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def test_read_batches_yields_record_batch() -> None:
    src = io.BytesIO(_make_xlsx_bytes())
    reader = XLSXReader()
    batches = list(reader.read_batches(src))
    table = pa.Table.from_batches(batches)
    assert table.num_rows == 3
    assert "id" in table.schema.names
    assert "name" in table.schema.names
    assert table.column("name").to_pylist() == ["alice", "bob", "carol"]


def test_xlsx_reader_in_registry() -> None:
    assert READERS["XLSX"] is XLSXReader
    assert READERS["XLS"] is XLSXReader


def test_read_batches_respects_batch_size() -> None:
    openpyxl = pytest.importorskip("openpyxl")
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["id"])
    for i in range(40):
        ws.append([i])
    buf = io.BytesIO()
    wb.save(buf)
    src = io.BytesIO(buf.getvalue())
    reader = XLSXReader()
    batches = list(reader.read_batches(src, batch_size=16))
    table = pa.Table.from_batches(batches)
    assert table.num_rows == 40
    assert len(batches) > 1


def test_read_batches_dedupes_duplicate_headers() -> None:
    """Duplicate column names get suffixed so no cell data is silently lost."""
    openpyxl = pytest.importorskip("openpyxl")
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["col", "col"])
    ws.append(["a", "b"])
    buf = io.BytesIO()
    wb.save(buf)
    src = io.BytesIO(buf.getvalue())
    reader = XLSXReader()
    batches = list(reader.read_batches(src))
    names = batches[0].schema.names
    assert len(names) == 2
    assert names[0] != names[1]


def test_read_batches_empty_sheet_yields_nothing() -> None:
    """A workbook with only a header row and no data yields no batches."""
    openpyxl = pytest.importorskip("openpyxl")
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["id", "name"])
    buf = io.BytesIO()
    wb.save(buf)
    src = io.BytesIO(buf.getvalue())
    reader = XLSXReader()
    batches = list(reader.read_batches(src))
    assert batches == []
