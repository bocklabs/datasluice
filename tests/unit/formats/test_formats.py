"""Unit tests for format readers."""

from __future__ import annotations

import pytest

from datasluice.formats import CSVReader, JSONReader, get_reader
from datasluice.formats.base import BaseFormatReader


def test_csv_reader(tmp_path) -> None:  # type: ignore[no-untyped-def]
    f = tmp_path / "data.csv"
    f.write_text("name,age\nAlice,30\nBob,25\n")
    reader = CSVReader()
    records = reader.read(f)
    assert len(records) == 2
    assert records[0]["name"] == "Alice"
    assert records[0]["age"] == "30"


def test_csv_reader_bytes() -> None:
    data = b"a,b\n1,2\n"
    reader = CSVReader()
    records = reader.read(data)
    assert records[0]["a"] == "1"


def test_json_reader_array() -> None:
    data = b'[{"a": 1}, {"a": 2}]'
    reader = JSONReader()
    records = reader.read(data)
    assert len(records) == 2
    assert records[0]["a"] == 1


def test_json_reader_jsonl() -> None:
    data = b'{"a": 1}\n{"a": 2}\n'
    reader = JSONReader()
    records = reader.read(data)
    assert len(records) == 2


def test_json_reader_empty() -> None:
    reader = JSONReader()
    assert reader.read(b"") == []


def test_get_reader() -> None:
    reader = get_reader("CSV")
    assert isinstance(reader, BaseFormatReader)
    assert isinstance(reader, CSVReader)


def test_get_reader_unknown() -> None:
    with pytest.raises(KeyError):
        get_reader("UNKNOWN_FORMAT")
