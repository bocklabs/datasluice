"""Golden logical-content SHA-256 behavior."""

from __future__ import annotations

import importlib
import importlib.util
import os
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

if importlib.util.find_spec("datasluice.sync._hashing") is None:
    if os.environ.get("DATASLUICE_TDD_RED") != "1":
        pytest.skip("logical hashing implementation pending GREEN phase", allow_module_level=True)

    def logical_sha256(table: Any) -> str:
        return ""

else:
    hashing_module = importlib.import_module("datasluice.sync._hashing")
    logical_sha256: Any = hashing_module.logical_sha256

GOLDEN_BASIC_SHA256 = "178504a34646005b155886ba5f39b3dfa6ec20af8e44db6b5f839a305cfcb932"


def test_identical_tables_equal() -> None:
    direct = pa.table({"id": [1, 2], "name": ["a", "b"]})
    from_rows = pa.Table.from_pylist([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])

    assert logical_sha256(direct) == GOLDEN_BASIC_SHA256
    assert logical_sha256(from_rows) == GOLDEN_BASIC_SHA256


def test_parquet_roundtrip_stable(tmp_path) -> None:
    table = pa.table({"id": [1, 2], "name": ["a", "b"]})
    path = tmp_path / "roundtrip.parquet"
    pq.write_table(table, path)

    roundtripped = pq.read_table(path)

    assert logical_sha256(table) == logical_sha256(roundtripped)


def test_one_cell_changed_differs() -> None:
    original = pa.table({"id": [1, 2], "name": ["a", "b"]})
    changed = pa.table({"id": [1, 2], "name": ["a", "changed"]})

    assert logical_sha256(original) != logical_sha256(changed)


def test_schema_order_matters() -> None:
    original = pa.table({"id": [1, 2], "name": ["a", "b"]})
    reordered = pa.table({"name": ["a", "b"], "id": [1, 2]})

    assert logical_sha256(original) != logical_sha256(reordered)


def test_nullability_matters() -> None:
    nullable = pa.Table.from_arrays(
        [pa.array([1, 2])],
        schema=pa.schema([pa.field("id", pa.int64(), nullable=True)]),
    )
    required = pa.Table.from_arrays(
        [pa.array([1, 2])],
        schema=pa.schema([pa.field("id", pa.int64(), nullable=False)]),
    )

    assert logical_sha256(nullable) != logical_sha256(required)


def test_empty_table_is_stable() -> None:
    empty = pa.table({"id": pa.array([], pa.int64())})
    assert isinstance(logical_sha256(empty), str)
    assert logical_sha256(empty) == logical_sha256(pa.table({"id": pa.array([], pa.int64())}))


def test_null_values_differ_from_zero() -> None:
    with_nulls = pa.table({"id": [1, None]})
    with_zero = pa.table({"id": [1, 0]})
    assert logical_sha256(with_nulls) != logical_sha256(with_zero)


def test_row_order_matters() -> None:
    forward = pa.table({"id": [1, 2], "name": ["a", "b"]})
    reverse = pa.table({"id": [2, 1], "name": ["b", "a"]})
    assert logical_sha256(forward) != logical_sha256(reverse)


def test_chunk_split_is_equal() -> None:
    single_chunk = pa.table({"id": [1, 2, 3, 4]})
    multi_chunk = pa.Table.from_batches(
        [
            pa.RecordBatch.from_pydict({"id": [1, 2]}),
            pa.RecordBatch.from_pydict({"id": [3, 4]}),
        ]
    )
    assert logical_sha256(single_chunk) == logical_sha256(multi_chunk)
