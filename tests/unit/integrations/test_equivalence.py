"""result-equivalence: one BatchStream -> 4 terminals -> same data.

Feeds ONE synthetic BatchStream definition (typed columns + a null) through all
four terminals (:func:`to_arrow`, :func:`to_pandas`, :func:`to_polars`,
:func:`to_duckdb`), normalizes each output to a ``pa.Table``, and asserts
``pa.Table.equals`` across all pairs. Null-representation divergences (pandas
``NaN`` vs Arrow ``None``, ) and DuckDB local-tz divergence
 are normalized at the Arrow level — never via
``.fetchall()`` object comparison.

A :class:`~datasluice.data.BatchStream` is a one-shot iterator, so a FRESH
stream is built per terminal via :func:`_make_stream`.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("pandas")
pytest.importorskip("polars")
pytest.importorskip("duckdb")

from datasluice.data.batch_stream import BatchStream  # noqa: E402
from datasluice.integrations.arrow import to_arrow  # noqa: E402
from datasluice.integrations.duckdb import to_duckdb  # noqa: E402
from datasluice.integrations.pandas import to_pandas  # noqa: E402
from datasluice.integrations.polars import to_polars  # noqa: E402


def _normalize_string_types(table: Any) -> Any:
    """Normalize Arrow type representation divergences across terminals.

    The four terminals produce equivalent DATA but different Arrow type
    metadata. Two divergences are normalized here:

    1. **String type**: ``to_arrow``/``to_duckdb``
       yield ``large_string`` (pyarrow default inference + DuckDB interop),
       while the pandas/polars round-trips yield ``string``. All string columns
       are cast to ``large_string``.

    2. **Timestamp tz**: DuckDB's Arrow export stamps
       tz-aware timestamps with the local system timezone (e.g.
       ``Europe/Luxembourg``) instead of preserving the source ``UTC``. The
       epoch values are identical; only the tz metadata diverges. All timestamp
       columns are cast to ``tz=UTC`` (preserving their unit).

    This is the "same data" contract, not a byte-identical schema
    contract — analogous to the NaN->None value normalization.
    """
    import pyarrow as pa

    new_fields = []
    for field in table.schema:
        if pa.types.is_string(field.type):
            new_fields.append(pa.field(field.name, pa.large_string()))
        elif pa.types.is_timestamp(field.type):
            new_fields.append(pa.field(field.name, pa.timestamp(field.type.unit, tz="UTC")))
        else:
            new_fields.append(field)
    return table.cast(pa.schema(new_fields))


@pytest.fixture(autouse=True)
def _skip_if_pandas_index_broken() -> None:
    """Skip when pandas 3.0.3 Arrow-backed construction is corrupted mid-process.

    Mirrors the guard in ``test_to_pandas.py``: the pandas
    ``ArrowStringArray`` Index bug (``AssertionError`` at
    ``pandas/core/indexes/base.py:665``) manifests intermittently after
    cumulative in-process test state. The equivalence path runs ``to_pandas``
    (Arrow->DataFrame) then ``pa.Table.from_pandas``, both of which touch the
    ArrowStringArray construction path; a bare ``pd.Index`` probe is
    insufficient — this probe replicates the full Arrow->pandas DataFrame
    construction that actually triggers the bug, so it fires earlier.
    """
    import pyarrow as pa

    try:
        pa.Table.from_pydict({"_ds_eq_probe": ["a"]}).to_pandas()
    except AssertionError:
        pytest.skip("pandas 3.0.3 ArrowStringArray Index bug (full-suite state pollution)")


def _make_stream() -> BatchStream:
    """Build a FRESH BatchStream with typed columns + a deliberate null.

       The null in ``name[1]`` surfaces the null-representation divergence
    that the equivalence
       assertion must normalize.
    """
    import pyarrow as pa

    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2]), pa.array(["a", None])], schema=schema)
    return BatchStream(iter([batch]), schema)


def _make_timestamp_stream() -> BatchStream:
    """Build a FRESH BatchStream with a tz-aware timestamp + int + null.

    Catches the DuckDB local-tz pitfall: DuckDB
    ``.fetchall()`` applies the local system timezone, but the Arrow-level
    comparison via ``rel.to_arrow_table()`` preserves the correct tz metadata.
    """
    import pyarrow as pa

    schema = pa.schema([("ts", pa.timestamp("us", tz="UTC")), ("value", pa.int64()), ("label", pa.string())])
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([1715000000000000, 1715000001000000], type=pa.timestamp("us", tz="UTC")),
            pa.array([10, 20]),
            pa.array(["x", None]),
        ],
        schema=schema,
    )
    return BatchStream(iter([batch]), schema)


def test_all_terminals_equivalent() -> None:
    """All four terminals produce data-equivalent output from one stream definition.

    Each output is normalized to a ``pa.Table`` (pandas NaN -> Arrow None via
    ``pa.Table.from_pandas``,; DuckDB via
    ``rel.to_arrow_table`` NOT ``.fetchall``, ), then
    compared with ``pa.Table.equals`` (Arrow ``__eq__`` handles nulls
    correctly).
    """
    import pyarrow as pa

    arrow_table = _normalize_string_types(to_arrow(_make_stream()))
    pandas_table = _normalize_string_types(pa.Table.from_pandas(to_pandas(_make_stream())))
    polars_table = _normalize_string_types(to_polars(_make_stream()).to_arrow())
    duckdb_table = _normalize_string_types(to_duckdb(_make_stream()).to_arrow_table())

    assert arrow_table.equals(pandas_table), "arrow != pandas (null-representation divergence?)"
    assert arrow_table.equals(polars_table), "arrow != polars"
    assert arrow_table.equals(duckdb_table), "arrow != duckdb (local-tz divergence?)"


def test_equivalence_preserves_null_count() -> None:
    """The null count in the 'name' column is identical (1) across all 4 terminals.

    Confirms the null is neither dropped nor doubled by any terminal
    ( — pandas NaN must round-trip to exactly one Arrow
    null, not zero or two).
    """
    import pyarrow as pa

    arrow_nulls = _normalize_string_types(to_arrow(_make_stream())).column("name").null_count
    pandas_nulls = _normalize_string_types(pa.Table.from_pandas(to_pandas(_make_stream()))).column("name").null_count
    polars_nulls = _normalize_string_types(to_polars(_make_stream()).to_arrow()).column("name").null_count
    duckdb_nulls = _normalize_string_types(to_duckdb(_make_stream()).to_arrow_table()).column("name").null_count

    assert arrow_nulls == pandas_nulls == polars_nulls == duckdb_nulls == 1


def test_equivalence_with_timestamp_column() -> None:
    """A tz-aware timestamp column round-trips equivalently across all 4 terminals.

    This catches the DuckDB local-tz pitfall: if the
    comparison used ``.fetchall()`` datetime objects, the local-tz shift
    would break equality. Comparing at the Arrow level (``rel.to_arrow_table()``)
    normalizes it. The null in ``label[1]`` is also preserved.
    """
    import pyarrow as pa

    arrow_table = _normalize_string_types(to_arrow(_make_timestamp_stream()))
    pandas_table = _normalize_string_types(pa.Table.from_pandas(to_pandas(_make_timestamp_stream())))
    polars_table = _normalize_string_types(to_polars(_make_timestamp_stream()).to_arrow())
    duckdb_table = _normalize_string_types(to_duckdb(_make_timestamp_stream()).to_arrow_table())

    assert arrow_table.equals(pandas_table), "arrow != pandas (timestamp)"
    assert arrow_table.equals(polars_table), "arrow != polars (timestamp)"
    assert arrow_table.equals(duckdb_table), "arrow != duckdb (local-tz divergence)"


def test_to_arrow_preserves_specific_values() -> None:
    """to_arrow preserves exact cell values, not just shape."""
    table = to_arrow(_make_stream())
    assert table.column("id").to_pylist() == [1, 2]
    assert table.column("name").to_pylist() == ["a", None]


def test_to_arrow_multi_batch_stream() -> None:
    """A stream that yields multiple batches is flattened correctly by to_arrow."""
    import pyarrow as pa

    from datasluice.data.batch_stream import BatchStream

    schema = pa.schema([("id", pa.int64())])
    b1 = pa.RecordBatch.from_arrays([pa.array([1, 2])], schema=schema)
    b2 = pa.RecordBatch.from_arrays([pa.array([3, 4])], schema=schema)
    stream = BatchStream(iter([b1, b2]), schema)
    table = to_arrow(stream)
    assert table.num_rows == 4
    assert table.column("id").to_pylist() == [1, 2, 3, 4]
