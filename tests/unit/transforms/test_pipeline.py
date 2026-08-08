"""Unit tests for the Pipeline + compose runner (TRANS-08, D-P6-04/05/06).

Covers: identity round-trip, left-to-right step threading (no intermediate
materialization), Pitfall 7 (output schema matches transformed batches, not
stale input), empty-stream handling, introspectable ``.steps``, and
``__repr__``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

pytest.importorskip("pyarrow")

from datasluice.data.batch_stream import BatchStream  # noqa: E402
from datasluice.transforms import Pipeline, compose  # noqa: E402

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from datasluice.transforms.protocol import TransformContext


class _RenameValueToAmount:
    """Rename the ``value`` column to ``amount`` (proves step output feeds the next)."""

    def apply(self, batches: Iterable[Any], context: TransformContext) -> Iterator[Any]:
        for batch in batches:
            new_names = [n if n != "value" else "amount" for n in batch.schema.names]
            yield batch.rename_columns(new_names)


class _SelectAmount:
    """Select only ``amount`` — only present once _RenameValueToAmount has run."""

    def apply(self, batches: Iterable[Any], context: TransformContext) -> Iterator[Any]:
        for batch in batches:
            yield batch.select(["amount"])


class _DropName:
    """Drop the ``name`` column — exercises the schema-change path (Pitfall 7)."""

    def apply(self, batches: Iterable[Any], context: TransformContext) -> Iterator[Any]:
        for batch in batches:
            yield batch.select(["id"])


def test_compose_empty_round_trips_schema(synthetic_batch_stream: Any) -> None:
    """compose([]) round-trips the schema and preserves row count."""
    out = compose([]).run(synthetic_batch_stream)
    assert out.schema == synthetic_batch_stream.schema
    rows = list(out.iter_batches())
    assert len(rows[0]) == 3


def test_compose_threads_steps_left_to_right() -> None:
    """step2 (_SelectAmount) sees step1's (_RenameValueToAmount) renamed output.

    The input has a ``value`` column, NOT ``amount`` — so _SelectAmount would
    fail if it ran first. Its success proves _RenameValueToAmount ran before it
    (left-to-right threading, no reordering).
    """
    import pyarrow as pa

    schema = pa.schema([("id", pa.int64()), ("name", pa.string()), ("value", pa.float64())])
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array(["a", "b", "c"]), pa.array([1.5, 2.5, 3.5])], schema=schema
    )
    stream = BatchStream(iter([batch]), schema)

    out = compose([_RenameValueToAmount(), _SelectAmount()]).run(stream)
    rows = list(out.iter_batches())
    # "amount" only exists after the rename; data carried through from "value"
    assert rows[0].schema.names == ["amount"]
    assert rows[0].column("amount").to_pylist() == [1.5, 2.5, 3.5]


def test_pipeline_output_schema_matches_transformed_batches() -> None:
    """Pitfall 7: output BatchStream.schema reflects the DROPPED schema, not input."""
    import pyarrow as pa

    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2]), pa.array(["a", "b"])], schema=schema)
    stream = BatchStream(iter([batch]), schema)

    out = compose([_DropName()]).run(stream)
    assert out.schema.names == ["id"]
    assert "name" not in out.schema.names
    rows = list(out.iter_batches())
    assert rows[0].schema.names == ["id"]


def test_pipeline_empty_stream_yields_empty_schema() -> None:
    """compose([]) over an empty stream yields a BatchStream with an empty schema."""
    import pyarrow as pa

    empty = BatchStream(iter(()), pa.schema([]))
    out = compose([]).run(empty)
    assert out.schema == pa.schema([])
    assert list(out.iter_batches()) == []


def test_pipeline_steps_introspectable() -> None:
    """Pipeline.steps is an introspectable list preserving order."""
    s1 = _DropName()
    s2 = _RenameValueToAmount()
    assert compose([s1, s2]).steps == [s1, s2]


def test_pipeline_repr_renders_steps() -> None:
    """Pipeline.__repr__ renders the step list."""
    s = _DropName()
    rendered = repr(Pipeline([s]))
    assert "Pipeline(" in rendered
    assert repr(s) in rendered
