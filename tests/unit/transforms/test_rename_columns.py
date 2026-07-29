"""Unit tests for the RenameColumns transform (TRANS-03, D-P6-11).

Covers renaming, the idempotent self-rename, and the actionable missing-source
error (naming the missing source AND the available columns).
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("pyarrow")

from datasluice.transforms.protocol import TransformContext  # noqa: E402
from datasluice.transforms.steps import RenameColumns  # noqa: E402


def _ctx(schema: Any) -> TransformContext:
    return TransformContext(arrow_schema=schema)


def test_rename_changes_names() -> None:
    """A mapped source is renamed to its target; the source name is gone."""
    import pyarrow as pa

    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2]), pa.array(["a", "b"])], schema=schema)
    out = list(RenameColumns({"id": "identifier"}).apply(iter([batch]), _ctx(schema)))
    names = out[0].schema.names
    assert "identifier" in names
    assert "id" not in names
    assert "name" in names


def test_rename_idempotent() -> None:
    """Renaming a column to its own name is a no-op."""
    import pyarrow as pa

    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2]), pa.array(["a", "b"])], schema=schema)
    out = list(RenameColumns({"id": "id"}).apply(iter([batch]), _ctx(schema)))
    assert out[0].schema.names == ["id", "name"]


def test_rename_missing_source_raises() -> None:
    """A missing source column raises TransformError naming it and the available ones."""
    import pyarrow as pa

    from datasluice.exceptions import TransformError

    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    with pytest.raises(TransformError) as exc_info:
        list(RenameColumns({"nonexistent": "x"}).apply(iter([]), _ctx(schema)))
    msg = str(exc_info.value)
    assert "nonexistent" in msg
    assert "id" in msg and "name" in msg
