"""Unit tests for TransformContext (TRANS-09, D-P6-08).

Verifies the context is frozen (``FrozenInstanceError`` on mutation), defaults
are ``None`` for the optional provenance fields, the field set is exactly the
locked four, and all fields round-trip through attribute access.
"""

from __future__ import annotations

import dataclasses

import pytest

pytest.importorskip("pyarrow")

from datasluice.transforms import TransformContext  # noqa: E402


def test_context_is_frozen() -> None:
    """Assigning to ctx.arrow_schema raises dataclasses.FrozenInstanceError."""
    import pyarrow as pa

    ctx = TransformContext(arrow_schema=pa.schema([("id", pa.int64())]))
    with pytest.raises(dataclasses.FrozenInstanceError):
        ctx.arrow_schema = pa.schema([])  # type: ignore[misc]


def test_context_defaults_are_none() -> None:
    """TransformContext(arrow_schema=s) has all optional provenance fields None."""
    import pyarrow as pa

    ctx = TransformContext(arrow_schema=pa.schema([("id", pa.int64())]))
    assert ctx.source_resource_id is None
    assert ctx.source_url is None
    assert ctx.domain_schema is None


def test_context_field_set() -> None:
    """The field set is exactly the locked four (D-P6-08)."""
    names = {f.name for f in dataclasses.fields(TransformContext)}
    assert names == {"arrow_schema", "source_resource_id", "source_url", "domain_schema"}


def test_context_accepts_all_fields() -> None:
    """Constructing with all four fields round-trips through attribute access."""
    import pyarrow as pa

    schema = pa.schema([("id", pa.int64())])
    ctx = TransformContext(
        arrow_schema=schema,
        source_resource_id="res-123",
        source_url="https://example.org/data.csv",
        domain_schema={"name": "advisory"},
    )
    assert ctx.arrow_schema == schema
    assert ctx.source_resource_id == "res-123"
    assert ctx.source_url == "https://example.org/data.csv"
    assert ctx.domain_schema == {"name": "advisory"}
