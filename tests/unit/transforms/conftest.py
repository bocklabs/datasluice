"""Shared fixtures for transform unit tests.

Provides a synthetic :class:`~datasluice.data.batch_stream.BatchStream` fixture
(no fixture files — D-P4-16). Mirrors ``tests/unit/data/conftest.py``. The
deliberate ``None`` in the ``name`` column surfaces null-handling divergence
for the later QUAL-10 work (RESEARCH Pitfall 1).
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("pyarrow")


@pytest.fixture
def synthetic_batch_stream() -> Any:
    """A small BatchStream with typed columns + a null (for null-handling tests)."""
    import pyarrow as pa

    from datasluice.data.batch_stream import BatchStream

    schema = pa.schema([("id", pa.int64()), ("name", pa.string()), ("value", pa.float64())])
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array(["a", "b", None]), pa.array([1.5, 2.5, 3.5])],
        schema=schema,
    )
    return BatchStream(iter([batch]), schema)
