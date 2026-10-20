from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    import pyarrow as pa


@pytest.fixture
def pa_schema_helper() -> pa.Schema:
    """Build a small pa.Schema for test convenience."""
    pa = pytest.importorskip("pyarrow")
    return pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
            ("value", pa.float64()),
        ]
    )
