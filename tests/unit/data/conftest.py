"""Shared fixtures for data-plane unit tests.

Provides synthetic row generators and a
small pa.Schema helper for test convenience. The HTTP streaming server
fixture lands in 04-03 which extends ``tests/helpers/http_server.py``.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    import pyarrow as pa


@pytest.fixture
def synthetic_csv_rows() -> Iterator[str]:
    """Yield rows of CSV-formatted text for streaming/memory tests.

    Returns a generator of ``N`` rows (default 100). Each row is
    ``id,name,value\\n``. Deterministic and in-process — no fixture files.
    """

    def _generate(n: int = 100) -> Iterator[str]:
        for i in range(n):
            yield f"{i},item_{i},{i * 1.5:.1f}\n"

    return _generate()


@pytest.fixture
def synthetic_json_rows() -> Iterator[str]:
    """Yield JSONL lines for streaming/memory tests.

    Returns a generator of ``N`` lines (default 100). Each line is a JSON
    object ``{"id": i, "name": "item_i", "value": i * 1.5}``. Deterministic
    and in-process — no fixture files.
    """

    import json

    def _generate(n: int = 100) -> Iterator[str]:
        for i in range(n):
            yield json.dumps({"id": i, "name": f"item_{i}", "value": round(i * 1.5, 1)}) + "\n"

    return _generate()


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
