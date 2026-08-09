"""SQL injection regression tests for the DuckDB integration.

plan 04-02 removed the ``resource_to_relation`` and ``query_resource``
read paths (rebuilds them over the shared BatchStream).
The regression boundary — :func:`_validate_table_name` — is preserved
as the standalone SQL-identifier guard that will protect whatever
relation-registering API ships. The tests below exercise only that
guard; the URL-injection tests for the removed read path are deleted.
"""

from __future__ import annotations

import pytest

from datasluice.integrations.duckdb import _validate_table_name

BAD_TABLE_NAMES = [
    "x; DROP TABLE v",
    "bad name",
    "1lead",
    'a"; SELECT',
    "dash-name",
    "",
]


@pytest.mark.parametrize("bad_name", BAD_TABLE_NAMES)
def test_table_name_injection_rejected(bad_name: str) -> None:
    with pytest.raises(ValueError):
        _validate_table_name(bad_name)


@pytest.mark.parametrize("good_name", ["resource", "my_table", "t1", "_under"])
def test_table_name_valid(good_name: str) -> None:
    assert _validate_table_name(good_name) == good_name
