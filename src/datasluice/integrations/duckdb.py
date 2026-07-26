"""DuckDB integration (read paths removed per D-P4-18; SEC-03 utility preserved).

The v0.1.0 ``resource_to_relation`` and ``query_resource`` helpers relied on
DuckDB's Python relation API to bind a resource URL as a virtual table.
Phase 6 rebuilds them over the shared :class:`datasluice.data.BatchStream`,
registering batches as DuckDB tables via Arrow zero-copy interop.

The standalone :func:`_validate_table_name` SQL-identifier guard is
preserved because it is the SEC-03 regression boundary (Phase 1 QUAL-07)
and continues to protect whatever relation-registering API Phase 6 ships.
"""

from __future__ import annotations

import re

_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_table_name(table_name: str) -> str:
    """Validate that *table_name* is a safe SQL identifier.

    Args:
        table_name: The table name to validate.

    Returns:
        The table name if it matches the safe-identifier regex.

    Raises:
        ValueError: If the name contains metacharacters.
    """
    if not _TABLE_NAME_RE.match(table_name):
        raise ValueError(f"Invalid table name: {table_name!r}")
    return table_name
