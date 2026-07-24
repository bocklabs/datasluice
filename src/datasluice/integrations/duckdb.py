"""DuckDB integration: query resources directly with DuckDB.

Requires ``duckdb``: install with ``pip install datasluice[duckdb]``.
"""

from __future__ import annotations

import re
from typing import Any

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


def resource_to_relation(
    resource_url: str,
    connection: Any = None,
    *,
    table_name: str = "resource",
) -> Any:
    """Register a remote resource as a DuckDB relation.

    The URL flows through DuckDB's Python relation API
    (``from_csv_auto``/``read_parquet``/``read_json``) so it reaches the
    C-level table-function bind phase as a filename and never enters the SQL
    parser as text, making SQL injection structurally impossible.

    Args:
        resource_url: URL of the resource to read.
        connection: Existing DuckDB connection (a new one is created if omitted).
        table_name: Name to give the virtual table; validated against an
            identifier regex before use.

    Returns:
        A DuckDB connection with the relation registered as a view.
    """
    try:
        import duckdb
    except ImportError as exc:
        raise ImportError("DuckDB integration requires 'duckdb'. Install with: pip install datasluice[duckdb]") from exc

    _validate_table_name(table_name)
    con = connection or duckdb.connect()
    lowered = resource_url.lower()
    if lowered.endswith(".csv"):
        relation = con.from_csv_auto(resource_url)
    elif lowered.endswith(".parquet"):
        relation = con.read_parquet(resource_url)
    elif lowered.endswith(".json"):
        relation = con.read_json(resource_url)
    else:
        raise ValueError(f"Unsupported resource format for DuckDB: {resource_url}")
    relation.create_view(table_name, replace=True)
    return con


def query_resource(resource_url: str, sql: str, connection: Any = None) -> Any:
    """Run arbitrary *sql* against a resource and return the result.

    Warning:
        ``sql`` is executed verbatim. The caller owns its safety; this is an
        intentionally opt-in raw-SQL passthrough for power users. Prefer building
        queries through the relation returned by :func:`resource_to_relation`
        when the SQL is not fully trusted.

    Args:
        resource_url: URL of the resource to read.
        sql: Arbitrary SQL to execute; safety is owned by the caller.
        connection: Existing DuckDB connection (a new one is created if omitted).

    Returns:
        The query result rows.
    """
    con = resource_to_relation(resource_url, connection)
    return con.execute(sql).fetchall()
