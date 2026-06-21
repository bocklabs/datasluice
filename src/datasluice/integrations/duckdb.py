"""DuckDB integration: query resources directly with DuckDB.

Requires ``duckdb``: install with ``pip install datasluice[duckdb]``.
"""

from __future__ import annotations

from typing import Any


def resource_to_relation(
    resource_url: str,
    connection: Any = None,
    *,
    table_name: str = "resource",
) -> Any:
    """Register a remote resource as a DuckDB relation.

    Args:
        resource_url: URL of the resource to read.
        connection: Existing DuckDB connection (a new one is created if omitted).
        table_name: Name to give the virtual table.

    Returns:
        A DuckDB connection with the table registered.
    """
    try:
        import duckdb
    except ImportError as exc:
        raise ImportError("DuckDB integration requires 'duckdb'. Install with: pip install datasluice[duckdb]") from exc

    con = connection or duckdb.connect()
    if resource_url.lower().endswith(".csv"):
        con.execute(f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_csv_auto('{resource_url}')")
    elif resource_url.lower().endswith(".parquet"):
        con.execute(f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{resource_url}')")
    elif resource_url.lower().endswith(".json"):
        con.execute(f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_json_auto('{resource_url}')")
    else:
        raise ValueError(f"Unsupported resource format for DuckDB: {resource_url}")
    return con


def query_resource(resource_url: str, sql: str, connection: Any = None) -> Any:
    """Run *sql* against a resource and return the result."""
    con = resource_to_relation(resource_url, connection)
    return con.execute(sql).fetchall()
