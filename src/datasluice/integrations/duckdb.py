"""DuckDB integration.

The v0.1.0 ``resource_to_relation`` and ``query_resource`` helpers relied on
DuckDB's Python relation API to bind a resource URL as a virtual table.
rebuilds them over the shared :class:`datasluice.data.BatchStream`,
registering batches as DuckDB tables via Arrow zero-copy interop.

The standalone :func:`_validate_table_name` SQL-identifier guard is
preserved because it is the regression boundary
and continues to protect whatever relation-registering API ships.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datasluice.data.batch_stream import BatchStream

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


def to_duckdb(
    stream: BatchStream,
    *,
    table_name: str = "datasluice",
    conn: Any = None,
) -> Any:
    """Register *stream* as a named DuckDB relation.

    No SQL string interpolation — the boundary is preserved by
    construction: ``_validate_table_name`` rejects injection payloads before
    registration, and the relation API (``conn.register`` + ``conn.table``)
    never interpolates the name into a SQL string. DuckDB streams lazily;
    consumption is deferred to fetch.

    Args:
        stream: The :class:`~datasluice.data.BatchStream` to register.
        table_name: The DuckDB relation name (must match
            ``^[A-Za-z_][A-Za-z0-9_]*$``). Defaults to ``"datasluice"``.
        conn: An existing DuckDB connection to reuse. When ``None``, a fresh
            in-memory connection is created via ``duckdb.connect()``.

    Returns:
        A ``duckdb.DuckDBPyRelation`` bound to *table_name* on *conn*.

    Raises:
        ValueError: If *table_name* fails the identifier regex.
        ImportError: If ``duckdb`` is not installed.
    """
    try:
        import duckdb
    except ImportError as exc:
        raise ImportError("to_duckdb requires 'duckdb'. Install with: pip install datasluice[duckdb]") from exc

    from datasluice.integrations.arrow import to_arrow

    _validate_table_name(table_name)
    owns_connection = conn is None
    connection = conn if conn is not None else duckdb.connect()
    try:
        table = to_arrow(stream)
        connection.register(table_name, table)
        return connection.table(table_name)
    except BaseException:
        if owns_connection:
            connection.close()
        raise
