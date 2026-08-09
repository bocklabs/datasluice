"""Optional integrations with the broader Python data ecosystem.

Each sub-module imports its heavy dependency lazily, so importing this
package does not require pandas, dlt, DuckDB, or Airflow to be installed.

removed the v0.1.0 ``polars`` integration:
rebuilt all terminal integrations over the shared
:class:`datasluice.data.BatchStream`.

removed the provisional ``airflow`` integration: the sole
Airflow surface is now the standalone ``apache-airflow-providers-datasluice``
distribution.
"""

__all__ = [
    "pandas",
    "polars",
    "arrow",
    "dlt",
    "duckdb",
]
