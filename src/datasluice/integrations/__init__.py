"""Optional integrations with the broader Python data ecosystem.

Each sub-module imports its heavy dependency lazily, so importing this
package does not require pandas, Polars, dlt, DuckDB, or Airflow to be
installed.
"""

__all__ = [
    "pandas",
    "polars",
    "dlt",
    "airflow",
    "duckdb",
]
