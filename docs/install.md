# Installation

## Requirements

- Python 3.12 or later

## Install

```bash
pip install datasluice
```

Or with [`uv`](https://docs.astral.sh/uv/):

```bash
uv add datasluice
```

## Optional dependencies

DataSluice keeps its core dependency footprint small. Some features require
optional packages:

| Feature          | Install command                    |
|------------------|------------------------------------|
| pandas support   | `pip install datasluice[pandas]`   |
| Polars support   | `pip install datasluice[polars]`   |
| dlt integration  | `pip install datasluice[dlt]`      |
| DuckDB support   | `pip install datasluice[duckdb]`   |
| Airflow operator | `pip install datasluice[airflow]`  |
| Parquet reading  | `pip install datasluice[parquet]`  |
| XLSX reading     | `pip install datasluice[xlsx]`     |
| All extras       | `pip install datasluice[all]`      |

## Verify installation

```bash
datasluice --version
```

## Development install

See the [README](https://github.com/nitish-raj/datasluice) for local
development setup instructions.
