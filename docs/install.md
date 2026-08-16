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

DataSluice keeps its core dependency footprint small. Some features
require optional packages:

| Feature          | Install command                    |
|------------------|------------------------------------|
| pandas support   | `pip install datasluice[pandas]`   |
| Polars support   | `pip install datasluice[polars]`   |
| dlt integration  | `pip install datasluice[dlt]`      |
| DuckDB support   | `pip install datasluice[duckdb]`   |
| Apache Airflow provider | `pip install apache-airflow-providers-datasluice` |
| Parquet reading  | `pip install datasluice[parquet]`  |
| XLSX reading     | `pip install datasluice[xlsx]`     |
| All extras       | `pip install datasluice[all]`      |

Airflow integration ships as a **separate distribution** called
`apache-airflow-providers-datasluice` and imports from the
`airflow.providers.datasluice` namespace. It is released and versioned
independently of the core `datasluice` package.

## Connector extras

The base installation already carries the full connector contract
surface: typed models, sync and async client Protocols, capability
profiles, reference fakes, and the public compliance runner.

Installable named connector extras (`ckan`, `udata`, `socrata`, and the
`all-connectors` convenience extra) belong to Phase 2 packaging work
(PACK-01/PACK-02) and their dependency tables. This release does not add
or advertise them, and the library never downloads dependencies at
runtime — missing capabilities explain the required install instead.

## Verify installation

```bash
datasluice --version
```

## Development install

See the [README](https://github.com/nitish-raj/datasluice) for local
development setup instructions.
