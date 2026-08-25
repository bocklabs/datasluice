<p align="center">
  <img src="docs/assets/datasluice.png" alt="DataSluice" width="600">
</p>

<p align="center">
  A contract-driven Python SDK for public-data catalog platforms, plus a direct-resource data plane for extraction and format normalization
</p>

<p align="center">
  <a href="https://pypi.org/project/datasluice/"><img src="https://img.shields.io/pypi/v/datasluice.svg" alt="PyPI version"></a>
  <a href="https://github.com/bocklabs/datasluice/actions"><img src="https://github.com/bocklabs/datasluice/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://bocklabs.github.io/datasluice/"><img src="https://img.shields.io/badge/docs-online-blue" alt="Documentation"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-green" alt="License"></a>
</p>

> ⚠️ **Unstable — under active development.**
> DataSluice is pre-1.0 and evolving fast. Breaking changes may occur at any time without notice. Use it at your own risk.

---

* [GitHub](https://github.com/bocklabs/datasluice/) | [PyPI](https://pypi.org/project/datasluice/) | [Documentation](https://bocklabs.github.io/datasluice/)
* Created by [Nitish Raj](https://rajnitish.com/) | GitHub [@nitish-raj](https://github.com/nitish-raj) | PyPI [@nitish-raj](https://pypi.org/user/nitish-raj/)
* MIT License

## Installation

```bash
pip install datasluice
```

Optional extras cover format readers and pipeline integrations:

```bash
pip install "datasluice[pandas,polars,parquet,xlsx]"
pip install "datasluice[all]"          # every supported optional extra
```

The base installation carries the shared catalog contracts, models,
capability profiles, reference fakes, and compliance runner. Named connector
extras belong to the Phase 2 packaging boundary and are not advertised here.

### Apache Airflow

Airflow integration is a separate distribution that reserves the
`airflow.providers.datasluice` namespace:

```bash
pip install apache-airflow-providers-datasluice
```

The provider's `DatasluiceHook` builds a live CKAN client from an Airflow
connection with explicit `base_url` and `api_token` extras. uData and Socrata
connections retain the deferred typed runtime until their live executors ship.

## Quick Start

### Direct-resource data plane

Convert a supported local file, URL, or object-storage resource to a portable
Parquet artifact in one command:

```bash
pip install "datasluice[parquet]"
datasluice materialize ./source.csv --destination ./converted --mode parquet --output json
datasluice materialize https://example.org/data.json --destination ./converted --mode parquet --output json
```

`./converted` is an output directory. DataSluice writes a content-addressed
Parquet file there and returns its URI and checksums. CSV, JSON, JSONL,
GeoJSON, XLSX, and Parquet inputs use the same command.

The equivalent Python operation is one `materialize` call:

```python
from datasluice import DataSluice, DirectResourceLocator

with DataSluice() as ds:
    artifact = ds.materialize(
        DirectResourceLocator(uri="https://example.org/data.csv"),
        "./converted",
        mode="parquet",
    )
    print(artifact.content_digest, artifact.uri)
```

Use `mode="raw"` when you want a checksummed byte-for-byte copy instead of a
conversion. For in-memory destinations, the same source can become a pandas
DataFrame, Polars DataFrame, DuckDB relation, or Arrow table through
`ds.open(locator).to_pandas()`, `.to_polars()`, `.to_duckdb()`, or `.to_arrow()`.

### Live CKAN catalog client

CKAN 2.11.5 (Action API v3) is available through a typed, context-managed
sync or async client. Pass the deployment origin explicitly:

```python
from datasluice.connectors.catalog.ckan import CKANClientSettings, create_sync_client

settings = CKANClientSettings(base_url="https://catalog.example.gov")

if globals().get("__name__") == "__main__":
    with create_sync_client(settings) as client:
        result = client.datasets.package_search(q="climate", rows=5)
        print(result.items)
```

The client exposes normalized dataset/resource/organization projections and
complete typed native service groups. It applies operation-level capability
guards, explicit credential handling, retries and time budgets, and redacted
mutation receipts. Mutating operations require an explicit safety policy.

### Connector contracts and upcoming platforms

Each platform has an explicit package; the shared catalog namespace never
re-exports platform APIs:

```python
from datasluice.connectors.catalog.ckan import CKANClientSettings, create_sync_client
from datasluice.connectors.catalog.socrata import SocrataConnector, create_socrata_connector
from datasluice.connectors.catalog.udata import UDataConnector, create_udata_connector
```

uData and Socrata currently expose typed façades, pinned profiles, fixtures,
and contract tests through their factories. Their live endpoint clients are
not implemented yet. All connectors can be exercised against deterministic
reference fakes through the public compliance runner.

CLI:

```bash
datasluice --version
datasluice scan ./source.csv --output json
datasluice open ./source.csv --output jsonl
datasluice materialize ./source.csv --destination ./out.parquet --output json
```

## Features

* **Live CKAN 2.11.5 client** — typed Action API v3 service groups with sync/async parity, capability evidence, authenticated operations, mutation safeguards, and read-only drift checks
* **Typed connector contracts** — explicit platform packages and pinned profiles for CKAN, uData, and Socrata; uData and Socrata live clients are forthcoming
* **Sync and async parity** — separate context-managed client surfaces with independent lifecycles
* **Evidence-backed capabilities** — pinned versioned profiles distinguish core, optional, authenticated, and deployment-unavailable operations; guards fail before dispatch with typed remedies
* **Public compliance runner** — fixture-backed contract cases produce pytest results and a machine-readable compliance report for built-in and third-party connectors
* **Direct-resource data plane** — streaming readers for CSV, JSON, JSONL, XLSX, Parquet, and GeoJSON over a shared batch-stream contract
* **Integrations** — pandas, Polars, dlt, and DuckDB (optional extras); Apache Airflow with live CKAN hook composition
* **CLI** — scan, open, and materialize resources from the command line

## Documentation

Documentation is built with [Zensical](https://zensical.org/) and deployed to GitHub Pages.

* **Live site:** https://bocklabs.github.io/datasluice/
* **Preview locally:** `just docs-serve` (serves at http://localhost:8000)
* **Build:** `just docs-build`

API documentation is auto-generated from docstrings using [mkdocstrings](https://mkdocstrings.github.io/).

Docs deploy automatically on push to `main` via GitHub Actions. To enable this, go to your repo's Settings > Pages and set the source to **GitHub Actions**.

## Development

To set up for local development:

```bash
# Clone your fork
git clone git@github.com:your_username/datasluice.git
cd datasluice

# Install dependencies (including all optional deps for dev)
uv sync --all-extras

# Install just (task runner) — one-time setup
curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to .venv/bin

# Install in editable mode with live updates
uv tool install --editable .
```

This installs the CLI globally but with live updates - any changes you make to the source code are immediately available when you run `datasluice`.

Install pre-commit hooks:

```bash
uv run pre-commit install
```

Run tests:

```bash
uv run pytest
```

Run quality checks (format, lint, type check, test):

```bash
just qa
```

## Release Process

Releases are automated with [Release Please](https://github.com/googleapis/release-please). There is no manual version bumping or tagging.

1. Use [**Conventional Commits**](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`, …) — see [CONTRIBUTING.md](CONTRIBUTING.md) for the full list.
2. Release Please maintains a **release PR** on `main` that bumps the version and updates the changelog.
3. **Merge the release PR** → Release Please creates a Git tag and a **GitHub Release**.
4. The GitHub Release auto-triggers **publishing to [TestPyPI](https://test.pypi.org/project/datasluice/)**, then **waits for approval** before publishing to [PyPI](https://pypi.org/project/datasluice/).

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, conventions, and the release workflow. Please follow the [Code of Conduct](CODE_OF_CONDUCT.md).
