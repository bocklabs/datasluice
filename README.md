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
pip install "datasluice[all]"          # every optional integration
```

The base installation carries the full connector contract surface: typed
models, sync and async client Protocols, capability profiles, reference
fakes, and the public compliance runner. Installable named connector
extras (`ckan`, `udata`, `socrata`, `all-connectors`) are planned for
Phase 2 packaging work and are **not** advertised or installable from this
release.

### Apache Airflow

Airflow integration is a separate distribution that reserves the
`airflow.providers.datasluice` namespace:

```bash
pip install apache-airflow-providers-datasluice
```

The provider currently ships package metadata only. Hooks and operators
arrive with the live platform executors of later phases.

## Quick Start

### Direct-resource data plane

Resolve, stream, and materialize individual resources without any catalog
connector:

```python
from datasluice import DataSluice, DirectResourceLocator

with DataSluice() as ds:
    direct = DirectResourceLocator(uri="https://example.org/data.csv")
    resource = ds.resolve(direct)

    with ds.open(direct) as opened:
        for batch in opened:
            print(batch.num_rows)

    artifact = ds.materialize(direct, "out.parquet")
    print(artifact.content_digest, artifact.uri)
```

### Catalog connector contract

Connectors are explicit, typed, and factory-constructed. Each platform is
imported from its own package — the catalog namespace never re-exports
platform APIs:

```python
from datasluice.connectors.catalog.ckan import CKANConnector, create_ckan_connector
from datasluice.connectors.catalog.socrata import SocrataConnector, create_socrata_connector
from datasluice.connectors.catalog.udata import UDataConnector, create_udata_connector
```

Every factory accepts a `CatalogConnectorContext` carrying injected sync
and async executors, normalized and native service projections, and the
pinned effective capability profile. In Phase 1 those executors are
caller-supplied — deterministic reference fakes back the executable
contract suite:

```python
from datasluice.contracts.catalog import (
    CatalogContractCase,
    run_catalog_contract,
)
from datasluice.contracts.catalog.fakes import (
    AsyncReferenceConnector,
    SyncReferenceConnector,
)

report = run_catalog_contract(
    CatalogContractCase(operation_id="datasets.get", dataset_id="fixture-dataset"),
    sync_client=SyncReferenceConnector(),
    async_client=AsyncReferenceConnector(),
)
print([(outcome.mode, outcome.state) for outcome in report.outcomes])
```

Live CKAN, uData, and Socrata endpoint clients are implemented in Phases
3–5, after pinned capability profiles and controlled endpoint evidence
are recorded for each platform. Until then, connector façades accept
injected executors only; nothing in this release contacts a live
deployment.

CLI:

```bash
datasluice --version
datasluice scan ./source.csv --output json
datasluice open ./source.csv --output jsonl
datasluice materialize ./source.csv --destination ./out.parquet --output json
```

## Features

* **Typed connector contracts** — explicit platform packages for CKAN, uData, and Socrata with factory-constructed façades over normalized and native service Protocols
* **Sync and async parity** — separate context-managed clients with identical operation surfaces and independent lifecycles
* **Evidence-backed capabilities** — pinned versioned profiles distinguish core, optional, authenticated, and deployment-unavailable operations; guards fail before dispatch with typed remedies
* **Public compliance runner** — fixture-backed contract cases produce pytest results and a machine-readable compliance report for built-in and third-party connectors
* **Direct-resource data plane** — streaming readers for CSV, JSON, JSONL, XLSX, Parquet, and GeoJSON over a shared batch-stream contract
* **Integrations** — pandas, Polars, dlt, and DuckDB (optional extras); Apache Airflow (separate provider)
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
