<p align="center">
  <img src="docs/assets/datasluice.png" alt="DataSluice" width="600">
</p>

<p align="center">
  One Python interface for open-data discovery, extraction, format normalization, and pipeline integration
</p>

<p align="center">
  <a href="https://pypi.org/project/datasluice/"><img src="https://img.shields.io/pypi/v/datasluice.svg" alt="PyPI version"></a>
  <a href="https://github.com/nitish-raj/datasluice/actions"><img src="https://github.com/nitish-raj/datasluice/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://nitish-raj.github.io/datasluice/"><img src="https://img.shields.io/badge/docs-online-blue" alt="Documentation"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-green" alt="License"></a>
</p>

---

* [GitHub](https://github.com/nitish-raj/datasluice/) | [PyPI](https://pypi.org/project/datasluice/) | [Documentation](https://nitish-raj.github.io/datasluice/)
* Created by [Nitish Raj](https://rajnitish.com/) | GitHub [@nitish-raj](https://github.com/nitish-raj) | PyPI [@nitish-raj](https://pypi.org/user/nitish-raj/)
* MIT License

## Installation

```bash
pip install datasluice
```

Optional extras for format and integration support:

```bash
pip install "datasluice[pandas,polars,parquet,xlsx]"
pip install "datasluice[all]"          # everything
```

## Quick Start

```python
from datasluice import DataSluice

ds = DataSluice()
results = ds.search("climate", portal="data.gouv")
for dataset in results:
    print(dataset.title, dataset.resources)
```

CLI:

```bash
datasluice search "climate" --portal data.gouv
datasluice detect https://demo.ckan.org
datasluice download <resource-url> --format csv
```

## Features

* **Unified API** — one interface for CKAN, data.gouv.fr, Socrata, and custom portals
* **Auto-detection** — point at a URL and DataSluice figures out the portal type
* **Format normalization** — CSV, JSON, XLSX, Parquet, and GeoJSON readers
* **Integrations** — pandas, Polars, dlt, DuckDB, and Apache Airflow
* **CLI** — search, inspect, download, and detect from the command line
* **Pipeline-ready** — retry, rate-limiting, caching, and checksum verification built in

## Documentation

Documentation is built with [Zensical](https://zensical.org/) and deployed to GitHub Pages.

* **Live site:** https://datasluice.rajnitish.com
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
