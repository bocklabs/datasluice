<p align="center">
  <img src="datasluice.png" alt="DataSluice" width="200">
</p>

<h1 align="center">DataSluice</h1>

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
