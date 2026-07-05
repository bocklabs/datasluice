# Technology Stack

**Analysis Date:** 2026-07-05

## Languages

**Primary:**
- Python 3.12+ — `requires-python = ">= 3.12"` declared in `pyproject.toml`. All source under `src/datasluice/`. CI matrix runs Python 3.12, 3.13, and 3.14 (`.github/workflows/ci.yml`).

**Secondary:**
- Markdown — documentation source under `docs/` (rendered by Zensical).
- YAML — `.github/workflows/*.yml`, `.pre-commit-config.yaml`, `zensical.toml`.
- Justfile / Makefile — developer task definitions (`justfile`, `Makefile`).

## Runtime

**Environment:**
- Python 3.12 minimum. PEP 695 type parameters (`def func[T](...)`) and `from __future__ import annotations` are used throughout — e.g. `src/datasluice/transport/retry.py:34`.

**Package Manager:**
- `uv` (Astral). Lockfile `uv.lock` present (committed). Per `AGENTS.md`, `uv` is the **only** allowed installer — never call `pip` directly.
- Install everything (dev + optional deps): `uv sync --all-extras`. `--all-extras` is **required** for `ty` type checking because heavy optional deps (pandas, polars, dlt, duckdb, pyarrow, openpyxl, airflow) are lazy-imported.

## Frameworks

**Core:**
- [Typer] 0.26.7 — CLI application framework (declared in `pyproject.toml` `[project] dependencies`). Entry point `datasluice.cli.app:app` (`src/datasluice/cli/app.py`).
- [Rich] 15.0.0 — Terminal rendering (tables, panels, console) used by all CLI commands (`src/datasluice/cli/*.py`).

**Testing:**
- [pytest] 9.1.1 — test runner. Config in `pyproject.toml` `[tool.pytest.ini_options]` (`testpaths = ["tests"]`).
- [coverage] 7.14.2 — coverage with branch + parallel mode. Threshold `fail_under = 50`.

**Build/Dev:**
- [Hatchling] — PEP 517 build backend (`[build-system] requires = ["hatchling.build"]`).
- [ruff] 0.15.18 — formatter + linter (`E, W, F, I, B, UP` selected, line-length 120).
- [ty] 0.0.51 — Astral type checker. All rules error by default (`[tool.ty]` in `pyproject.toml`).
- [pre-commit] 4.6.0 — git hooks. Config in `.pre-commit-config.yaml` (includes local hooks for `ty check` and `pytest`).
- [just] — task runner (Rust binary, installed into `.venv/bin`). `Makefile` is a zero-dependency fallback with identical targets.

**Docs:**
- [Zensical] 0.0.45 — MkDocs Material wrapper. Config in `zensical.toml` (NOT `mkdocs.yml`).
- [mkdocstrings-python] 2.0.5 — auto-generates API reference from docstrings (`docs/api.md` uses `::: datasluice` directive).

## Key Dependencies

**Critical (runtime, always installed — `pyproject.toml` `[project] dependencies`):**
- `typer` 0.26.7 — CLI framework; required for `datasluice` console script.
- `rich` 15.0.0 — Pretty terminal output for search/inspect/download/detect commands.

**Optional extras (`pyproject.toml` `[project.optional-dependencies]`):**
- `pandas` (3.0.3) — `datasluice[pandas]`, used by `src/datasluice/integrations/pandas.py`.
- `polars` (1.41.2) — `datasluice[polars]`, used by `src/datasluice/integrations/polars.py`.
- `dlt` (1.28.1) — `datasluice[dlt]`, used by `src/datasluice/integrations/dlt.py`.
- `duckdb` (1.5.4) — `datasluice[duckdb]`, used by `src/datasluice/integrations/duckdb.py`.
- `apache-airflow` (3.2.2) — `datasluice[airflow]`, used by `src/datasluice/integrations/airflow.py`.
- `pyarrow` (24.0.0) — `datasluice[parquet]`, used by `src/datasluice/formats/parquet.py`.
- `openpyxl` (3.1.5) — `datasluice[xlsx]`, used by `src/datasluice/formats/xlsx.py`.
- `all` — convenience meta-extra installing every optional dep.

**Important:** Every optional dep is **lazy-imported inside the function body**, never at module top-level. This keeps the base install minimal (only typer + rich) and is enforced by `ty` resolving the lazy imports via `--all-extras`.

**Notable absence:** The project uses Python's **standard library `urllib`** (`urllib.request`, `urllib.parse`, `urllib.error`) for all HTTP I/O — see `src/datasluice/transport/http_client.py`. There is **no `requests`, `httpx`, or `aiohttp` dependency**.

## Configuration

**Environment:**
- Loaded from `os.environ` in `src/datasluice/config/settings.py` via the `Settings` dataclass and `load_settings()`. Defaults in `src/datasluice/config/defaults.py`.
- `.env.example` documents the supported variables (do NOT read `.env` contents).
- Supported env vars (all prefixed `DATASLUICE_`):
  - `DATASLUICE_HTTP_TIMEOUT` (default `30.0`)
  - `DATASLUICE_HTTP_RETRIES` (default `3`)
  - `DATASLUICE_HTTP_RATE_LIMIT` (default `10.0` requests/sec)
  - `DATASLUICE_PAGE_SIZE` (default `100`)
  - `DATASLUICE_CACHE_DIR` (default `.datasluice/cache`)
  - `DATASLUICE_CACHE_TTL` (default `3600` seconds)
  - `DATASLUICE_LOG_LEVEL` (default `INFO`)
  - `DATASLUICE_API_KEY` (optional portal API key)
  - `DATASLUICE_BEARER_TOKEN` (optional bearer token)
  - `DATASLUICE_USER_AGENT` (optional UA override; auto-built by `src/datasluice/transport/user_agent.py`)

**Build / Tooling Config Files:**
- `pyproject.toml` — PEP 621 metadata, build backend, ruff, ty, coverage, pytest, uv config.
- `.editorconfig` — UTF-8 / LF / 4-space indent (2-space for yaml/json/css).
- `.pre-commit-config.yaml` — pre-commit hooks (whitespace, ruff, ty, pytest, check-ast).
- `zensical.toml` — docs site config (palette, nav, mkdocstrings python handler `paths = ["src"]`).
- `justfile` / `Makefile` — task runners with identical targets (`qa`, `test`, `coverage`, `build`, `docs-serve`, `publish`).
- `CNAME` — custom docs domain (`datasluice.rajnitish.com`).

## Platform Requirements

**Development:**
- Python ≥ 3.12, `uv`, optionally `just` (installed into `.venv/bin`).
- Run `uv sync --all-extras` then `uv run pre-commit install`.

**Production:**
- Distributed as a library on [PyPI](https://pypi.org/project/datasluice/) (`name = "datasluice"`, `version = "0.1.0"`).
- Installed via `pip install datasluice` (or any optional extra). Console script `datasluice` registered at `datasluice = "datasluice.cli.app:app"`.
- No server, daemon, or database required — pure client library plus CLI.
- Docs hosted on GitHub Pages at https://nitish-raj.github.io/datasluice/ (and custom domain https://datasluice.rajnitish.com).

---

*Stack analysis: 2026-07-05*
