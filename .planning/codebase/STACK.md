# Technology Stack

**Analysis Date:** 2026-07-26

## Languages

**Primary:**
- Python `>= 3.12` (declared in `pyproject.toml` `requires-python`). CI matrix targets Python 3.12, 3.13, and 3.14 (`.github/workflows/ci.yml`).

**Secondary:**
- YAML / TOML for configuration (`pyproject.toml`, `zensical.toml`, `.pre-commit-config.yaml`).
- Markdown for documentation (`docs/`, `README.md`).
- Bash/shell snippets embedded in `justfile` and `Makefile` task targets.

## Runtime

**Environment:**
- CPython 3.12–3.14. No pinned interpreter file (`.python-version` / `.tool-versions` not present); the version is selected per-command via `uv run --python=3.1x` (see `justfile`, `Makefile`).

**Package Manager:**
- `uv` (Astral) — the only sanctioned installer. Never call `pip` directly (per `AGENTS.md`).
- Lockfile: `uv.lock` present and committed (~583 KB).
- `--all-extras` is required for type checking and pre-commit so `ty` can resolve lazy optional imports (`pandas`, `polars`, `dlt`, `duckdb`, `pyarrow`, `openpyxl`, `airflow`).

## Frameworks

**Core:**
- `typer` (CLI framework) — drives the `datasluice` command (`src/datasluice/cli/app.py`). Commands registered via `app.command(name=...)`.
- `rich` — console rendering (`src/datasluice/cli/app.py` uses `rich.console.Console`).
- Python standard library — the default HTTP transport is built on `urllib.request` / `urllib.parse` (`src/datasluice/transport/http_client.py`) and the content cache uses `sqlite3` in WAL mode (`src/datasluice/io/content_cache.py`).

**Testing:**
- `pytest` — runner. Config in `pyproject.toml` `[tool.pytest.ini_options]` (`testpaths = ["tests"]`, `pythonpath = ["src", "."]`).
- `coverage` — branch + parallel coverage. Config in `pyproject.toml` `[tool.coverage.*]`; threshold `fail_under = 50`.

**Type Checking:**
- `ty` (Astral) — the sole type checker, replacing mypy. Run as `uv run --all-extras ty check .` Config in `pyproject.toml` `[tool.ty]` (all rules error by default).

**Linting / Formatting:**
- `ruff` — formatter + linter. Config in `pyproject.toml` `[tool.ruff]` / `[tool.ruff.lint]` (`line-length = 120`; selects `E, W, F, I, B, UP`). Pinned to `v0.13.2` in `.pre-commit-config.yaml`.

**Build / Dev:**
- `hatchling` — PEP 517 build backend (`pyproject.toml` `[build-system]`).
- `pre-commit` — git hooks (`.pre-commit-config.yaml`). Includes local hooks that invoke `ty` and `pytest` via `uv run`.
- `just` — task runner (`justfile`); `make` as zero-dependency fallback (`Makefile`). Both expose identical `qa` → format, lint, typecheck, test.
- `zensical` + `mkdocstrings-python` — documentation build (MkDocs Material wrapper). Config in `zensical.toml` (NOT `mkdocs.yml`).

## Key Dependencies

**Critical (installed by default):**
- `typer` — CLI entry surface (`src/datasluice/cli/`).
- `rich` — terminal output.

**Optional extras (declared in `pyproject.toml` `[project.optional-dependencies]`):**
- `http` → `httpx>=0.27` — preferred HTTP transport; auto-selected when importable (`src/datasluice/runtime/defaults.py`).
- `storage` → `fsspec>=2025.1` — multi-backend filesystem (S3 / GCS / Azure / HTTP / memory / local).
- `pandas`, `polars` — DataFrame integration (`src/datasluice/integrations/pandas.py`, `polars.py`).
- `dlt` — data-load-tool source (`src/datasluice/integrations/dlt.py`).
- `duckdb` — in-process SQL over remote resources (`src/datasluice/integrations/duckdb.py`).
- `apache-airflow` — `DataSluiceOperator` (`src/datasluice/integrations/airflow.py`).
- `parquet` → `pyarrow` — Parquet reader (`src/datasluice/formats/parquet.py`).
- `xlsx` → `openpyxl` — XLSX reader (`src/datasluice/formats/xlsx.py`).
- `all` — convenience metagroup installing every extra above.

**Lazy-import discipline:** every optional heavy dependency is imported *inside* the function/method that needs it (never at module top level), so a bare `pip install datasluice` stays importable. See `src/datasluice/formats/parquet.py`, `src/datasluice/integrations/duckdb.py`, `src/datasluice/transport/httpx_transport.py`, `src/datasluice/io/filesystem.py`. Preserve this when adding new optional deps.

**Standard-library usage worth noting:**
- `urllib.request` / `urllib.parse` — default transport backend.
- `sqlite3` (WAL mode) — content cache metadata index (`src/datasluice/io/content_cache.py`).
- `importlib.metadata` entry_points — connector plugin discovery (`src/datasluice/runtime/plugin_manager.py`).
- `importlib.util.find_spec` — httpx availability probe without eager import (`src/datasluice/runtime/defaults.py`).
- `threading.Lock` — per-host single-flight credential refresh (`src/datasluice/credentials/host_provider.py`).
- `hashlib` — SHA-256 cache keys and download checksums.

## Configuration

**Environment:**
- No env-var settings system. The legacy `Settings` dataclass was removed (D-14, CORR-04). All configuration flows through explicit `DataSluiceSession(...)` kwargs (`src/datasluice/runtime/session.py`).
- `.env.example` is present — contains environment variable templates for the CI/external-LLM secrets (existence noted; contents not read).
- Default runtime constants live in `src/datasluice/config/defaults.py`: `DEFAULT_TIMEOUT=30.0`, `DEFAULT_RETRIES=3`, `DEFAULT_RATE_LIMIT=10.0`, `DEFAULT_PAGE_SIZE=100`, `DEFAULT_CACHE_DIR=".datasluice/cache"`, `DEFAULT_CACHE_TTL=3600`, `DEFAULT_LOG_LEVEL="INFO"`.
- `DATASLUICE_NO_REDACT=1` env var disables log redaction (`src/datasluice/logging.py`).

**Build / Tool config files:**
- `pyproject.toml` — single source of truth: project metadata, deps, ruff, ty, pytest, coverage config.
- `uv.lock` — locked dependency resolution.
- `.pre-commit-config.yaml` — hook definitions (pre-commit-hooks v5.0.0, ruff-pre-commit v0.13.2, local `ty` + `pytest` hooks).
- `release-please-config.json` + `.release-please-manifest.json` — automated release configuration.
- `zensical.toml` — documentation site config (MkDocs Material).
- `.editorconfig` — line-ending (LF), indent (4 spaces Python / 2 for YAML/JSON), charset (UTF-8).
- `CNAME` — GitHub Pages custom domain pointer.

**Versioning:**
- Single source of truth: `version = "0.1.0"` in `pyproject.toml`.
- Bumped automatically by **Release Please** (Conventional Commits).
- Exposed at runtime via `importlib.metadata` in `src/datasluice/_version.py` — a separate module (kept out of `__init__.py`) to break a circular import with `src/datasluice/transport/user_agent.py`. Do NOT inline it.

## Platform Requirements

**Development:**
- Python 3.12, 3.13, or 3.14.
- `uv` installed; run `uv sync --all-extras` to populate the venv.
- `just` (Rust binary) recommended for task running; install into the venv per `AGENTS.md` or fall back to `make`.
- OS: Linux/macOS for CI (`ubuntu-latest`); local dev assumed POSIX (content cache relies on POSIX-atomic `mv`).

**Production:**
- Published as a library + CLI to [PyPI](https://pypi.org/project/datasluice/) (and [TestPyPI](https://test.pypi.org/project/datasluice/)).
- Pure-Python wheel; no compiled extensions. Runs anywhere CPython 3.12+ runs.
- Docs hosted on GitHub Pages at `https://nitish-raj.github.io/datasluice/` (`CNAME`, `.github/workflows/docs.yml`).

## CI / Release Pipeline

| Pipeline | File | Purpose |
|----------|------|---------|
| CI | `.github/workflows/ci.yml` | lint → type-check → test matrix (3.12/3.13/3.14) → coverage → build → smoke-test (install wheel, `import datasluice`) → all-checks-pass gate |
| Docs | `.github/workflows/docs.yml` | build zensical docs → deploy to GitHub Pages on push to `main` |
| Release Please | `.github/workflows/release-please.yml` | maintains release PR, bumps version + changelog, tags releases |
| Publish | `.github/workflows/publish.yml` | on GitHub Release: build → attest provenance → TestPyPI → (await approval) → PyPI |
| CodeQL | `.github/workflows/codeql.yml` | GitHub code scanning for `python` + `actions` (weekly + on PR) |
| Zizmor | `.github/workflows/zizmor.yml` | workflow security analysis on `.github/workflows/**` |
| PR-Agent | `.github/workflows/pr-agent.yml` | auto-generated PR descriptions via external LLM |
| OpenCodeReview | `.github/workflows/ocr-review.yml` | AI code review on PRs via external LLM |

---

*Stack analysis: 2026-07-26*
