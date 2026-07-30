# Technology Stack

**Analysis Date:** 2026-07-30

## Languages

**Primary:**
- Python 3.12+ — requires-python `>= 3.12` (`pyproject.toml:26`). All source under `src/datasluice/`. Tested on 3.12, 3.13, 3.14 matrix.

**No secondary languages.** The project is pure Python (PEP 561 typed — `src/datasluice/py.typed` committed).

## Runtime

**Environment:**
- Python >= 3.12 (CI matrix: `["3.12", "3.13", "3.14"]` in `.github/workflows/ci.yml:61`)

**Package Manager:**
- `uv` (Astral) — the only supported package manager; never `pip` directly (per `AGENTS.md`)
- Lockfile: `uv.lock` (present, committed)
- Default groups: `["dev"]` (`pyproject.toml:132`), so `uv sync` installs lint + test + typecheck + pre-commit

## Build System

**Backend:**
- Hatchling (`build-system.requires = ["hatchling"]`, `build-backend = "hatchling.build"` — `pyproject.toml:1-3`)

**Build commands:**
- `uv build` — builds wheel + sdist into `dist/`
- `uvx twine check dist/*` — validates distribution (CI `.github/workflows/ci.yml:128`)
- Justfile/Makefile `build` target: `rm -rf build dist && uv build`

## Frameworks

**Core (required dependencies):**
- Typer `0.26.7` — CLI application framework (`pyproject.toml:23`). Entry point `datasluice = "datasluice.cli.app:app"` (`pyproject.toml:82`)
- Rich `15.0.0` — terminal pretty-printing (`pyproject.toml:24`, used in `src/datasluice/cli/app.py:6`)

**Standard Library (default transport — zero-config):**
- `urllib.request` / `urllib.error` — default HTTP backend; `src/datasluice/transport/http_client.py` wraps urllib directly. No third-party dep required for basic operation.

**Documentation:**
- Zensical `0.0.45` — MkDocs Material wrapper (config: `zensical.toml`, NOT `mkdocs.yml`)
- mkdocstrings-python `2.0.5` — auto-generated API reference (`docs/api.md` uses `::: datasluice`)

**Build/Dev:**
- uv — dependency management, venv, running tools
- just (Justfile) / make (Makefile) — task runners with identical targets
- pre-commit `4.6.0` — git hooks (`.pre-commit-config.yaml`)

## Key Dependencies

**Required (`project.dependencies`):**
| Package | Lockfile Version | Why it matters |
|---------|-----------------|----------------|
| `typer` | 0.26.7 | CLI framework, the user-facing entry point |
| `rich` | 15.0.0 | Console output rendering |

The runtime dependency surface is intentionally minimal (2 packages). Everything else is optional, lazy-imported extras.

**Optional Dependencies (`[project.optional-dependencies]`, `pyproject.toml:50-73`):**

Each extra is a feature gate that unlocks a lazy-imported integration. Install with `pip install datasluice[<extra>]` or `uv sync --extra <extra>` / `--all-extras`:

| Extra | Package | Lockfile Version | Purpose / Location |
|-------|---------|-----------------|---------------------|
| `http` | `httpx` | 0.28.1 | Preferred HTTP transport (`>=0.27`) — `src/datasluice/transport/httpx_transport.py` |
| `storage` | `fsspec` | 2026.6.0 | Cloud/local filesystem abstraction (`>=2025.1`) — `src/datasluice/io/filesystem.py`, `src/datasluice/io/fsspec_storage.py` |
| `parquet` | `pyarrow` | 24.0.0 | Parquet read/write, Arrow substrate — `src/datasluice/data/readers/parquet.py`, `src/datasluice/integrations/arrow.py` |
| `streaming` | `pyarrow` | 24.0.0 | Same as parquet (BatchStream materialization) |
| `xlsx` | `openpyxl` | 3.1.5 | Excel (.xlsx) reader — `src/datasluice/data/readers/xlsx.py` |
| `compression` | `zstandard` | 0.25.0 | Zstd decompression (`>=0.23`) — `src/datasluice/data/compression.py` |
| `pandas` | `pandas` | 3.0.3 | DataFrame terminal via Arrow zero-copy — `src/datasluice/integrations/pandas.py` |
| `polars` | `polars` | 1.41.2 | Polars DataFrame terminal — `src/datasluice/integrations/polars.py` |
| `duckdb` | `duckdb` | 1.5.4 | SQL relation registration — `src/datasluice/integrations/duckdb.py` |
| `dlt` | `dlt` | 1.28.1 | data load tool source adapter — `src/datasluice/integrations/dlt.py` |
| `airflow` | `apache-airflow` | 3.2.2 | Airflow operator factory — `src/datasluice/integrations/airflow.py` |
| `all` | (aggregate) | — | Installs everything except polars/pandas/dlt/duckdb/airflow-less combos; see `pyproject.toml:62-73` |

**Lazy import discipline (critical convention):** All optional deps are imported *inside function bodies*, never at module top-level. This keeps `import datasluice` working on a bare install with only typer + rich. See `src/datasluice/integrations/arrow.py:30`, `src/datasluice/transport/httpx_transport.py:124`, `src/datasluice/io/filesystem.py:48`. Each missing dep raises a helpful `ImportError` naming the extra to install.

## Dev Dependencies

**Dependency groups (`[dependency-groups]`, `pyproject.toml:28-48`):**

| Group | Packages | Purpose |
|-------|----------|---------|
| `lint` | `ruff` (0.15.18) | Format + lint |
| `test` | `pytest` (9.1.1), `coverage` (7.14.2) | Test runner + coverage |
| `typecheck` | `ty` (0.0.51) | Astral type checker |
| `docs` | `zensical` (0.0.45), `mkdocstrings-python` (2.0.5) | Documentation build |
| `dev` | aggregates lint + test + typecheck + `pre-commit` (4.6.0) | Default group |

## Configuration

**Environment:**
- No runtime env-var settings system. The `Settings` env-var dataclass was deliberately removed (D-14, CORR-04) — all configuration is explicit kwargs on `DataSluiceSession` (`src/datasluice/runtime/session.py:105`).
- `.env.example` exists documenting *optional* tuning vars (`DATASLUICE_HTTP_TIMEOUT`, `DATASLUICE_HTTP_RETRIES`, `DATASLUICE_API_KEY`, `DATASLUICE_CACHE_DIR`, `DATASLUICE_LOG_LEVEL`, `DATASLUICE_USER_AGENT`), but these are reference/documentation — the code itself does not read a `.env` file.
- `DATASLUICE_NO_REDACT=1` — escape hatch to disable log redaction (`src/datasluice/logging.py:57`).
- Default constants in `src/datasluice/config/defaults.py`: timeout `30.0s`, retries `3`, rate limit `10.0` req/s, page size `100`, cache dir `.datasluice/cache`, cache TTL `3600s`, log level `INFO`.

**Linting (`[tool.ruff]`, `pyproject.toml:94-105`):**
- Line length: 120
- Selected rule sets: `E` (pycodestyle errors), `W` (pycodestyle warnings), `F` (Pyflakes), `I` (isort), `B` (flake8-bugbear), `UP` (pyupgrade)
- Pre-commit hook: `ruff --fix` + `ruff-format` (`.pre-commit-config.yaml:17-23`, ruff-pre-commit rev `v0.13.2`)

**Type Checking (`[tool.ty]`, `pyproject.toml:89-92`):**
- `ty` (Astral) — all rules enabled as "error" by default
- **Must run with `--all-extras`**: `uv run --all-extras ty check .` so `ty` can resolve lazy imports of optional deps. Without it → `unresolved-import` errors.
- CI type-check job: `.github/workflows/ci.yml:53`

**Testing (`[tool.pytest.ini_options]`, `pyproject.toml:127-129`):**
- `testpaths = ["tests"]`
- `pythonpath = ["src", "."]`

**Coverage (`[tool.coverage]`, `pyproject.toml:107-125`):**
- Branch coverage enabled (`branch = true`), parallel mode (`parallel = true`)
- Source: `src/`, `tests/`
- Threshold: `fail_under = 50`
- Excludes: `TYPE_CHECKING` blocks, `@overload`, `Protocol` classes, `@abstractmethod`, `NotImplementedError`, `...`
- `show_missing = true`, `skip_covered = true`

**Pre-commit (`.pre-commit-config.yaml`):**
- pre-commit-hooks v5.0.0 (trailing-whitespace, end-of-file-fixer, check-yaml/toml, large-files 2048kb, merge-conflict, case-conflict, debug-statements, mixed-line-ending=lf, check-ast, check-docstring-first)
- ruff-pre-commit v0.13.2 (ruff --fix, ruff-format)
- **Local hooks** (run via `uv run`, not bare): `ty check .` and `pytest -q`
- Local hooks MUST be invoked as `uv run pre-commit` (per `AGENTS.md`)

**Editor config (`.editorconfig`):**
- Python: UTF-8, LF, 4-space indent, trim trailing whitespace, final newline
- HTML/CSS/JS/JSON/YAML: 2-space indent

**Docs (`zensical.toml`):**
- Site: DataSluice, URL `https://nitish-raj.github.io/datasluice/`
- mkdocstrings python handler paths: `["src"]`
- Light/dark palette toggle

## CI/CD & Release

**CI Pipeline (`.github/workflows/ci.yml`):**
6 jobs gated by an `all-checks-pass` aggregate:
1. **lint** — `ruff format --check` + `ruff check`
2. **type-check** — `uv run --all-extras ty check .`
3. **test** — `coverage run -m pytest` on Python 3.12/3.13/3.14 matrix
4. **coverage** — combines results, publishes report to `$GITHUB_STEP_SUMMARY`
5. **build** — `uv build` + `twine check` + upload dist artifact
6. **smoke-test** — installs built wheel in fresh venv, imports `datasluice`, prints `__version__`

**Security workflows:**
- `.github/workflows/codeql.yml` — CodeQL analysis (Python `security-extended` + GitHub Actions)
- `.github/workflows/zizmor.yml` — workflow security analysis (zizmor on `.github/`)

**Release (Release Please — automated, no manual tagging):**
- Config: `release-please-config.json`, manifest: `.release-please-manifest.json` (current `0.1.0`)
- Release type: `python`, conventional commits required (`feat:`, `fix:`, etc.)
- Bumps `version` in `pyproject.toml` + updates `CHANGELOG.md` via a release PR
- Workflow: `.github/workflows/release-please.yml`
- Version exposed at runtime via `importlib.metadata` in `src/datasluice/_version.py` (single source of truth = pyproject; no second copy to sync). `_version.py` is a separate module to break a circular import with `transport/user_agent.py`.

**Publishing (`.github/workflows/publish.yml`):**
Triggered on `release: published`:
1. Build + validate + attest build provenance + upload artifact
2. Publish to **TestPyPI** (env `test-pypi`, secret `TEST_PYPI_API_KEY`)
3. Publish to **PyPI** (env `pypi`, secret `PYPI_API_KEY`) — waits for TestPyPI + manual approval

**Docs deployment (`.github/workflows/docs.yml`):**
- On push to `main`: `zensical build --clean` → deploy to GitHub Pages

**Dependabot (`.github/dependabot.yml`):**
- GitHub Actions (weekly) + pip (weekly), 7-day cooldown, max 5 open PRs, minor/patch grouped

## Platform Requirements

**Development:**
- Python 3.12, 3.13, or 3.14
- `uv` installed
- `just` (optional, Rust binary) or `make` as fallback — both provide identical targets
- `--all-extras` required for type checking and full local dev: `uv sync --all-extras`

**Production:**
- Distributed via **PyPI** (`datasluice` package). Consumers install `pip install datasluice[<extras>]`.
- Zero-dependency runtime beyond typer+rich for bare operation (urllib-based transport, no cloud, no data libs).
- Linux/macOS/Windows; CI runs `ubuntu-latest`. EditorConfig sets LF line endings (CRLF only for `*.bat`).

---

*Stack analysis: 2026-07-30*
