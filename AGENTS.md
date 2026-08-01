# AGENTS.md

High-signal guidance for working in this repo. Read this before making changes.

## Package manager

Use `uv` exclusively — never call `pip` directly.

```bash
uv sync --all-extras   # install everything (dev + all optional deps)
uv run <command>       # run anything in the project venv
```

`--all-extras` is **required** for type checking and running pre-commit locally, because `ty` resolves lazy imports of optional deps (pandas, polars, dlt, duckdb, pyarrow, openpyxl, airflow). Without it you get `unresolved-import` errors.

`just` (task runner) is a Rust binary, not a pip package. Install it into the venv:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to .venv/bin
```
If `just` is missing, `make` works as a zero-dependency fallback (same targets).

## Developer commands

| Task | Command |
|------|---------|
| Full QA (format → lint → typecheck → test) | `just qa` or `make qa` |
| Format only | `uv run ruff format .` |
| Lint only | `uv run ruff check . --fix` |
| Type check | `uv run --all-extras ty check .` |
| Tests | `uv run pytest` |
| Single test file | `uv run pytest tests/unit/domain/test_models.py` |
| Pre-commit (all files) | `uv run pre-commit run --all-files` |
| Install pre-commit hooks | `uv run pre-commit install` |
| Serve docs | `just docs-serve` |
| Build dist | `uv build` |

`just qa` and `make qa` run the same pipeline: ruff format → ruff lint → ty check → pytest.

## Pre-commit

Pre-commit includes **local hooks** for `ty check` and `pytest` (see `.pre-commit-config.yaml`). These run via `uv run`, so they need the project venv. Always invoke pre-commit as `uv run pre-commit`, not bare `pre-commit`.

## Architecture

- **Entry point**: `datasluice.cli.app:app` (Typer app). Not `datasluice.cli:app`.
- **Version**: source of truth is `version` in `pyproject.toml` (bumped by **Release Please** via `release-please-config.json`); `datasluice/_version.py` exposes it at runtime via `importlib.metadata` (no second copy to sync). `_version.py` stays a separate module to break a circular import with `transport/user_agent.py` — do NOT move it into `__init__.py`.
- **Adapters auto-register**: importing `datasluice.adapters` triggers side-effect registration of all built-in adapters (CKAN, data.gouv, Socrata, custom) into the module-level `registry`.
- **Adapter pattern**: each adapter subpackage has `adapter.py`, `mapper.py`, `pagination.py`, `errors.py`. Mappers translate portal-native JSON into `datasluice.domain` models.
- **Lazy imports**: `formats/` and `integrations/` import heavy optional deps (pyarrow, openpyxl, pandas, etc.) inside functions, not at module top-level. Keep it that way.

## Style conventions

- **Line length**: 120 (ruff).
- **Ruff selects**: E, W, F, I, B, UP (pyupgrade).
- **PEP 695 type params**: use `def func[T](...)` syntax, not `TypeVar`. The project targets Python 3.12+.
- **Typer commands**: use `Annotated[str, typer.Option(...)]` pattern, not `param: str = typer.Option(...)`. The B008 rule (flake8-bugbear) rejects function calls in argument defaults.
- **No comments** in code unless explicitly requested.
- **Docstrings**: Google style. First line is a summary.

## Docs

Built with **Zensical** (MkDocs Material wrapper). Config is `zensical.toml`, not `mkdocs.yml`. Logo lives at `docs/assets/datasluice.png`. API docs auto-generated via mkdocstrings — the `docs/api.md` file uses `::: datasluice` directive.

## CI

GitHub Actions (`.github/workflows/ci.yml`):
- Runs on pull requests and push to `main`.
- **type-check job** uses `uv run --all-extras ty check .` — if you add new optional deps, the CI must install them.
- Tests run on Python 3.12, 3.13, and 3.14 matrix.
- **build** job builds the dist, runs `twine check`, and uploads the artifact.
- **smoke-test** job installs the built wheel in a fresh venv and imports `datasluice`.
- Coverage threshold: 50% (`fail_under` in pyproject.toml).

## Release

Automated by **Release Please** (`.github/workflows/release-please.yml`). Commits **must** use [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`, …). On push to `main`, Release Please maintains a release PR that bumps `version` in `pyproject.toml` and updates `CHANGELOG.md`. Merging that PR tags the release and creates a GitHub Release.

Publishing (`.github/workflows/publish.yml`) triggers on `release: published`:
1. **Build** — builds, validates (`twine check`), attests provenance, uploads the artifact.
2. **TestPyPI** (`test-pypi` env, secret `TEST_PYPI_API_KEY`) — publishes automatically.
3. **PyPI** (`pypi` env, secret `PYPI_API_KEY`) — `needs` TestPyPI to pass, then **waits for approval** (required reviewers on the `pypi` environment) before publishing.

- **No manual tagging.** Config lives in `release-please-config.json` + `.release-please-manifest.json`.
