# Codebase Structure

**Analysis Date:** 2026-07-26

## Directory Layout

```
datasluice/
├── src/datasluice/            # All production source (single package)
│   ├── __init__.py            # Public API re-exports
│   ├── _version.py            # Version from importlib.metadata (standalone — breaks circular import)
│   ├── exceptions.py          # Rooted exception hierarchy
│   ├── logging.py             # get_logger() / configure_logging()
│   ├── py.typed               # PEP 561 marker
│   ├── cli/                   # Typer CLI (entry surface)
│   ├── runtime/               # Composition root + plugin manager
│   ├── ports/                 # Protocol boundary contracts
│   ├── connectors/            # Portal adapters (one subpackage per platform)
│   ├── domain/                # Portal-agnostic frozen dataclass models
│   ├── transport/             # HTTP clients + retry/rate-limit/redirect
│   ├── auth/                  # Authentication strategies
│   ├── discovery/             # Portal-type auto-detection
│   ├── io/                    # Download, cache, storage, checksums
│   ├── formats/               # Format readers (lazy optional deps)
│   ├── integrations/          # pandas/polars/dlt/duckdb/airflow bridges
│   ├── credentials/           # Host-scoped credential providers
│   └── config/                # DEFAULT_* constants
├── tests/                     # Mirror of src layout (unit/ + integration/ + fixtures/)
├── docs/                      # Zensical (MkDocs) site source
├── site/                      # Built docs / examples (architecture, adapters, api)
├── notebook/                  # Exploration notebooks + sample data
├── graphify-out/              # Generated knowledge graph (do not edit by hand)
├── .planning/                 # GSD planning artifacts
├── .github/workflows/         # CI: ci.yml, publish.yml, release-please.yml
├── pyproject.toml             # Build, deps, ruff, ty, pytest, entry points
├── justfile / Makefile        # `just qa` / `make qa` = format→lint→typecheck→test
├── zensical.toml              # Docs build config (NOT mkdocs.yml)
└── uv.lock                    # Lockfile (uv)
```

## Directory Purposes

**`src/datasluice/cli/`:**
- Purpose: Thin Typer command handlers; one module per subcommand.
- Contains: `app.py` (the `app` Typer object + callback), `search.py`, `inspect.py`, `download.py`, `detect.py`.
- Key files: `src/datasluice/cli/app.py` (assembles the app), `src/datasluice/cli/search.py`, `src/datasluice/cli/download.py`.

**`src/datasluice/runtime/`:**
- Purpose: Composition root and plugin machinery.
- Contains: `session.py` (`DataSluiceSession` facade), `plugin_manager.py` (`PluginManager` entry-point loader), `context.py` (`ConnectorContext`), `defaults.py` (`create_default_transport`).
- Key files: `src/datasluice/runtime/session.py`, `src/datasluice/runtime/plugin_manager.py`.

**`src/datasluice/ports/`:**
- Purpose: `@runtime_checkable` Protocol contracts that adapters satisfy structurally.
- Contains: `transport.py`, `catalog.py`, `storage.py`, `cache.py`, `credentials.py`, `detector.py`, `resource_reader.py`, `state_store.py`.
- Key files: `src/datasluice/ports/transport.py`, `src/datasluice/ports/catalog.py`, `src/datasluice/ports/storage.py`.

**`src/datasluice/connectors/`:**
- Purpose: One subpackage per supported portal platform.
- Contains: `base.py` (`BaseAdapter` ABC) + subpackages `ckan/`, `socrata/`, `datagouv/`, `custom/`.
- Each portal subpackage has exactly: `adapter.py`, `mapper.py`, `pagination.py`, `factory.py`, `errors.py`, `__init__.py`.
- Key files: `src/datasluice/connectors/base.py`, `src/datasluice/connectors/ckan/adapter.py`, `src/datasluice/connectors/ckan/factory.py`, `src/datasluice/connectors/socrata/adapter.py`.

**`src/datasluice/domain/`:**
- Purpose: Portal-agnostic frozen dataclass models — the shared vocabulary.
- Contains: `dataset.py`, `resource.py`, `organization.py`, `license.py`, `query.py`, `result.py`, `schema.py`, `artifact.py`, `access.py`, `capabilities.py`, `credentials.py`, `detection.py`, `sync_state.py`.
- Key files: `src/datasluice/domain/dataset.py`, `src/datasluice/domain/resource.py`, `src/datasluice/domain/query.py`, `src/datasluice/domain/result.py`.

**`src/datasluice/transport/`:**
- Purpose: HTTP execution, cross-cutting policies, optional streaming.
- Contains: `http_client.py` (urllib, default fallback), `httpx_transport.py` (lazy, primary when `http` extra installed), `retry.py`, `rate_limit.py`, `redirect.py`, `pagination.py`, `user_agent.py`.
- Key files: `src/datasluice/transport/http_client.py`, `src/datasluice/transport/httpx_transport.py`, `src/datasluice/transport/__init__.py` (PEP 562 lazy `HttpxTransport`/`StreamResponse`).

**`src/datasluice/auth/`:**
- Purpose: Pluggable request-authentication strategies.
- Contains: `base.py` (`BaseAuth` ABC), `none.py`, `api_key.py`, `bearer.py`, `basic.py`, `headers.py`.
- Key files: `src/datasluice/auth/base.py`, `src/datasluice/auth/none.py`.

**`src/datasluice/discovery/`:**
- Purpose: Auto-detect a portal's platform from its URL.
- Contains: `detector.py` (`detect_portal_type`), `fingerprints.py` (`PATH_FINGERPRINTS`/`HTML_FINGERPRINTS`), `portal_metadata.py`.
- Key files: `src/datasluice/discovery/detector.py`, `src/datasluice/discovery/fingerprints.py`.

**`src/datasluice/io/`:**
- Purpose: Resource download, byte caching, storage abstraction, checksums.
- Contains: `downloader.py`, `storage.py` (`Storage` ABC + `LocalStorage`), `cache.py` (`FileCache`), `content_cache.py` (lazy `ContentCache`), `fsspec_storage.py` (lazy `FsspecStorage`), `filesystem.py` (lazy `open_filesystem`), `checksums.py`, `local.py`.
- Key files: `src/datasluice/io/downloader.py`, `src/datasluice/io/storage.py`, `src/datasluice/io/__init__.py` (PEP 562 lazy symbols).

**`src/datasluice/formats/`:**
- Purpose: Read a file/bytes blob into `list[dict]` rows.
- Contains: `base.py` (`BaseFormatReader` ABC), `csv.py`, `json.py`, `parquet.py`, `xlsx.py`, `geojson.py`; `__init__.py` holds the `READERS` registry and `get_reader()`.
- Key files: `src/datasluice/formats/base.py`, `src/datasluice/formats/__init__.py`, `src/datasluice/formats/parquet.py`.

**`src/datasluice/integrations/`:**
- Purpose: Bridge into external ecosystems; each function lazily imports its optional dep.
- Contains: `pandas.py`, `polars.py`, `dlt.py`, `duckdb.py`, `airflow.py`.
- Key files: `src/datasluice/integrations/pandas.py`, `src/datasluice/integrations/dlt.py`.

**`src/datasluice/credentials/`:**
- Purpose: Host-scoped credential providers for dynamic auth refresh.
- Contains: `host_provider.py` (`HostCredentialProvider`).

**`src/datasluice/config/`:**
- Purpose: Default scalar knobs as plain module-level constants.
- Contains: `defaults.py` (`DEFAULT_TIMEOUT`, `DEFAULT_RETRIES`, `DEFAULT_RATE_LIMIT`, `DEFAULT_PAGE_SIZE`, `DEFAULT_CACHE_DIR`, `DEFAULT_CACHE_TTL`, `DEFAULT_LOG_LEVEL`).

## Key File Locations

**Entry Points:**
- `src/datasluice/cli/app.py`: Typer `app` (console script target via `pyproject.toml`).
- `src/datasluice/__init__.py`: Library public API (re-exports `DataSluiceSession`, domain models, exceptions).
- `src/datasluice/runtime/session.py`: `DataSluiceSession` — programmatic entry facade.

**Configuration:**
- `pyproject.toml`: build (hatchling), deps, ruff (line=120, E/W/F/I/B/UP), `ty` typecheck, pytest, coverage (`fail_under=50`), `[project.scripts]`, `[project.entry-points."datasluice.connectors"]`.
- `justfile` / `Makefile`: `qa` target = ruff format → ruff check → ty check → pytest.
- `.pre-commit-config.yaml`: local hooks for `ty check` and `pytest` (run via `uv run pre-commit`).
- `src/datasluice/config/defaults.py`: runtime default constants.
- `zensical.toml`: docs build config (NOT `mkdocs.yml`).
- `release-please-config.json` + `.release-please-manifest.json`: automated release config.

**Core Logic:**
- `src/datasluice/runtime/session.py`: composition root.
- `src/datasluice/runtime/plugin_manager.py`: entry-point discovery.
- `src/datasluice/connectors/base.py`: adapter ABC.
- `src/datasluice/transport/httpx_transport.py`: primary HTTP backend.
- `src/datasluice/discovery/detector.py`: portal fingerprinting.

**Testing:**
- `tests/conftest.py`: shared fixtures.
- `tests/helpers/http_server.py`: in-process test HTTP server.
- `tests/unit/`: unit tests mirroring `src/` layout (e.g. `tests/unit/connectors/test_ckan_mapper.py`).
- `tests/integration/`: live/integration tests per connector (`ckan/`, `socrata/`, `datagouv/`).
- `tests/fixtures/`: golden JSON payloads per portal.

## Naming Conventions

**Files:**
- `snake_case.py` throughout.
- Per-portal connector subpackage files are fixed: always `adapter.py`, `mapper.py`, `pagination.py`, `factory.py`, `errors.py`, `__init__.py`.
- ABCs live in `base.py`; Protocols live in `ports/` (one concept per file).
- Test files: `test_<module>.py` (e.g. `test_http_client.py`, `test_ckan_mapper.py`).

**Directories:**
- `src/datasluice/<layer>/` for each architectural layer.
- `src/datasluice/connectors/<portal>/` — one subpackage per platform, named by canonical `portal_type` (`ckan`, `socrata`, `datagouv`, `custom`).
- `tests/unit/<layer>/` mirrors `src/datasluice/<layer>/`.

**Classes:**
- Adapters: `<Portal>Adapter` (e.g. `CKANAdapter`, `SocrataAdapter`, `CustomAdapter`).
- Transports: `<Backend>Transport` (e.g. `HttpxTransport`); `HttpClient` (urllib, legacy name).
- Strategies: `<Scheme>Auth` (e.g. `BearerAuth`, `APIKeyAuth`, `BasicAuth`, `HeadersAuth`, `NoAuth`).
- Readers: `<FORMAT>Reader` (e.g. `CSVReader`, `ParquetReader`).
- Pagination: `<Portal>Page` (e.g. `CKANPage`, `SocrataPage`).
- Ports: `<Concept>Port` (e.g. `StoragePort`, `CachePort`, `CatalogPort`); or bare concept name (`Transport`, `CredentialProvider`).
- Factories: `create_<portal>_connector(ctx)` — the entry-point callable.

**ClassVar:**
- Every adapter exposes `portal_type: ClassVar[str]` (e.g. `"ckan"`), matching its entry-point name.

**Constants:**
- `UPPER_SNAKE_CASE` in `src/datasluice/config/defaults.py` and `src/datasluice/discovery/fingerprints.py`.

## Where to Add New Code

**New portal connector (the canonical 6-file recipe):**
1. Create `src/datasluice/connectors/<portal>/` with: `__init__.py` (re-export `<Portal>Adapter`), `adapter.py` (subclass `BaseAdapter`, set `portal_type`), `mapper.py` (portal JSON → `datasluice.domain`), `pagination.py` (`<Portal>Page` dataclass), `factory.py` (`create_<portal>_connector(ctx)`), `errors.py`.
2. Register the entry point in `pyproject.toml` under `[project.entry-points."datasluice.connectors"]`: `<portal> = "datasluice.connectors.<portal>.factory:create_<portal>_connector"`.
3. Optionally add detection fingerprints in `src/datasluice/discovery/fingerprints.py`.
4. Tests: `tests/unit/connectors/test_<portal>_mapper.py` + `tests/integration/<portal>/`; fixtures in `tests/fixtures/<portal>/`.
- Reference implementation: `src/datasluice/connectors/ckan/`.

**New CLI command:**
1. Add `src/datasluice/cli/<command>.py` defining a function `def <command>(...)`.
2. Register it in `src/datasluice/cli/app.py`: `app.command(name="<command>")(command)`.
3. Lazy-import `DataSluiceSession` **inside** the function body (matches existing commands).
4. Test: `tests/unit/cli/test_<command>.py`.

**New domain model:**
1. Add `src/datasluice/domain/<model>.py` as a `@dataclass(frozen=True)`.
2. Export it from `src/datasluice/domain/__init__.py` (`__all__` + import).
3. If part of the public API, also re-export from `src/datasluice/__init__.py`.
4. Test: `tests/unit/domain/test_models.py` or `test_<model>.py`.

**New port / Protocol:**
1. Add `src/datasluice/ports/<concept>.py` with a `@runtime_checkable class <Concept>(Protocol)`.
2. Export from `src/datasluice/ports/__init__.py`.
3. Probe capabilities with `isinstance(obj, <Concept>)` rather than backend-specific type checks.

**New transport backend:**
1. Add `src/datasluice/transport/<backend>_transport.py` satisfying the `Transport` Protocol (and `StreamingTransport` if it streams).
2. If it pulls an optional dep, resolve it lazily via PEP 562 `__getattr__` in `src/datasluice/transport/__init__.py` (mirror how `HttpxTransport` is exported).
3. Wire selection into `src/datasluice/runtime/defaults.py:create_default_transport` if it should be a default candidate.

**New auth strategy:**
1. Add `src/datasluice/auth/<scheme>.py` subclassing `BaseAuth`.
2. Export from `src/datasluice/auth/__init__.py`.

**New format reader:**
1. Add `src/datasluice/formats/<format>.py` subclassing `BaseFormatReader`; import the optional dep **inside** `read()`.
2. Register in the `READERS` dict in `src/datasluice/formats/__init__.py`.
3. Add the format alias to `src/datasluice/domain/resource.py:_FORMAT_ALIASES` if a media type should normalize to it.

**New integration (pandas/polars/dlt/duckdb/airflow/...):**
1. Add `src/datasluice/integrations/<lib>.py`; import the optional dep inside the public function and raise `ImportError` with an install hint when absent.
2. Add a corresponding optional-dependency group in `pyproject.toml` `[project.optional-dependencies]` and include it in `all`.

**Shared utility:**
- Pure helpers with no domain meaning: the closest layer's module (e.g. `src/datasluice/io/local.py` for filesystem helpers). There is no top-level `utils.py`.

**Tests:**
- Co-locate unit tests under `tests/unit/<layer>/test_<module>.py`.
- Integration tests: `tests/integration/<portal>/`.
- Golden fixtures: `tests/fixtures/<portal>/`.
- Shared test infra: `tests/helpers/` (e.g. `http_server.py`).

## Special Directories

**`graphify-out/`:**
- Purpose: Generated knowledge graph (god nodes, communities, cross-file relationships) consumed by the `graphify` skill.
- Generated: Yes — produced by `graphify update .` (AST-only, no API cost).
- Committed: Yes (per AGENTS.md; dirty files after hooks are expected and not a reason to skip graphify).
- Contains: `cache/ast/`, `cache/semantic/`, plus the graph + wiki artifacts.

**`.planning/`:**
- Purpose: GSD workflow artifacts (PROJECT.md, ROADMAP.md, REQUIREMENTS.md, STATE.md, `phases/`, `codebase/`, `research/`).
- Generated: Maintained by GSD skills, not auto-generated.
- Committed: Yes.

**`site/`:**
- Purpose: Built documentation site (Zensical output) plus hand-curated `examples/`, `architecture/`, `adapters/`, `supported-portals/`, `api/`.
- Generated: Partially (API docs via mkdocstrings); examples are curated.
- Committed: Yes.

**`notebook/`:**
- Purpose: Exploration notebooks and sample data (`notebook/data/`).
- Generated: No.
- Committed: Yes.

**`src/datasluice/__pycache__/` and `tests/**/__pycache__/`:**
- Purpose: Python bytecode cache.
- Generated: Yes.
- Committed: No (gitignored).

**`.venv/`:**
- Purpose: Project virtualenv managed by `uv`.
- Generated: Yes (`uv sync --all-extras`).
- Committed: No.

---

*Structure analysis: 2026-07-26*
