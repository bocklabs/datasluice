# Codebase Structure

**Analysis Date:** 2026-07-30

## Directory Layout

```
datasluice/
├── src/datasluice/           # Library source (single top-level package)
│   ├── __init__.py           # Public API re-exports (DataSluiceSession, domain, errors)
│   ├── _version.py           # Version via importlib.metadata (separate to break circular import)
│   ├── exceptions.py         # Single exception hierarchy (DataSluiceError root)
│   ├── logging.py            # get_logger, RedactingFilter, SENSITIVE_HEADERS
│   ├── py.typed              # PEP 561 marker (shipped in wheel)
│   ├── auth/                 # BaseAuth ABC + 5 strategies
│   ├── cli/                  # Typer app + search/inspect/download/detect commands
│   ├── config/               # DEFAULT_* constants (no env-var system)
│   ├── connectors/           # Portal adapters: ckan/, datagouv/, socrata/ + base.py
│   ├── contracts/            # run_contract_suite conformance harness
│   ├── credentials/          # HostCredentialProvider (single-flight refresh)
│   ├── data/                 # Arrow data plane: BatchStream, readers, compression
│   ├── discovery/            # detect() + fingerprints
│   ├── domain/               # Portal-agnostic frozen dataclass models
│   ├── integrations/         # Terminal exports: pandas/polars/duckdb/dlt/airflow
│   ├── io/                   # Storage, cache, downloader, filesystem, checksums
│   ├── ports/                # runtime_checkable Protocol boundary contracts
│   ├── runtime/              # Composition root: session, plugin_manager, context
│   ├── sync/                 # Incremental sync: state stores, materialize
│   ├── transforms/           # TransformStep pipeline (closed normalization set)
│   └── transport/            # HttpClient (urllib) + HttpxTransport + retry/rate-limit
├── tests/                    # Pytest suite (mirrors src/ layout)
│   ├── conftest.py
│   ├── fixtures/<portal>/    # Hand-authored portal JSON for contract suite
│   ├── helpers/              # Shared test utilities
│   ├── unit/                 # Unit tests (mirror src/ package tree)
│   └── integration/<portal>/ # Integration tests per portal
├── docs/                     # Zensical (MkDocs Material) docs; assets/datasluice.png logo
├── notebook/                 # Exploration notebooks
├── graphify-out/             # Knowledge-graph artifacts (query/path/explain)
├── .github/workflows/        # ci.yml, release-please.yml, publish.yml
├── .planning/                # GSD state: PROJECT/REQUIREMENTS/ROADMAP, phases/, codebase/
├── pyproject.toml            # Build (hatchling), deps, entry-points, ruff/pytest/coverage config
├── zensical.toml             # Docs config (NOT mkdocs.yml)
├── justfile                  # `just qa` task runner
├── Makefile                  # `make qa` zero-dependency fallback
├── release-please-config.json
├── .release-please-manifest.json
└── AGENTS.md                 # High-signal contributor guide
```

## Directory Purposes

**`src/datasluice/domain/`:**
- Purpose: Portal-agnostic, dependency-free value objects (frozen dataclasses).
- Contains: One model per file — `dataset.py`, `resource.py`, `access.py`
  (ResourceAccess sum-type family), `organization.py`, `license.py`, `query.py`,
  `result.py`, `schema.py`, `capabilities.py`, `credentials.py`,
  `detection.py`, `sync_state.py`, `artifact.py`.
- Key files: `src/datasluice/domain/__init__.py` (re-export surface),
  `src/datasluice/domain/access.py` (5 access variants).

**`src/datasluice/ports/`:**
- Purpose: `runtime_checkable Protocol` boundary contracts — the ONLY types
  crossing the hexagon boundary.
- Contains: One Protocol per file — `transport.py` (Transport +
  StreamingTransport + ConditionalTransport + ConditionalFetchResult),
  `catalog.py` (CatalogPort + SearchableCatalog + OrganizationCatalog),
  `resource_reader.py`, `state_store.py`, `storage.py`, `cache.py`,
  `credentials.py`, `detector.py`.
- Key files: `src/datasluice/ports/transport.py`, `src/datasluice/ports/catalog.py`.

**`src/datasluice/runtime/`:**
- Purpose: Composition root — wires injected infra into a zero-config facade.
- Contains: `session.py` (`DataSluiceSession`), `context.py`
  (`ConnectorContext`), `plugin_manager.py` (`PluginManager`,
  `PluginFailure`), `defaults.py` (`create_default_transport`).
- Key files: `src/datasluice/runtime/session.py` (the public facade).

**`src/datasluice/connectors/`:**
- Purpose: Concrete portal adapters, one subpackage per platform.
- Contains: `base.py` (`BaseAdapter` ABC), `_reject.py` (pre-flight Query gate),
  and `ckan/`, `datagouv/`, `socrata/` subpackages.
- Each portal subpackage has the SAME file shape:
  - `adapter.py` — the `BaseAdapter` subclass (declares `capabilities` ClassVar).
  - `mapper.py` — pure JSON→dataclass mapping functions.
  - `pagination.py` — page-cursor dataclass (`to_params()`, `next_page()`).
  - `errors.py` — `map_*_error(status_code, body) -> PortalError`.
  - `factory.py` — `create_*_connector(ctx) -> Adapter` (entry-point target).
  - `__init__.py` — re-exports the adapter class.
- Key files: `src/datasluice/connectors/base.py`,
  `src/datasluice/connectors/_reject.py`,
  `src/datasluice/connectors/ckan/adapter.py`.

**`src/datasluice/transport/`:**
- Purpose: HTTP execution satisfying the Transport Protocols.
- Contains: `http_client.py` (urllib `HttpClient`, fallback),
  `httpx_transport.py` (httpx `HttpxTransport` + `StreamResponse`, default),
  `retry.py` (`RetryPolicy`, `with_retry`), `rate_limit.py` (`RateLimiter`),
  `redirect.py` (`CredentialAwareRedirectHandler`, `SENSITIVE_HEADERS`),
  `user_agent.py` (`build_user_agent`), `pagination.py`
  (`PaginationConfig`, `paginate`).
- Key files: `src/datasluice/transport/httpx_transport.py`,
  `src/datasluice/transport/http_client.py`.

**`src/datasluice/data/`:**
- Purpose: Arrow data plane — acquire bytes → decompress → decode →
  `RecordBatch` stream.
- Contains: `access.py` (`DataPlaneResourceReader` access-kind dispatch),
  `batch_stream.py` (`BatchStream`, `BatchCursor`, `ParquetRowGroupPosition`),
  `compression.py` (`apply_compression`, `PeekableReader`), `_byte_source.py`
  (`IterableBytesIO`), `schema.py` (`to_arrow_schema`), `readers/` subpackage.
- Key files: `src/datasluice/data/access.py`, `src/datasluice/data/batch_stream.py`.

**`src/datasluice/data/readers/`:**
- Purpose: Format-specific decoders, one file per format.
- Contains: `base.py` (`BaseFormatReader` ABC), `csv.py`, `json.py`,
  `parquet.py`, `geojson.py`, `xlsx.py`. `__init__.py` holds the `READERS`
  registry dict + `get_reader(format_name)`.
- Key files: `src/datasluice/data/readers/__init__.py` (registry),
  `src/datasluice/data/readers/csv.py`.

**`src/datasluice/transforms/`:**
- Purpose: Closed-set normalization pipeline over `RecordBatch` iterators.
- Contains: `protocol.py` (`TransformStep` Protocol + `TransformContext`),
  `pipeline.py` (`Pipeline`, `compose`), `steps.py` (`Filter`,
  `SelectColumns`, `RenameColumns`, `CastSchema`, `NormalizeTimestamps`,
  `Flatten`).
- Key files: `src/datasluice/transforms/pipeline.py`,
  `src/datasluice/transforms/steps.py`.

**`src/datasluice/sync/`:**
- Purpose: Incremental, checkpointed resource synchronization.
- Contains: `sync.py` (`sync_resources`, `SyncOutcome`), `materialize.py`
  (`materialize`, `materialize_checkpointed`), `state_store.py`
  (`FileStateStore`, `InMemoryStateStore`), `_hashing.py`
  (`logical_sha256`).
- Key files: `src/datasluice/sync/sync.py`,
  `src/datasluice/sync/state_store.py`.

**`src/datasluice/integrations/`:**
- Purpose: Terminal export to downstream data ecosystems.
- Contains: `arrow.py` (`to_arrow` shared substrate), `pandas.py`, `polars.py`,
  `duckdb.py` (also `_validate_table_name` SEC-03 guard), `dlt.py`
  (`datasluice_source`), `airflow.py` (`DataSluiceOperator`).
- Key files: `src/datasluice/integrations/arrow.py` (substrate all terminals
  delegate through).

**`src/datasluice/io/`:**
- Purpose: Local/remote byte storage, caching, checksums, downloading.
- Contains: `storage.py` (`Storage` ABC + `LocalStorage`),
  `fsspec_storage.py` (`FsspecStorage`), `local.py` (`ensure_dir`,
  `safe_filename`, `save_bytes`), `cache.py` (`FileCache`),
  `content_cache.py` (`ContentCache`, SQLite WAL), `downloader.py`
  (`Downloader`), `filesystem.py` (`open_filesystem`), `checksums.py`
  (`compute_hash`, `compute_sha256`, `compute_md5`, `verify_checksum`).
- Key files: `src/datasluice/io/filesystem.py`, `src/datasluice/io/storage.py`.

**`src/datasluice/discovery/`:**
- Purpose: Auto-detect portal platform type.
- Contains: `detector.py` (`detect`), `fingerprints.py` (`PATH_FINGERPRINTS`,
  `HTML_FINGERPRINTS`), `portal_metadata.py` (`PortalMetadata`).
- Key files: `src/datasluice/discovery/detector.py`.

**`src/datasluice/auth/`:**
- Purpose: Pluggable authentication strategies.
- Contains: `base.py` (`BaseAuth` ABC), `none.py` (`NoAuth`, default),
  `api_key.py`, `bearer.py`, `basic.py`, `headers.py`.
- Key files: `src/datasluice/auth/base.py`.

**`src/datasluice/credentials/`:**
- Purpose: Host-scoped credential resolution with single-flight refresh.
- Contains: `host_provider.py` (`HostCredentialProvider`).
- Key files: `src/datasluice/credentials/host_provider.py`.

**`src/datasluice/contracts/`:**
- Purpose: Public conformance suite for catalog connectors.
- Contains: `checks.py` (`run_contract_suite` 8-check matrix), `fixtures.py`.
- Key files: `src/datasluice/contracts/checks.py`.

**`src/datasluice/cli/`:**
- Purpose: Typer CLI commands.
- Contains: `app.py` (Typer `app`, registers commands), `search.py`,
  `inspect.py`, `download.py`, `detect.py`.
- Key files: `src/datasluice/cli/app.py` (entry point `datasluice.cli.app:app`).

**`src/datasluice/config/`:**
- Purpose: Default configuration constants (no env-var system).
- Contains: `defaults.py` (`DEFAULT_TIMEOUT`, `DEFAULT_RETRIES`,
  `DEFAULT_RATE_LIMIT`, `DEFAULT_PAGE_SIZE`, `DEFAULT_CACHE_DIR`,
  `DEFAULT_CACHE_TTL`, `DEFAULT_LOG_LEVEL`).
- Key files: `src/datasluice/config/defaults.py`.

**`tests/`:**
- Purpose: Pytest suite mirroring the `src/datasluice/` package tree.
- Contains: `conftest.py`, `unit/` (per-package subdirs: `auth/`, `cli/`,
  `connectors/`, `contracts/`, `credentials/`, `data/`+`data/readers/`,
  `discovery/`, `domain/`, `formats/`, `integrations/`, `io/`, `ports/`,
  `runtime/`, `sync/`, `transforms/`, `transport/`), `integration/<portal>/`,
  `fixtures/<portal>/` (hand-authored JSON), `helpers/`.
- Key files: `tests/conftest.py`, `tests/fixtures/`,
  `tests/helpers/`.

## Key File Locations

**Entry Points:**
- `src/datasluice/cli/app.py`: Typer `app` (registered as the `datasluice`
  console script in `pyproject.toml`).
- `src/datasluice/__init__.py`: Public API — re-exports `DataSluiceSession`,
  domain models, and exception hierarchy.
- `src/datasluice/_version.py`: Version via `importlib.metadata` (kept separate
  to break a circular import with `transport/user_agent.py`).
- `pyproject.toml` `[project.entry-points."datasluice.connectors"]`: the three
  built-in connector factories.

**Configuration:**
- `pyproject.toml`: build (hatchling), deps, optional extras, ruff
  (`line-length = 120`, selects `E,W,F,I,B,UP`), coverage (`fail_under = 50`),
  pytest (`testpaths = ["tests"]`, `pythonpath = ["src", "."]`), `ty` config.
- `src/datasluice/config/defaults.py`: `DEFAULT_*` constants.
- `zensical.toml`: docs config (NOT `mkdocs.yml`).
- `justfile` / `Makefile`: `qa` task (ruff format → ruff lint → ty check → pytest).
- `.pre-commit-config.yaml`: includes LOCAL hooks for `ty check` and `pytest`.
- `release-please-config.json` + `.release-please-manifest.json`: release
  automation (Conventional Commits required).

**Core Logic:**
- `src/datasluice/runtime/session.py`: composition root / public facade.
- `src/datasluice/runtime/plugin_manager.py`: entry-point connector discovery.
- `src/datasluice/connectors/base.py`: `BaseAdapter` ABC.
- `src/datasluice/data/access.py`: access-kind dispatch (the data-plane router).
- `src/datasluice/data/batch_stream.py`: the Arrow stream currency type.
- `src/datasluice/sync/sync.py`: incremental sync loop.
- `src/datasluice/transforms/pipeline.py`: normalization pipeline runner.

**Testing:**
- `tests/conftest.py`: shared fixtures.
- `tests/fixtures/<portal>/`: hand-authored portal JSON for the contract suite.
- `tests/helpers/`: shared test utilities.
- `tests/unit/<package>/`: unit tests mirroring `src/datasluice/`.
- `tests/integration/<portal>/`: per-portal integration tests.

## Naming Conventions

**Files:**
- Library modules: `snake_case.py` (e.g. `http_client.py`, `batch_stream.py`).
- Private/internal modules prefixed with `_`: `_version.py`, `_reject.py`,
  `_byte_source.py`, `_hashing.py` (sync). These are implementation detail
  and not re-exported from package `__init__.py`.
- Test files: `test_<module>.py` co-located under `tests/unit/<package>/`
  mirroring the source tree.
- One concept per file inside `domain/`, `ports/`, `auth/`, `data/readers/`,
  `transforms/`.

**Directories:**
- One subpackage per portal under `connectors/` (`ckan/`, `datagouv/`,
  `socrata/`).
- One subpackage per concern at the top level (`transport/`, `data/`, `sync/`,
  `io/`, etc.) — flat, not deeply nested.
- Plural for collections of related concepts (`ports/`, `transforms/`,
  `integrations/`, `contracts/`, `credentials/`); singular for a single
  concern (`domain/`, `runtime/`, `config/`).

**Classes / Functions:**
- Adapter classes: `<Platform>Adapter` (`CKANAdapter`, `DataGouvAdapter`,
  `SocrataAdapter`).
- Factory functions: `create_<platform>_connector(ctx)`.
- Mapper functions: `map_<noun>(raw, ...)` (`map_dataset`, `map_resource`,
  `map_organization`, `map_license`).
- Error mappers: `map_<platform>_error(status_code, body)`.
- Pagination: `<Platform>Page` dataclass with `to_params()` + `next_page()`.
- Transport classes: `<Backend>Transport` or `<Backend>Client`.
- Transform steps: verb-noun (`SelectColumns`, `RenameColumns`,
  `NormalizeTimestamps`, `Filter`, `Flatten`, `CastSchema`).
- Ports: `<Noun>Port` or `<Noun>Catalog`/`<Adjective>Transport`
  (`StoragePort`, `CachePort`, `SearchableCatalog`, `StreamingTransport`).
- Constants: `UPPER_SNAKE_CASE` (`DEFAULT_TIMEOUT`, `PATH_FINGERPRINTS`).

## Where to Add New Code

**New portal connector (e.g. ArcGIS Open Data):**
1. Pick a canonical `portal_type` slug (e.g. `"arcgis"`).
2. Create `src/datasluice/connectors/arcgis/` with the standard file shape:
   `__init__.py`, `adapter.py` (`ArcGISAdapter(BaseAdapter)` with a
   `capabilities: ClassVar[CatalogCapabilities]`), `mapper.py`,
   `pagination.py`, `errors.py`, `factory.py`
   (`create_arcgis_connector(ctx) -> ArcGISAdapter`).
3. Register the factory in `pyproject.toml`
   `[project.entry-points."datasluice.connectors"]`:
   `arcgis = "datasluice.connectors.arcgis.factory:create_arcgis_connector"`.
4. Add detection probes to `src/datasluice/discovery/fingerprints.py`
   (`PATH_FINGERPRINTS["/api/v3/datasets"] = "arcgis"`).
5. Call `_reject_unsupported_fields(query, capabilities, "arcgis")` at the top
   of `search()`.
6. Add fixtures under `tests/fixtures/arcgis/` and run
   `run_contract_suite` against them.

**New format reader (e.g. XML):**
1. Add `src/datasluice/data/readers/xml.py` with an `XMLReader(BaseFormatReader)`
   implementing `read_batches(source, batch_size)`; lazy-import heavy deps
   inside the method.
2. Register it in `src/datasluice/data/readers/__init__.py:READERS`
   (`"XML": XMLReader`).
3. Add a format alias to `src/datasluice/domain/resource.py:_FORMAT_ALIASES`
   if needed.

**New transform step:**
- The transform set is CLOSED for normalization only (PROJECT.md Out of Scope
  for third-party extension). If adding an internal one: implement
  `TransformStep.apply(batches, context)` as a frozen dataclass in
  `src/datasluice/transforms/steps.py`, delegate to `pyarrow.compute`, and
  re-export from `src/datasluice/transforms/__init__.py`'s `__getattr__`.

**New transport backend:**
1. Add `src/datasluice/transport/<backend>_transport.py` with a class
   satisfying the relevant Protocol(s) (`Transport` minimum;
   `StreamingTransport`/`ConditionalTransport` if supported).
2. Import heavy deps lazily inside `__init__`/methods.
3. Wire it via `DataSluiceSession(transport=...)` injection — do NOT add it to
   `create_default_transport` unless it should be a default.

**New StateStore backend:**
1. Implement `StateStore` Protocol (`get`/`put`/`delete`) in
   `src/datasluice/sync/` or a third-party package.
2. Inject via `DataSluiceSession(state_store=...)`.

**New terminal integration:**
1. Add `src/datasluice/integrations/<ecosystem>.py` with a function taking a
   `BatchStream` (delegate through `to_arrow` for single-substrate consistency).
2. Lazy-import the heavy dep inside the function body.
3. Re-export from `src/datasluice/integrations/__init__.py:__all__`.

**New utility/helper:**
- Shared library helper: `src/datasluice/<concern>/<name>.py`, re-export from
  the package `__init__.py` (use PEP 562 `__getattr__` if it pulls an optional
  dependency).
- Test-only helper: `tests/helpers/`.

**New CLI command:**
1. Add `src/datasluice/cli/<command>.py` with a `<command>(...)` function
   (use `Annotated[T, typer.Option(...)]`, not `param: T = typer.Option(...)`
   — B008).
2. Register it in `src/datasluice/cli/app.py`:
   `app.command(name="<command>")(command)`.

## Special Directories

**`graphify-out/`:**
- Purpose: Knowledge-graph artifacts (god nodes, community structure,
  cross-file relationships) consumed by the `graphify` skill.
- Generated: Yes (by `graphify update .`, AST-only, no API cost).
- Committed: Yes. Dirty files after hooks are expected and not a reason to
  skip graphify.

**`.planning/`:**
- Purpose: GSD workflow state — `PROJECT.md`, `REQUIREMENTS.md`, `ROADMAP.md`,
  `STATE.md`, `phases/`, `codebase/` (this document), `research/`, `todos/`.
- Generated: Partially (GSD commands read/write it).
- Committed: Yes.

**`docs/`:**
- Purpose: Zensical (MkDocs Material wrapper) docs; `docs/api.md` uses the
  `::: datasluice` mkdocstrings directive for auto-generated API docs.
- Logo: `docs/assets/datasluice.png`.
- Config: `zensical.toml` (NOT `mkdocs.yml`).
- Generated: API pages are auto-generated at build time.
- Committed: Yes.

**`dist/`:**
- Purpose: Built distributions (`uv build` output).
- Generated: Yes.
- Committed: No (build artifact).

**`site/`:**
- Purpose: Built documentation site.
- Generated: Yes.
- Committed: No.

**`notebook/`:**
- Purpose: Exploration notebooks (not part of the shipped package).
- Generated: No.
- Committed: Yes.

**`tests/fixtures/<portal>/`:**
- Purpose: Hand-authored portal JSON payloads served over localhost sockets
  by the contract suite (no transport mocking).
- Generated: No.
- Committed: Yes.

**`.github/workflows/`:**
- Purpose: CI/CD — `ci.yml` (lint/type/test/build/smoke on PR + push to main,
  Python 3.12/3.13/3.14 matrix), `release-please.yml` (release PR automation),
  `publish.yml` (TestPyPI auto → PyPI gated on approval).
- Generated: No.
- Committed: Yes.

---

*Structure analysis: 2026-07-30*
