# Codebase Structure

**Analysis Date:** 2026-07-05

## Directory Layout

```
datasluice/
├── src/
│   └── datasluice/                # Library package (src-layout)
│       ├── __init__.py            # Public API re-exports
│       ├── _version.py            # __version__ (kept separate — circular-import guard)
│       ├── client.py              # DataSluice facade (primary entry)
│       ├── exceptions.py          # Exception hierarchy
│       ├── logging.py             # Package logger setup
│       ├── py.typed               # PEP 561 marker
│       ├── adapters/              # Portal adapters (strategy/plugin layer)
│       ├── auth/                  # Authentication strategies
│       ├── cli/                   # Typer CLI commands
│       ├── config/                # Settings + defaults
│       ├── discovery/             # Portal-type auto-detection
│       ├── domain/                # Portal-agnostic dataclasses
│       ├── formats/               # File-format readers
│       ├── integrations/          # Optional ecosystem bridges
│       ├── io/                    # Download / cache / storage / checksums
│       └── transport/             # HTTP client, retry, rate-limit, pagination, UA
├── tests/
│   ├── conftest.py                # Shared fixtures (fixtures_dir)
│   ├── fixtures/                  # Sample portal responses (ckan/datagouv/socrata)
│   ├── integration/               # Integration tests (currently empty — .gitkeep only)
│   └── unit/                      # Unit tests mirroring src/ layout
├── docs/                          # MkDocs / Zensical docs source
├── scripts/                       # release.py
├── .github/workflows/             # ci.yml, codeql.yml, docs.yml, publish.yml, release.yml, zizmor.yml
├── pyproject.toml                 # Build + deps + ruff/pytest/coverage config
├── justfile                       # Task runner (just qa, just docs-serve)
├── Makefile                       # Zero-dependency fallback for same targets
├── zensical.toml                  # Docs build config (NOT mkdocs.yml)
├── uv.lock                        # Locked dependency resolution
└── AGENTS.md                      # High-signal contributor guide
```

## Directory Purposes

**`src/datasluice/`:**
- Purpose: Library package; only importable surface.
- Contains: All production Python modules.
- Key files: `client.py`, `__init__.py`, `exceptions.py`, `_version.py`.

**`src/datasluice/adapters/`:**
- Purpose: Portal-specific translation logic.
- Contains: `base.py` (ABC), `registry.py` (singleton `registry`), `factory.py` (`create_adapter`), and one subpackage per portal.
- Key files: `adapters/base.py`, `adapters/registry.py`, `adapters/factory.py`, `adapters/__init__.py`.
- Subpackage convention: each portal has `adapter.py`, `mapper.py`, `pagination.py`, `errors.py`, `__init__.py` (e.g. `adapters/ckan/`).

**`src/datasluice/domain/`:**
- Purpose: Pure-dataclass contracts shared across layers.
- Contains: One file per model.
- Key files: `domain/dataset.py`, `domain/resource.py`, `domain/result.py`, `domain/organization.py`, `domain/license.py`, `domain/query.py`.

**`src/datasluice/transport/`:**
- Purpose: All HTTP I/O and request-time cross-cutting concerns.
- Contains: `http_client.py`, `retry.py`, `rate_limit.py`, `pagination.py`, `user_agent.py`.
- Key files: `transport/http_client.py` (urllib-based `HttpClient`), `transport/retry.py` (`RetryPolicy`, `with_retry[T]`).

**`src/datasluice/auth/`:**
- Purpose: Credential-injection strategies.
- Contains: `base.py` (ABC) + five implementations.
- Key files: `auth/base.py`, `auth/api_key.py`, `auth/bearer.py`, `auth/basic.py`, `auth/headers.py`, `auth/none.py`.

**`src/datasluice/discovery/`:**
- Purpose: Auto-detect portal platform from a URL.
- Contains: `detector.py`, `fingerprints.py`, `portal_metadata.py`.
- Key files: `discovery/detector.py` (`detect_portal_type`), `discovery/fingerprints.py` (`PATH_FINGERPRINTS`, `HTML_FINGERPRINTS`).

**`src/datasluice/io/`:**
- Purpose: Download, cache, verify, and persist resource bytes.
- Contains: `downloader.py`, `cache.py`, `storage.py`, `checksums.py`, `local.py`.
- Key files: `io/downloader.py` (`Downloader`), `io/storage.py` (`Storage` ABC + `LocalStorage`), `io/cache.py` (`FileCache`).

**`src/datasluice/formats/`:**
- Purpose: Format readers that normalise content into `list[dict]`.
- Contains: `base.py` (ABC) + per-format readers + `__init__.py` registry.
- Key files: `formats/__init__.py` (`READERS`, `get_reader`), `formats/csv.py`, `formats/json.py`, `formats/parquet.py`, `formats/xlsx.py`, `formats/geojson.py`.

**`src/datasluice/integrations/`:**
- Purpose: Bridges to pandas, polars, dlt, duckdb, apache-airflow.
- Contains: One module per integration; all heavy deps imported lazily.
- Key files: `integrations/pandas.py`, `integrations/polars.py`, `integrations/dlt.py`, `integrations/duckdb.py`, `integrations/airflow.py`.

**`src/datasluice/config/`:**
- Purpose: Runtime configuration.
- Contains: `defaults.py` (constants), `settings.py` (`Settings`, `load_settings`).
- Key files: `config/settings.py`, `config/defaults.py`.

**`src/datasluice/cli/`:**
- Purpose: Typer-based CLI surface.
- Contains: `app.py` (root Typer app) + one module per subcommand.
- Key files: `cli/app.py`, `cli/search.py`, `cli/inspect.py`, `cli/download.py`, `cli/detect.py`.

**`tests/unit/`:**
- Purpose: Fast, hermetic unit tests.
- Contains: Subdirectories mirroring `src/datasluice/` layout (`adapters/`, `auth/`, `discovery/`, `domain/`, `formats/`, `integrations/`, `io/`, `transport/`).
- Key files: `tests/conftest.py` (`fixtures_dir` fixture), `tests/unit/adapters/test_ckan_mapper.py`, `tests/unit/domain/test_models.py`.

**`tests/fixtures/`:**
- Purpose: Sample portal payloads for tests.
- Contains: Subdirectories per portal (`ckan/`, `datagouv/`, `socrata/`) — currently empty.
- Generated: No (hand-curated).

**`tests/integration/`:**
- Purpose: Live network-bound tests per portal.
- Contains: `ckan/`, `datagouv/`, `socrata/` — each only holds `.gitkeep`.

**`docs/`:**
- Purpose: User-facing documentation (Zensical / MkDocs Material).
- Contains: `index.md`, `architecture.md`, `adapters.md`, `api.md`, `install.md`, `supported-portals.md`, `assets/`, `examples/`.
- Generated: `docs/api.md` uses `::: datasluice` mkdocstrings directive (auto-generated at build time).

## Key File Locations

**Entry Points:**
- `src/datasluice/__init__.py`: Package public API (re-exports `DataSluice`, domain, exceptions).
- `src/datasluice/client.py`: `DataSluice` facade class — primary programmatic entry.
- `src/datasluice/cli/app.py`: Typer app registered as `datasluice` console script (`pyproject.toml:[project.scripts]`).

**Configuration:**
- `pyproject.toml`: Build (hatchling), deps, ruff, ty, pytest, coverage config.
- `src/datasluice/config/settings.py`: `Settings` dataclass + `load_settings()`.
- `src/datasluice/config/defaults.py`: All `DEFAULT_*` constants.
- `zensical.toml`: Docs build config (NOT `mkdocs.yml`).
- `justfile` / `Makefile`: QA targets (`qa`, `docs-serve`, etc.).
- `.pre-commit-config.yaml`: Pre-commit hooks (includes local `ty check` and `pytest` hooks).
- `.editorconfig`: Editor formatting rules.

**Core Logic:**
- `src/datasluice/adapters/factory.py`: `create_adapter()` — adapter resolution + discovery fallback.
- `src/datasluice/adapters/registry.py`: Module-level `registry` singleton.
- `src/datasluice/adapters/base.py`: `BaseAdapter` ABC.
- `src/datasluice/transport/http_client.py`: `HttpClient` — single network chokepoint.
- `src/datasluice/transport/retry.py`: `with_retry()` + `RetryPolicy`.
- `src/datasluice/io/downloader.py`: `Downloader` — cache + checksum pipeline.
- `src/datasluice/discovery/detector.py`: `detect_portal_type()`.
- `src/datasluice/formats/__init__.py`: `READERS` dict + `get_reader()`.

**Testing:**
- `tests/conftest.py`: Shared `fixtures_dir` pytest fixture.
- `tests/unit/adapters/test_ckan_mapper.py`: Example mapper unit test.
- `tests/unit/domain/test_models.py`: Domain dataclass tests.
- `tests/fixtures/`: Portal payload fixtures (per-portal subdirs).

## Naming Conventions

**Files:**
- Library modules: `snake_case.py` (e.g. `http_client.py`, `rate_limit.py`).
- One concept per file: `dataset.py`, `resource.py`, `license.py`.
- Test files: `test_<module>.py` (e.g. `test_transport.py`, `test_io.py`, `test_ckan_mapper.py` — nested under the matching source subdirectory).
- Adapter subpackages share a fixed file set: `adapter.py`, `mapper.py`, `pagination.py`, `errors.py`, `__init__.py`.

**Directories:**
- Lowercase, no separators: `adapters`, `transport`, `formats`, `integrations`.
- Adapter subpackages match the canonical portal type: `ckan/`, `datagouv/`, `socrata/`, `custom/`.
- Tests mirror source layout: `tests/unit/<src_subdir>/`.

**Classes:**
- `PascalCase` (e.g. `DataSluice`, `HttpClient`, `CKANAdapter`, `DataGouvAdapter`, `RateLimiter`, `BaseAdapter`).
- Adapter classes suffix `Adapter` (e.g. `CKANAdapter`, `SocrataAdapter`, `CustomAdapter`).
- Strategy base classes prefix `Base` (e.g. `BaseAdapter`, `BaseAuth`, `BaseFormatReader`).
- Pagination helper classes suffix `Page` per portal (e.g. `CKANPage`, `DataGouvPage`, `SocrataPage`).
- Storage implementations suffix `Storage` (e.g. `LocalStorage`).
- Format readers suffix `Reader` (e.g. `CSVReader`, `JSONReader`, `ParquetReader`).
- Auth strategies suffix `Auth` (e.g. `APIKeyAuth`, `BearerAuth`, `BasicAuth`, `HeadersAuth`, `NoAuth`).

**Functions:**
- `snake_case` (e.g. `create_adapter`, `detect_portal_type`, `get_reader`, `with_retry`, `load_settings`, `build_user_agent`).
- Mapper functions prefix `map_` (e.g. `map_dataset`, `map_resource`, `map_organization`, `map_license`, `map_ckan_error`).
- Constructor-like module-level helpers use `load_` / `get_` / `build_` verbs.

**Constants:**
- `UPPER_SNAKE_CASE` (e.g. `DEFAULT_TIMEOUT`, `PATH_FINGERPRINTS`, `HTML_FINGERPRINTS`, `READERS`, `_FORMAT_ALIASES`).

**Module-level singletons:**
- Lowercase (`registry` in `adapters/registry.py`).

**Portal type identifiers:**
- Lowercase canonical strings used as registry keys: `"ckan"`, `"datagouv"`, `"socrata"`, `"custom"` (stored as `ClassVar[str] portal_type` on each adapter).

## Where to Add New Code

**New portal adapter (e.g. for ArcGIS Hub):**
1. Create `src/datasluice/adapters/arcgis/` with `__init__.py`, `adapter.py`, `mapper.py`, `pagination.py`, `errors.py`.
2. Subclass `BaseAdapter` in `adapter.py`; set `portal_type = "arcgis"`.
3. Implement `search`, `get_dataset`, `list_resources`, `get_organization` by calling `self.transport.get_json(...)` and mapping via `mapper.py` functions.
4. Register the adapter in `src/datasluice/adapters/__init__.py`:
   - Add `from datasluice.adapters.arcgis import ArcGISAdapter` to the imports.
   - Add `registry.register(ArcGISAdapter.portal_type, ArcGISAdapter)` after the existing registrations.
   - Add `"ArcGISAdapter"` to `__all__`.
5. Add fingerprints to `src/datasluice/discovery/fingerprints.py` (`PATH_FINGERPRINTS` and/or `HTML_FINGERPRINTS`).
6. Add unit tests under `tests/unit/adapters/test_arcgis_mapper.py`.

**New CLI command (e.g. `datasluice list-orgs`):**
1. Create `src/datasluice/cli/list_orgs.py` with a function decorated via `typer.Option`/`typer.Argument` (use `Annotated[...]` form, not function-call defaults — see B008 in `AGENTS.md`).
2. Register it in `src/datasluice/cli/app.py` via `app.command(name="list-orgs")(list_orgs)`.
3. Import `DataSluice` lazily inside the function body (matches existing commands).

**New format reader (e.g. XML):**
1. Create `src/datasluice/formats/xml.py` subclassing `BaseFormatReader`; set `format_name = "XML"`.
2. Register it in `src/datasluice/formats/__init__.py`: import the class and add `"XML": XMLReader` to the `READERS` dict; add to `__all__`.
3. If the reader needs an optional dep, import it lazily inside `read()` and raise `FormatError` on `ImportError` (see `formats/parquet.py` and `formats/xlsx.py`).
4. Add an alias entry to `_FORMAT_ALIASES` in `src/datasluice/domain/resource.py` if a new media type should map to it.
5. Add tests under `tests/unit/formats/test_formats.py`.

**New auth strategy:**
1. Create `src/datasluice/auth/<strategy>.py` subclassing `BaseAuth`.
2. Implement `apply(headers, params) -> tuple[dict, dict]` (return copies, never mutate inputs).
3. Export from `src/datasluice/auth/__init__.py` (`__all__`).

**New storage backend (e.g. S3):**
1. Create `src/datasluice/io/s3.py` subclassing `Storage`.
2. Implement `write`, `read`, `exists`.
3. Export from `src/datasluice/io/__init__.py`.
4. Pass an instance to `Downloader(transport, storage=...)` (the `DataSluice` client does not currently wire a non-default storage — extend `client.py` if needed).

**New integration:**
1. Create `src/datasluice/integrations/<tool>.py`.
2. Import the heavy dependency lazily inside each public function; raise `ImportError` with install instructions on missing dep.
3. Add the module name to `__all__` in `src/datasluice/integrations/__init__.py`.
4. Add an optional dependency group in `pyproject.toml:[project.optional-dependencies]` and to the `all` aggregate.

**New domain model or field:**
1. Add a new file in `src/datasluice/domain/` (e.g. `domain/tag.py`) or extend an existing frozen dataclass.
2. Export from `src/datasluice/domain/__init__.py` and consider re-exporting from the top-level `src/datasluice/__init__.py`.
3. Update mappers in every adapter that should populate the new field.
4. Update the matching unit tests under `tests/unit/domain/`.

**New utility / shared helper:**
- Place under the most specific existing layer (`transport/`, `io/`, etc.).
- Avoid creating a top-level `utils.py`; the project has none and prefers targeted module names.

**Tests:**
- Unit tests: `tests/unit/<src_subdir>/test_<module>.py` mirroring source layout.
- Integration tests: `tests/integration/<portal>/test_*.py` (currently empty).
- Fixtures: portal sample payloads under `tests/fixtures/<portal>/`.
- Shared pytest fixtures: `tests/conftest.py`.

## Special Directories

**`.planning/`:**
- Purpose: GSD planning artifacts (codebase maps, phase plans, roadmaps).
- Generated: Partially (by GSD tooling).
- Committed: Yes (`.planning/codebase/` documents are committed for downstream agents).

**`.github/workflows/`:**
- Purpose: CI/CD pipelines.
- Contains: `ci.yml` (lint/type/test matrix on Python 3.12/3.13/3.14), `codeql.yml`, `docs.yml`, `publish.yml`, `release.yml`, `zizmor.yml`.
- Generated: No.
- Committed: Yes.

**`docs/assets/`:**
- Purpose: Static doc assets (logo at `docs/assets/datasluice.png`).
- Generated: No.

**`site/`:**
- Purpose: Built documentation output (Zensical).
- Generated: Yes (by `just docs-build` / `zensical build`).
- Committed: No (build artifact).

**`.venv/`, `.cache/`, `.pytest_cache/`, `.ruff_cache/`:**
- Purpose: Local tooling caches and virtualenv.
- Generated: Yes.
- Committed: No (gitignored).

**`.datasluice/cache/` (runtime):**
- Purpose: Default `FileCache` directory created by `io/cache.py` when downloads are cached.
- Generated: Yes (at runtime).
- Committed: No.

**`scripts/`:**
- Purpose: One-off maintenance scripts.
- Contains: `release.py`.
- Generated: No.
- Committed: Yes.

---

*Structure analysis: 2026-07-05*
