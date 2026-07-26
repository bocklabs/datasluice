<!-- refreshed: 2026-07-26 -->
# Architecture

**Analysis Date:** 2026-07-26

## System Overview

DataSluice is a Python library **and** CLI providing one unified interface for
open-data portal discovery, extraction, format normalization, and pipeline
integration (pandas / dlt / duckdb / airflow). The design follows a
**hexagonal (ports-and-adapters) architecture** with an explicit dependency-injection
composition root.

```text
┌─────────────────────────────────────────────────────────────────────┐
│                          Entry Surfaces                              │
├──────────────────────────────────┬──────────────────────────────────┤
│   CLI (Typer)                    │   Python API (library import)    │
│   `src/datasluice/cli/app.py`    │   `from datasluice import ...`   │
│   commands: search/inspect/      │                                  │
│   download/detect                │                                  │
└───────────────┬──────────────────┴──────────────┬───────────────────┘
                │                                 │
                ▼                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│              Composition Root / Facade (ARCH-03)                     │
│   `src/datasluice/runtime/session.py`  →  DataSluiceSession          │
│   wires: PluginManager + create_default_transport() + auth + cache   │
└───────┬───────────────────┬──────────────────────┬──────────────────┘
        │                   │                      │
        ▼                   ▼                      ▼
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────────────┐
│ Plugin Discovery │ │  Discovery       │ │  Transport (infra port)  │
│ runtime/         │ │  detect_portal_  │ │  transport/http_client   │
│ plugin_manager   │ │  type()          │ │  transport/httpx_transport│
│ (entry points)   │ │  discovery/      │ │  + retry + rate-limit    │
└────────┬─────────┘ └──────────────────┘ └────────────┬─────────────┘
         │                                              │
         ▼                                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  Connectors / Adapters (CatalogPort impls)           │
│   base.py: BaseAdapter (ABC)   ←  ckan/  socrata/  datagouv/  custom│
│   each subpackage: adapter.py, mapper.py, pagination.py, factory.py │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ produce / consume
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│        Domain Models (portal-agnostic, frozen dataclasses)           │
│   `src/datasluice/domain/`  Dataset, Resource, Organization,         │
│   Query, SearchResult, License, Schema, Artifact, DetectionResult... │
└─────────────────────────────────────────────────────────────────────┘
                            ▲ consumed by
┌───────────────────────────┴─────────────────────────────────────────┐
│   IO / Formats / Integrations (lazy optional deps)                   │
│   io/downloader  io/storage  io/cache   formats/*  integrations/*    │
└─────────────────────────────────────────────────────────────────────┘
```

## Component Responsibilities

| Component | Responsibility | File |
|-----------|----------------|------|
| `DataSluiceSession` | Public facade & composition root — resolves portals, injects infra | `src/datasluice/runtime/session.py` |
| `PluginManager` | Entry-point-based connector discovery (no module singleton) | `src/datasluice/runtime/plugin_manager.py` |
| `ConnectorContext` | Frozen DI carrier passed to every connector factory | `src/datasluice/runtime/context.py` |
| `create_default_transport` | Picks httpx when importable, else urllib `HttpClient` | `src/datasluice/runtime/defaults.py` |
| `BaseAdapter` | ABC every portal adapter implements (`search`/`get_dataset`/...) | `src/datasluice/connectors/base.py` |
| `BaseAuth` | ABC for pluggable request auth strategies | `src/datasluice/auth/base.py` |
| `Transport` / `StreamingTransport` | Runtime-checkable Protocol ports for HTTP execution | `src/datasluice/ports/transport.py` |
| `CatalogPort` family | Capability Protocols (`SearchableCatalog`, `OrganizationCatalog`) | `src/datasluice/ports/catalog.py` |
| `detect_portal_type` | Probes well-known endpoints to fingerprint a portal | `src/datasluice/discovery/detector.py` |
| Domain models | Portal-agnostic record types exchanged across layers | `src/datasluice/domain/*.py` |

## Pattern Overview

**Overall:** Hexagonal architecture (ports & adapters) with dependency injection.

**Key Characteristics:**
- **Ports** are `typing.Protocol` classes decorated `@runtime_checkable` (structural typing, `isinstance`-capable) — see `src/datasluice/ports/`.
- **Adapters** are concrete implementations: `connectors/` implement `CatalogPort`, `transport/` implements `Transport`, `io/` implements `StoragePort`.
- **Entry-point plugin discovery** replaces side-effect registration: built-in + third-party connectors declare themselves under the `datasluice.connectors` entry-points group in `pyproject.toml` (`[project.entry-points."datasluice.connectors"]`). Loaded eagerly by `PluginManager.__init__`, failures isolated as `PluginFailure`.
- **Lazy imports everywhere** for optional/heavy deps so bare installs (`pip install datasluice`) work with zero optional deps — only `typer` and `rich` are hard deps.
- **Composition root is explicit** (`DataSluiceSession.__init__`); no env-var-driven `Settings` singleton (deliberately removed — enforced by `tests/unit/test_no_dead_settings.py`).
- **Frozen dataclass domain models** (`@dataclass(frozen=True)`) for value semantics and hashability.

## Layers

**CLI Layer:**
- Purpose: Thin Typer command handlers; render domain models with `rich`.
- Location: `src/datasluice/cli/`
- Contains: One module per command (`app.py`, `search.py`, `inspect.py`, `download.py`, `detect.py`).
- Depends on: `DataSluiceSession` (lazy-imported inside each command body), `rich`, `typer`.
- Used by: The `datasluice` console script (`[project.scripts] datasluice = "datasluice.cli.app:app"`).

**Composition Root / Runtime:**
- Purpose: Wire plugins, transport, auth, cache, storage into a zero-config session.
- Location: `src/datasluice/runtime/`
- Contains: `session.py`, `plugin_manager.py`, `context.py`, `defaults.py`.
- Depends on: `ports/`, `transport/`, `auth/`, `discovery/`, `config/`.
- Used by: CLI commands and library users (`from datasluice import DataSluiceSession`).

**Ports (Boundary Contracts):**
- Purpose: Unstable-but-narrow Protocol interfaces defining what adapters must satisfy.
- Location: `src/datasluice/ports/`
- Contains: `transport.py` (`Transport`, `StreamingTransport`), `catalog.py` (`CatalogPort`/`SearchableCatalog`/`OrganizationCatalog`), `storage.py` (`StoragePort`), `cache.py` (`CachePort`), `credentials.py` (`CredentialProvider`), `detector.py` (`PortalDetector`), `resource_reader.py` (`ResourceReader`), `state_store.py` (`StateStore`).
- Depends on: `domain/` (only under `TYPE_CHECKING`).
- Used by: `runtime/`, `transport/`, `io/`, `connectors/`.

**Connectors (Adapters):**
- Purpose: Translate one portal platform's API into `datasluice.domain` models.
- Location: `src/datasluice/connectors/<portal>/`
- Contains per subpackage: `adapter.py` (the `BaseAdapter` subclass), `mapper.py` (portal JSON → domain), `pagination.py` (portal-specific paging), `factory.py` (entry-point target `create_*_connector(ctx)`), `errors.py`, `__init__.py`.
- Depends on: `connectors/base.py`, `domain/`, `transport/` (via injected `Transport`), `auth/`.
- Used by: `PluginManager` resolves the factory; `DataSluiceSession.portal()` invokes it.

**Domain:**
- Purpose: Portal-agnostic data model — the lingua franca every layer speaks.
- Location: `src/datasluice/domain/`
- Contains: `dataset.py`, `resource.py`, `organization.py`, `query.py`, `result.py`, `license.py`, `schema.py`, `artifact.py`, `access.py`, `capabilities.py`, `credentials.py`, `detection.py`, `sync_state.py`.
- Depends on: nothing (only stdlib + sibling domain modules under `TYPE_CHECKING`).
- Used by: every other layer.

**Transport:**
- Purpose: HTTP execution with retry, rate-limiting, redirect safety, optional streaming.
- Location: `src/datasluice/transport/`
- Contains: `http_client.py` (urllib, always available), `httpx_transport.py` (lazy, needs `http` extra), `retry.py`, `rate_limit.py`, `redirect.py`, `pagination.py`, `user_agent.py`.
- Depends on: `auth/`, `config/defaults.py`, `exceptions.py`.
- Used by: `runtime/defaults.py`, `connectors/base.py` (lazy fallback), `io/downloader.py`.

**Discovery:**
- Purpose: Auto-detect which platform powers a portal URL.
- Location: `src/datasluice/discovery/`
- Contains: `detector.py` (`detect_portal_type`), `fingerprints.py` (`PATH_FINGERPRINTS`/`HTML_FINGERPRINTS`), `portal_metadata.py`.
- Depends on: `transport/` (probes via a fresh `HttpClient`), `runtime/plugin_manager.py` (to check registered types).
- Used by: `runtime/session.py`, `cli/detect.py`.

**IO:**
- Purpose: Download resources, cache bytes, verify checksums, abstract storage.
- Location: `src/datasluice/io/`
- Contains: `downloader.py` (`Downloader`), `storage.py` (`Storage` ABC + `LocalStorage`), `cache.py` (`FileCache`), `content_cache.py` (lazy, sqlite `ContentCache`), `fsspec_storage.py` (lazy, `FsspecStorage`), `filesystem.py` (lazy `open_filesystem`), `checksums.py`, `local.py`.
- Depends on: `transport/`, `domain/resource.py`, `exceptions.py`.
- Used by: `cli/download.py`, integrations.

**Formats:**
- Purpose: Normalize a file/bytes blob into `list[dict]` rows.
- Location: `src/datasluice/formats/`
- Contains: `base.py` (`BaseFormatReader` ABC), one reader per format (`csv.py`, `json.py`, `parquet.py` [pyarrow], `xlsx.py` [openpyxl], `geojson.py`), and `__init__.py` registry `READERS` + `get_reader()`.
- Depends on: optional deps imported **inside** `read()` methods.
- Used by: `integrations/`.

**Integrations:**
- Purpose: Bridge DataSluice into external ecosystems (pandas, polars, dlt, duckdb, airflow).
- Location: `src/datasluice/integrations/`
- Contains: `pandas.py`, `polars.py`, `dlt.py`, `duckdb.py`, `airflow.py`.
- Depends on: `DataSluiceSession` (lazy), `domain/`, optional third-party libs imported inside functions.
- Used by: end users opting into an extra.

## Data Flow

### Primary Request Path — `datasluice search`

1. CLI parses args → `src/datasluice/cli/search.py:search()` lazily imports `DataSluiceSession`.
2. `DataSluiceSession()` constructs transport via `create_default_transport()` (`src/datasluice/runtime/defaults.py:28`) and a `PluginManager` that eagerly loads `datasluice.connectors` entry points.
3. `session.portal(portal)` (`src/datasluice/runtime/session.py:166`) calls `detect_portal_type(url)` → resolves factory via `plugins.get(portal_type)` → builds `ConnectorContext(base_url, transport, auth, page_size)` → `factory(ctx)` returns e.g. `CKANAdapter`.
4. `connector.search(Query(...))` (`src/datasluice/connectors/ckan/adapter.py:28`) calls `transport.get_json(.../package_search)`, then `map_dataset()` on each result (`src/datasluice/connectors/ckan/mapper.py:53`).
5. Returns `SearchResult(datasets=[Dataset(...)])`; the CLI renders a `rich` table.

### Detection Flow — `datasluice detect`

1. `src/datasluice/cli/detect.py:detect()` imports `detect_portal_type`.
2. `detect_portal_type` (`src/datasluice/discovery/detector.py:26`) normalizes the URL, iterates `PATH_FINGERPRINTS`, and issues a probe `HttpClient().request(url)` for each path whose `portal_type` is registered in a fresh `PluginManager`.
3. First non-error probe wins → returns the canonical portal name.

### Download Flow — `datasluice download`

1. `src/datasluice/cli/download.py:download()` resolves a connector as above.
2. `connector.get_dataset(dataset_id)` → `Dataset.resources`.
3. Filters by `--format`, then calls `cast("Any", connector).downloader.download_many(resources, dest)` (`src/datasluice/cli/download.py:37`). `Downloader.download()` (`src/datasluice/io/downloader.py:41`) consults the optional `FileCache`, fetches via `transport.download()`, optionally verifies SHA-256, and writes via `LocalStorage`/`save_bytes()`.

**State Management:**
- No global mutable state. `PluginManager` is an **injected instance** (ARCH-06), never module-level — verified by `tests/unit/runtime/test_no_global_state.py`.
- The only module-level singletons are immutable: `DEFAULT_*` constants (`src/datasluice/config/defaults.py`) and the `READERS` registry (`src/datasluice/formats/__init__.py`).
- Per-request retry/refresh state is local to the closure inside `HttpxTransport.request` (`refreshed: list[bool]`).

## Key Abstractions

**`ConnectorContext` (frozen dataclass):**
- Purpose: The single injection seam handed to every connector factory.
- Examples: `src/datasluice/runtime/context.py`; consumed by `create_ckan_connector(ctx)` (`src/datasluice/connectors/ckan/factory.py`).
- Pattern: Dependency-injection carrier — third-party connectors receive infra via the context rather than reaching for globals.

**`BaseAdapter` (ABC) + `CatalogPort` (Protocol):**
- Purpose: `BaseAdapter` is the inheritance contract; `CatalogPort` is the structural capability contract.
- Examples: `src/datasluice/connectors/base.py`, `src/datasluice/ports/catalog.py`.
- Pattern: Capability probing — `isinstance(connector, SearchableCatalog)` lets callers ask "can this portal search?" without backend-specific types.

**`Transport` / `StreamingTransport` Protocols:**
- Purpose: Backend-agnostic HTTP boundary satisfied structurally by `HttpClient` and `HttpxTransport`.
- Examples: `src/datasluice/ports/transport.py`, `src/datasluice/transport/http_client.py`, `src/datasluice/transport/httpx_transport.py`.
- Pattern: `HttpxTransport` satisfies **both** protocols; `HttpClient` satisfies only `Transport`, so Phase-4 streaming code can probe `isinstance(transport, StreamingTransport)`.

**Domain models (frozen dataclasses):**
- Purpose: Portal-agnostic value objects exchanged across all layers.
- Examples: `src/datasluice/domain/dataset.py`, `resource.py`, `query.py`, `result.py`.
- Pattern: `Resource.normalize_format()` (`src/datasluice/domain/resource.py:55`) centralizes format-alias normalization used by every mapper.

## Entry Points

**CLI `datasluice`:**
- Location: `src/datasluice/cli/app.py:app` (Typer app).
- Declared: `[project.scripts] datasluice = "datasluice.cli.app:app"` in `pyproject.toml`.
- Triggers: console invocation; `python -m datasluice.cli.app`.
- Responsibilities: route to `search`/`inspect`/`download`/`detect` subcommands; `--version` short-circuits via eager `@app.callback()`.

**Python library `DataSluiceSession`:**
- Location: `src/datasluice/runtime/session.py`; re-exported from package root in `src/datasluice/__init__.py`.
- Triggers: `from datasluice import DataSluiceSession`.
- Responsibilities: zero-config facade; `.portal(url)` returns a connector, `.search(url, query)` is a one-liner convenience.

**Connector factories (entry-point targets):**
- Location: `src/datasluice/connectors/<portal>/factory.py`.
- Declared: `[project.entry-points."datasluice.connectors"]` in `pyproject.toml`.
- Responsibilities: `create_<portal>_connector(ctx: ConnectorContext) -> BaseAdapter`.

## Architectural Constraints

- **Python version:** 3.12+ required (`requires-python = ">= 3.12"`). Use PEP 695 type-param syntax (`def f[T](...)`), not `TypeVar`.
- **Threading:** Single-threaded; transport clients (`httpx.Client`, urllib opener) are constructed per-session. `HttpxTransport` documents its underlying client as thread-safe for reuse.
- **Global state:** Forbidden by design. No module-level `AdapterRegistry`/`Settings` (both deliberately removed). Validated by `tests/unit/runtime/test_no_global_state.py` and `tests/unit/test_no_dead_settings.py`.
- **Circular imports:** `src/datasluice/_version.py` is a standalone module to break the cycle with `src/datasluice/transport/user_agent.py`. Do NOT inline it into `__init__.py`.
- **Lazy import discipline (D-P3-01):** Heavy/optional deps (httpx, pyarrow, openpyxl, pandas, polars, dlt, duckdb, airflow, fsspec) must be imported inside functions/methods, or via PEP 562 `__getattr__` (see `src/datasluice/transport/__init__.py` and `src/datasluice/io/__init__.py`), never at module top-level.
- **Hard dependencies:** Only `typer` and `rich` may be assumed at import time. Everything else is an optional extra.
- **Line length:** 120 (ruff).
- **No comments in code** unless explicitly requested; docstrings are Google-style with a summary first line.

## Anti-Patterns

### CLI `download` reaches for a non-existent `connector.downloader`

**What happens:** `src/datasluice/cli/download.py:37` does `cast("Any", connector).downloader.download_many(resources, dest)`, but `BaseAdapter` (`src/datasluice/connectors/base.py`) declares no `downloader` attribute and no built-in connector sets one.
**Why it's wrong:** The `download` command will raise `AttributeError` at runtime for every portal — the typed contract (`BaseAdapter`) and the CLI's expectation are out of sync. The `cast("Any", ...)` silences the type checker that would otherwise have caught this.
**Do this instead:** Either add a `downloader` (or `Downloader` accessor) to `BaseAdapter`/`ConnectorContext`, or have the CLI construct a `Downloader` from the session's transport explicitly: `Downloader(transport=session._transport, storage=LocalStorage(dest))`.

### `detect_portal_type` bypasses dependency injection

**What happens:** `src/datasluice/discovery/detector.py:42-46` constructs its own `HttpClient()` and a fresh `PluginManager()` inside the function body instead of receiving them as parameters.
**Why it's wrong:** It ignores the caller's configured transport (auth, retry, rate-limit, proxy) and re-scans entry points on every call. It also couples the discovery layer to concrete transports, undermining the hexagonal boundary.
**Do this instead:** Pass `transport` and `plugin_manager` (or the `DataSluiceSession`) into `detect_portal_type`, mirroring how `session.portal()` already calls detection — refactor so detection reuses the session's injected infra.

### Two storage abstractions coexist

**What happens:** `src/datasluice/io/storage.py` defines a `Storage` ABC with `LocalStorage`, while `src/datasluice/ports/storage.py` defines a separate `StoragePort` Protocol. `Downloader` (`src/datasluice/io/downloader.py`) types against the concrete `Storage` ABC, not the `StoragePort` Protocol.
**Why it's wrong:** Two contracts for the same concept invite drift; the ABC approach also forfeits structural `isinstance` checks and forces third parties to inherit rather than just satisfy the Protocol.
**Do this instead:** Consolidate on `StoragePort` (`src/datasluice/ports/storage.py`); make `LocalStorage`/`FsspecStorage` structurally satisfy it and type `Downloader` against `StoragePort`.

## Error Handling

**Strategy:** A single rooted exception hierarchy in `src/datasluice/exceptions.py`.

```
DataSluiceError
├── PortalError                 (portal returned error / unreachable)
│   ├── RateLimitError          (HTTP 429; carries retry_after)
│   ├── RetryableHTTPError      (HTTP 5xx; carries status_code)
│   └── NotFoundError
├── AdapterError
│   └── AdapterNotFoundError    (no connector registered)
├── PortalDetectionError        (cannot auto-detect type)
├── AuthenticationError
├── DownloadError
│   └── ChecksumMismatchError   (carries expected/actual)
├── FormatError                 (parse failure / missing optional dep)
└── ConfigError
```

**Patterns:**
- Transport maps HTTP status codes to typed exceptions: 429 → `RateLimitError`, ≥500 → `RetryableHTTPError`, other 4xx → `PortalError` (`src/datasluice/transport/http_client.py:133`, `httpx_transport.py:243`).
- `RetryPolicy`/`with_retry` (`src/datasluice/transport/retry.py`) retries on `RetryableHTTPError` and `RateLimitError` (honoring `Retry-After`).
- Optional-dep absence raises `FormatError`/`ImportError` with an actionable install hint (e.g. `pip install datasluice[parquet]`) — see `src/datasluice/formats/parquet.py:24`.
- `PluginManager` never raises on a broken third-party entry point; it records a `PluginFailure` and logs a warning (`src/datasluice/runtime/plugin_manager.py:48`).

## Cross-Cutting Concerns

**Logging:** `src/datasluice/logging.py` — module-level `get_logger(name)` returning configured loggers; `configure_logging(level)` called once by `DataSluiceSession.__init__` using `DEFAULT_LOG_LEVEL`. A redacting filter is tested by `tests/unit/test_redacting_filter.py`.

**Validation:** Lightweight and ad-hoc inside domain dataclasses and mappers (e.g. `Resource.normalize_format`). No external validation library (no pydantic); frozen dataclasses enforce structural validity at construction.

**Authentication:** Pluggable via `BaseAuth` ABC (`src/datasluice/auth/base.py`) with strategies `NoAuth`, `APIKeyAuth`, `BearerAuth`, `BasicAuth`, `HeadersAuth`. Applied per-request by `HttpClient.request`/`HttpxTransport.request` via `auth.apply(headers, params)`. Redirect-time credential stripping is governed by `CredentialScope` (`src/datasluice/domain/credentials.py`) via `CredentialAwareRedirectHandler` (urllib) and `_should_strip_authorization` (httpx).

**User-Agent:** Built once by `build_user_agent()` (`src/datasluice/transport/user_agent.py`) and attached to every request.

---

*Architecture analysis: 2026-07-26*
