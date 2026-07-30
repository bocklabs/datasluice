<!-- refreshed: 2026-07-30 -->
# Architecture

**Analysis Date:** 2026-07-30

## System Overview

DataSluice is a Python library + Typer CLI for discovering, reading, normalizing,
and synchronizing open-data from heterogeneous portal platforms (CKAN, data.gouv,
Socrata). The architecture is a **ports-and-adapters (hexagonal) design** with a
strict lazy-import discipline so that `import datasluice` succeeds on a bare
install (only `typer` + `rich` are hard dependencies). Every optional dependency
(httpx, pyarrow, pandas, polars, dlt, duckdb, fsspec, openpyxl, zstandard,
apache-airflow) is imported lazily inside function bodies or resolved via PEP 562
`__getattr__` module-level hooks.

```text
┌──────────────────────────────────────────────────────────────────────┐
│                          ENTRY POINTS                                 │
│   CLI (Typer)              Programmatic API                           │
│  `src/datasluice/cli/`     `datasluice.DataSluiceSession`             │
│  app.py / search /         (public re-export of                       │
│  inspect / download /      `runtime.session.DataSluiceSession`)       │
│  detect                                                                │
└───────────────┬──────────────────────┬────────────────────────────────┘
                │                      │
                ▼                      ▼
┌──────────────────────────────────────────────────────────────────────┐
│                  COMPOSITION ROOT (runtime/)                          │
│  `runtime/session.py:DataSluiceSession`  — public facade             │
│  `runtime/plugin_manager.py:PluginManager` — entry-point discovery   │
│  `runtime/context.py:ConnectorContext`    — DI carrier               │
│  `runtime/defaults.py:create_default_transport` — transport factory   │
└──────┬───────────────────────────────────┬───────────────────────────┘
       │                                   │
       ▼                                   ▼
┌─────────────────────────────┐  ┌─────────────────────────────────────┐
│   PORTS (boundary Protocols) │  │   CONNECTORS (portal adapters)       │
│  `src/datasluice/ports/`     │  │  `src/datasluice/connectors/`        │
│  Transport, StreamingTrans,  │  │  ckan/  datagouv/  socrata/          │
│  ConditionalTransport,       │  │  base.py:BaseAdapter (ABC)           │
│  CatalogPort, Searchable-    │  │  _reject.py (pre-flight Query gate)  │
│  Catalog, OrganizationCat,   │  │  Each adapter: adapter.py,           │
│  ResourceReader, StateStore, │  │    mapper.py, pagination.py,         │
│  StoragePort, CachePort,     │  │    errors.py, factory.py             │
│  CredentialProvider,         │  │  (factories registered as             │
│  PortalDetector              │  │   `datasluice.connectors` entry pts) │
└─────────────────────────────┘  └─────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        INFRASTRUCTURE LAYER                           │
│  `transport/`  HttpClient (urllib) + HttpxTransport (httpx, default)  │
│                retry.py, rate_limit.py, redirect.py, user_agent.py    │
│                httpx_transport.py: StreamResponse + conditional_fetch  │
│  `auth/`       BaseAuth ABC + NoAuth/APIKey/Bearer/Basic/Headers      │
│  `credentials/`HostCredentialProvider (single-flight refresh)         │
│  `discovery/`  detect() + fingerprints.py (PATH/HTML probes)          │
│  `io/`         downloader, FileCache, ContentCache (SQLite WAL),      │
│                filesystem.open_filesystem, LocalStorage/FsspecStorage │
└──────────────────────────────────────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        DATA PLANE (Arrow)                             │
│  `data/`       BatchStream (ctx-mgr RecordBatch stream), access.py    │
│                DataPlaneResourceReader (access-kind dispatch),        │
│                compression.py (GZIP/BZIP2/ZSTD/ZIP),                  │
│                _byte_source.py:IterableBytesIO,                       │
│                readers/: CSV/JSON/Parquet/GeoJSON/XLSX                │
│  `transforms/` TransformStep Protocol + Pipeline + compose()          │
│                steps: SelectColumns, RenameColumns, CastSchema,       │
│                NormalizeTimestamps, Filter, Flatten                   │
└──────────────────────────────────────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────────────────────────┐
│            SYNC + TERMINAL INTEGRATIONS                               │
│  `sync/`       sync_resources, materialize/materialize_checkpointed,  │
│                state_store.py:FileStateStore/InMemoryStateStore,      │
│                _hashing.logical_sha256                                │
│  `integrations/` to_arrow (substrate), to_pandas, to_polars,          │
│                  to_duckdb, dlt.datasluice_source, airflow.Operator   │
└──────────────────────────────────────────────────────────────────────┘
```

## Component Responsibilities

| Component | Responsibility | File |
|-----------|----------------|------|
| `DataSluiceSession` | Public facade + composition root; wires plugins/transport/auth/state/cache | `src/datasluice/runtime/session.py` |
| `PluginManager` | Entry-point-based connector discovery (no module singleton) | `src/datasluice/runtime/plugin_manager.py` |
| `ConnectorContext` | Frozen DI carrier (base_url, transport, auth, page_size) passed to factories | `src/datasluice/runtime/context.py` |
| `create_default_transport` | Picks `HttpxTransport` when importable, else urllib `HttpClient` | `src/datasluice/runtime/defaults.py` |
| Domain models | Portal-agnostic frozen dataclasses (Dataset, Resource, Query, etc.) | `src/datasluice/domain/*.py` |
| Ports | `runtime_checkable` Protocol boundary contracts | `src/datasluice/ports/*.py` |
| `BaseAdapter` | ABC every connector implements (search/get_dataset/list_resources) | `src/datasluice/connectors/base.py` |
| Per-portal adapter | Translate portal-native JSON → domain models | `src/datasluice/connectors/<portal>/adapter.py` |
| Per-portal mapper | Pure JSON→dataclass mapping functions | `src/datasluice/connectors/<portal>/mapper.py` |
| Per-portal factory | Entry-point target; `create_*_connector(ctx) -> Adapter` | `src/datasluice/connectors/<portal>/factory.py` |
| `_reject_unsupported_fields` | Pre-flight `Query` field validation gate (runs before transport) | `src/datasluice/connectors/_reject.py` |
| `detect` | Probe portal endpoints → `DetectionResult` with evidence trail | `src/datasluice/discovery/detector.py` |
| `HttpClient` / `HttpxTransport` | Concrete `Transport` impls (urllib fallback / httpx default) | `src/datasluice/transport/{http_client,httpx_transport}.py` |
| `DataPlaneResourceReader` | Open a `Resource` as a `BatchStream` (access-kind dispatch) | `src/datasluice/data/access.py` |
| `BatchStream` | Context-managed Arrow `RecordBatch` stream + `__arrow_c_stream__` | `src/datasluice/data/batch_stream.py` |
| Format readers | Decode bytes → `Iterator[RecordBatch]` (CSV/JSON/Parquet/etc.) | `src/datasluice/data/readers/*.py` |
| `apply_compression` | Magic-byte peek + transparent decompression decorator | `src/datasluice/data/compression.py` |
| `Pipeline` / `compose` | Thread `TransformStep`s over a `BatchStream` → new `BatchStream` | `src/datasluice/transforms/pipeline.py` |
| `sync_resources` | Checkpointed incremental sync (ETag/Last-Modified + Parquet row-group) | `src/datasluice/sync/sync.py` |
| `materialize` | Idempotent write of resource → fsspec destination (parquet/raw) | `src/datasluice/sync/materialize.py` |
| `FileStateStore` / `InMemoryStateStore` | Durable / ephemeral `StateStore` implementations | `src/datasluice/sync/state_store.py` |
| Terminal integrations | `to_arrow`/`to_pandas`/`to_polars`/`to_duckdb`/dlt/airflow | `src/datasluice/integrations/*.py` |

## Pattern Overview

**Overall:** Ports-and-adapters (hexagonal) with capability-probe `Protocol`s,
plugin discovery via `importlib.metadata` entry points, and a single composition
root (`DataSluiceSession`). Inside the hexagon, the data plane is a lazy Arrow
pipeline (acquire bytes → decompress → decode → transform → materialize/expose).

**Key Characteristics:**
- **Boundary Protocols over classes.** `src/datasluice/ports/*.py` defines
  `@runtime_checkable Protocol`s. Adapters satisfy them *structurally*; callers
  probe capabilities with `isinstance(x, StreamingTransport)` rather than
  backend-specific types.
- **Entry-point plugin discovery, no module singleton.** `PluginManager` loads
  factories from the `datasluice.connectors` entry-points group in
  `pyproject.toml`; a broken third-party plugin is recorded as a
  `PluginFailure` and never crashes session creation.
- **Lazy imports everywhere.** Heavy optional deps (pyarrow, httpx, pandas,
  dlt, duckdb, fsspec, openpyxl, zstandard) are imported inside function
  bodies. Package `__init__.py` files for `data/`, `transforms/`, `sync/`,
  `io/`, `transport/` use PEP 562 `__getattr__` so attribute access is the
  import trigger.
- **Frozen dataclasses for value objects.** Every domain model is
  `@dataclass(frozen=True)`. Mutable state lives only in the session, stores,
  and transport client.
- **Pre-flight reject gates.** Catalog adapters call `_reject_unsupported_fields`
  at the top of `search()` so an unsupported `Query` filter raises
  `UnsupportedQueryFieldError` *before* any transport call.
- **Single-substrate terminals.** `to_pandas`/`to_polars`/`to_duckdb` all
  delegate through `to_arrow` for consistency.

## Layers

**Domain Layer (`src/datasluice/domain/`):**
- Purpose: Portal-agnostic, dependency-free value objects.
- Location: `src/datasluice/domain/*.py`
- Contains: Frozen dataclasses — `Dataset`, `Resource`, `Organization`,
  `License`, `Query`, `SearchResult`, `Schema`, `SyncState`, `DetectionResult`,
  `CatalogCapabilities`, `CredentialScope`, plus the `ResourceAccess` sum-type
  family (`HttpDownload`, `ObjectStorage`, `QueryAccess`, `StreamAccess`,
  `LocalFile`) in `src/datasluice/domain/access.py`.
- Depends on: nothing (stdlib only).
- Used by: every other layer.

**Ports Layer (`src/datasluice/ports/`):**
- Purpose: `runtime_checkable Protocol` boundary contracts — the *only* types
  crossing the hexagon boundary.
- Location: `src/datasluice/ports/*.py`
- Contains: `Transport`, `StreamingTransport`, `ConditionalTransport`,
  `CatalogPort`, `SearchableCatalog`, `OrganizationCatalog`, `ResourceReader`,
  `CheckpointableResourceReader`, `StateStore`, `StoragePort`, `CachePort`,
  `CredentialProvider`, `PortalDetector`. Plus `ConditionalFetchResult`
  (frozen dataclass).
- Depends on: `domain/` (TYPE_CHECKING only).
- Used by: `runtime/`, `connectors/`, `transport/`, `sync/`, `data/`.

**Runtime / Composition Root (`src/datasluice/runtime/`):**
- Purpose: Wire injected infra into a zero-config facade.
- Location: `src/datasluice/runtime/session.py` (and `context.py`,
  `defaults.py`, `plugin_manager.py`).
- Contains: `DataSluiceSession`, `ConnectorContext`, `PluginManager`,
  `create_default_transport`.
- Depends on: `ports/`, `auth/`, `config/`, `discovery/`, `sync/`.
- Used by: `cli/`, public `datasluice.__init__`, `integrations/dlt.py`.

**Connectors Layer (`src/datasluice/connectors/`):**
- Purpose: Translate one portal platform's API into domain models.
- Location: `src/datasluice/connectors/<portal>/{adapter,mapper,pagination,errors,factory}.py`.
- Contains: `BaseAdapter` ABC + 3 built-ins: `ckan`, `datagouv`, `socrata`.
  Each adapter declares a `capabilities: ClassVar[CatalogCapabilities]`.
- Depends on: `domain/`, `ports/`, `connectors/_reject.py`, `transport/` (lazy).
- Used by: `PluginManager` resolves factories; `DataSluiceSession.portal()`.

**Transport Layer (`src/datasluice/transport/`):**
- Purpose: HTTP execution satisfying the `Transport` / `StreamingTransport` /
  `ConditionalTransport` Protocols.
- Location: `src/datasluice/transport/http_client.py` (urllib, fallback),
  `httpx_transport.py` (httpx, default), plus `retry.py`, `rate_limit.py`,
  `redirect.py`, `user_agent.py`, `pagination.py`.
- Contains: `HttpClient`, `HttpxTransport`, `StreamResponse`,
  `RetryPolicy`/`with_retry`, `RateLimiter`, `CredentialAwareRedirectHandler`,
  `build_user_agent`, `PaginationConfig`/`paginate`.
- Depends on: `auth/`, `config/`, `exceptions.py`, `logging.py`.
- Used by: `runtime/defaults.py`, `discovery/detector.py`, `data/access.py`,
  `sync/sync.py`.

**Discovery Layer (`src/datasluice/discovery/`):**
- Purpose: Auto-detect portal platform type by probing well-known endpoints.
- Location: `src/datasluice/discovery/detector.py`, `fingerprints.py`,
  `portal_metadata.py`.
- Contains: `detect(url, transport, plugin_manager) -> DetectionResult`,
  `PATH_FINGERPRINTS`, `HTML_FINGERPRINTS`, `PortalMetadata`.
- Depends on: `ports/`, `runtime/plugin_manager.py`, `domain/detection.py`.
- Used by: `runtime/session.py:DataSluiceSession.portal()`, `cli/detect.py`.

**Data Plane (`src/datasluice/data/`):**
- Purpose: Acquire bytes → decompress → decode → yield Arrow `RecordBatch`.
- Location: `src/datasluice/data/access.py` (dispatch), `batch_stream.py`,
  `compression.py`, `_byte_source.py`, `schema.py`, `readers/`.
- Contains: `BatchStream`, `BatchCursor`, `ParquetRowGroupPosition`,
  `DataPlaneResourceReader`, `IterableBytesIO`, `apply_compression`,
  `PeekableReader`, and readers `CSVReader`/`JSONReader`/`ParquetReader`/
  `GeoJSONReader`/`XLSXReader` + `get_reader` registry.
- Depends on: `ports/`, `domain/`, `io/filesystem.py`, `exceptions.py` (lazy
  pyarrow inside methods).
- Used by: `sync/`, `integrations/`, `runtime/session.py:sync_resources`.

**Transforms Layer (`src/datasluice/transforms/`):**
- Purpose: Closed-set normalization pipeline over `RecordBatch` iterators.
- Location: `src/datasluice/transforms/protocol.py`, `pipeline.py`, `steps.py`.
- Contains: `TransformStep` Protocol, `TransformContext` (frozen),
  `Pipeline`, `compose`, and steps `Filter`, `SelectColumns`, `RenameColumns`,
  `CastSchema`, `NormalizeTimestamps`, `Flatten`.
- Depends on: `data/batch_stream.py` (lazy pyarrow inside `apply`).
- Used by: callers composing a normalization pipeline before terminal export.

**Sync Layer (`src/datasluice/sync/`):**
- Purpose: Incremental, checkpointed resource synchronization to fsspec URIs.
- Location: `src/datasluice/sync/sync.py`, `materialize.py`, `state_store.py`,
  `_hashing.py`.
- Contains: `sync_resources`, `SyncOutcome`, `materialize`,
  `materialize_checkpointed`, `FileStateStore`, `InMemoryStateStore`,
  `logical_sha256`.
- Depends on: `ports/`, `domain/`, `data/`, `io/filesystem.py`,
  `integrations/arrow.py` (lazy pyarrow).
- Used by: `runtime/session.py:sync_resources`, `integrations/dlt.py`.

**Integrations Layer (`src/datasluice/integrations/`):**
- Purpose: Terminal export to downstream data ecosystems.
- Location: `src/datasluice/integrations/{arrow,pandas,polars,duckdb,dlt,airflow}.py`.
- Contains: `to_arrow` (shared substrate), `to_pandas`, `to_polars`,
  `to_duckdb`, `datasluice_source` (dlt), `DataSluiceOperator` (Airflow).
- Depends on: `data/`, `runtime/session.py` (dlt only). All heavy deps lazy.
- Used by: end users; not imported by the library core.

**IO Layer (`src/datasluice/io/`):**
- Purpose: Local/remote byte storage, caching, checksums, downloading.
- Location: `src/datasluice/io/{storage,local,cache,content_cache,downloader,filesystem,fsspec_storage,checksums}.py`.
- Contains: `Storage` ABC + `LocalStorage`/`FsspecStorage`, `FileCache`,
  `ContentCache` (SQLite WAL), `Downloader`, `open_filesystem`,
  `compute_hash`/`compute_sha256`/`compute_md5`/`verify_checksum`.
- Depends on: `transport/`, `exceptions.py`, `config/` (lazy fsspec).
- Used by: `data/access.py`, `sync/`, `runtime/session.py`.

**Cross-cutting:**
- `src/datasluice/auth/` — `BaseAuth` ABC + 5 strategies.
- `src/datasluice/credentials/host_provider.py` — `HostCredentialProvider`.
- `src/datasluice/contracts/` — `run_contract_suite` conformance harness.
- `src/datasluice/config/defaults.py` — `DEFAULT_*` constants.
- `src/datasluice/exceptions.py` — single exception hierarchy rooted at
  `DataSluiceError`.
- `src/datasluice/logging.py` — `get_logger`, `RedactingFilter`,
  `configure_logging`, `SENSITIVE_HEADERS`.

## Data Flow

### Primary Request Path: discover → resolve → search

1. User calls `DataSluiceSession.portal(url)` (`src/datasluice/runtime/session.py:178`).
2. Unless `portal_type=` override is given, `detect(url, transport, plugins)`
   probes `PATH_FINGERPRINTS` endpoints through the session transport
   (`src/datasluice/discovery/detector.py:48`). Each probe is recorded as
   `DetectionEvidence`; first hit pins `portal_type` at confidence 1.0.
3. `PluginManager.get(portal_type)` resolves the registered factory
   (`src/datasluice/runtime/plugin_manager.py:65`).
4. A `ConnectorContext(base_url, transport, auth, page_size)` is built and the
   factory `create_*_connector(ctx)` constructs the adapter wired to the
   session's transport/auth (`src/datasluice/connectors/<portal>/factory.py`).
5. Caller invokes `adapter.search(query)` → `_reject_unsupported_fields` runs
   pre-flight → transport `get_json` → `mapper.map_dataset` translates each
   portal package into a `Dataset` → returns `SearchResult`
   (`src/datasluice/connectors/<portal>/adapter.py`).

### Read Path: resource → bytes → Arrow → terminal

1. `DataPlaneResourceReader.open(resource)` resolves `resource.access` (default
   `HttpDownload(url=resource.url)`) (`src/datasluice/data/access.py:93`).
2. Access-kind dispatch acquires a byte source:
   - `http_download` → `StreamingTransport.stream()` wrapped in
     `IterableBytesIO` (or buffered `BytesIO` fallback for urllib)
     (`src/datasluice/data/access.py:251`).
   - `object_storage` → `open_filesystem(uri).open(path)`
     (`src/datasluice/data/access.py:286`).
   - `local_file` → `open(path, "rb")`.
   - `query`/`stream` → raise `UnsupportedAccessError`.
3. `apply_compression(source, content_encoding)` peeks magic bytes and wraps in
   the right decompressor (`src/datasluice/data/compression.py:277`).
4. `get_reader(resource.format)` selects the format reader; `read_batches`
   yields `RecordBatch` (`src/datasluice/data/readers/__init__.py:33`).
5. Output is wrapped in a `BatchStream` exposing `.schema`, `.iter_batches()`,
   `.iter_batches_with_cursors()`, and `__arrow_c_stream__`
   (`src/datasluice/data/batch_stream.py`).
6. Optional: `Pipeline(steps).run(stream)` threads `TransformStep`s and returns
   a NEW `BatchStream` whose schema reflects the post-transform batches
   (`src/datasluice/transforms/pipeline.py:40`).
7. Terminal: `to_arrow`/`to_pandas`/`to_polars`/`to_duckdb` materialize the
   stream (`src/datasluice/integrations/*.py`).

### Sync Path: incremental materialization

1. `DataSluiceSession.sync_resources(resources, destination_uri=...)` builds a
   `DataPlaneResourceReader` and delegates to `sync_resources`
   (`src/datasluice/runtime/session.py:235`).
2. For each resource, `sync_resources` consults the `StateStore` for a prior
   watermark/checkpoint (`src/datasluice/sync/sync.py:33`).
3. HTTP resources with a non-SHA256 watermark use
   `ConditionalTransport.conditional_fetch` (ETag/Last-Modified); a 304 yields
   `SyncOutcome(action="skipped-unchanged")` without reading bytes
   (`src/datasluice/sync/sync.py:69`).
4. Otherwise `materialize` (or `materialize_checkpointed` for resumable
   Parquet) opens the reader, computes `logical_sha256`, writes to a temp file
   and atomically `mv`s into place at the fsspec destination
   (`src/datasluice/sync/materialize.py:15`).
5. The new watermark/checkpoint is persisted via `state_store.put` BEFORE the
   `SyncOutcome` is yielded (checkpoint-then-emit ordering).

**State Management:**
- The only mutable session state is `DataSluiceSession`'s injected ports
  (`_transport`, `_cache`, `state_store`, `_credential_provider`, `auth`,
  `storage`, `plugins`).
- `InMemoryStateStore` (default) holds a plain dict; `FileStateStore` writes
  SHA-256-named JSON envelopes with detection-only optimistic CAS.
- All domain models and `ConnectorContext`/`TransformContext` are frozen.

## Key Abstractions

**`Transport` / `StreamingTransport` / `ConditionalTransport` Protocols:**
- Purpose: HTTP execution boundary, split so a transport advertises only the
  capabilities it implements.
- Examples: `src/datasluice/ports/transport.py`. `HttpClient` satisfies only
  `Transport`; `HttpxTransport` satisfies all three.
- Pattern: `@runtime_checkable Protocol` + `isinstance` capability probes.

**`ResourceAccess` sum-type family:**
- Purpose: Describe how a `Resource` is reached, discriminated by `.kind`
  (`"http_download"`, `"object_storage"`, `"query"`, `"stream"`, `"local_file"`).
- Examples: `src/datasluice/domain/access.py`. Dispatch lives in
  `DataPlaneResourceReader.open` (`src/datasluice/data/access.py:117`).
- Pattern: Frozen-dataclass inheritance with a `kind` discriminator string
  (avoids `isinstance` chains in the reader).

**`BatchStream`:**
- Purpose: Backend-agnostic, context-managed Arrow `RecordBatch` stream — the
  single data-plane currency. Wraps either a `pa.RecordBatchReader` or a bare
  iterator.
- Examples: `src/datasluice/data/batch_stream.py`. Implements
  `__arrow_c_stream__` for zero-copy interop.
- Pattern: Composition over inheritance (never subclass `pa.RecordBatchReader`).

**`TransformStep` Protocol:**
- Purpose: Closed-set normalization contract (`apply(batches, context) ->
  Iterator[RecordBatch]`). NOT a third-party extension point.
- Examples: `src/datasluice/transforms/protocol.py`; steps in
  `src/datasluice/transforms/steps.py`.
- Pattern: Frozen-dataclass-configured classes + generator `apply`.

**`CatalogCapabilities`:**
- Purpose: Per-connector ClassVar declaring which `Query` filter fields and
  capabilities (search/organizations/facets) a portal honors.
- Examples: `src/datasluice/domain/capabilities.py`; declared on every adapter.
- Pattern: Frozen dataclass published as `ClassVar`, consumed by
  `_reject_unsupported_fields` and the conformance suite.

**`StateStore` Protocol + `SyncState`:**
- Purpose: Persist incremental sync watermarks/Parquet checkpoints.
- Examples: `src/datasluice/ports/state_store.py`,
  `src/datasluice/sync/state_store.py`.
- Pattern: Protocol port + two dep-free concrete impls; JSON envelopes with
  schema validation on every write.

## Entry Points

**CLI (`datasluice` console script):**
- Location: `src/datasluice/cli/app.py:app` (Typer app, registered in
  `pyproject.toml` `[project.scripts]`).
- Triggers: `datasluice search|inspect|download|detect`, `--version`.
- Responsibilities: parse args, build a `DataSluiceSession`, render Rich tables.

**Programmatic API (`DataSluiceSession`):**
- Location: `src/datasluice/runtime/session.py`; re-exported from
  `src/datasluice/__init__.py`.
- Triggers: `import datasluice; datasluice.DataSluiceSession()`.
- Responsibilities: `portal(url)`, `search(url, query)`, `sync_resources(...)`.

**Connector entry points (`datasluice.connectors` group):**
- Location: `pyproject.toml` `[project.entry-points."datasluice.connectors"]`.
  - `ckan = datasluice.connectors.ckan.factory:create_ckan_connector`
  - `datagouv = datasluice.connectors.datagouv.factory:create_datagouv_connector`
  - `socrata = datasluice.connectors.socrata.factory:create_socrata_connector`
- Triggers: `PluginManager.__init__` eagerly loads via `importlib.metadata`.
- Responsibilities: each factory takes a `ConnectorContext` and returns a
  `BaseAdapter` wired to the injected transport/auth.

## Architectural Constraints

- **Python version:** `>= 3.12` (PEP 695 type params, `type X = ...` aliases).
  CI matrix: 3.12, 3.13, 3.14.
- **Threading:** Single-threaded by default. `HostCredentialProvider` uses a
  per-host `threading.Lock` for single-flight refresh; `HttpxTransport`'s
  underlying `httpx.Client` is thread-safe and connection-pooled.
- **Lazy-import invariant:** `import datasluice` MUST NOT pull any optional
  dependency. Enforced by PEP 562 `__getattr__` in `data/`, `transforms/`,
  `sync/`, `io/`, `transport/` `__init__.py` files and by importing heavy deps
  inside function bodies. The CI `type-check` job uses
  `uv run --all-extras ty check .` to resolve lazy imports.
- **No env-var settings system.** `DataSluiceSession` takes explicit kwargs
  only (the legacy `Settings`/`DataSluice` env-var system was removed, D-14).
  The only env vars read are `DATASLUICE_NO_REDACT` (logging escape hatch) and
  those consumed by fsspec backends.
- **No module-level singletons for connectors.** `PluginManager` is an injected
  instance, never a global (ARCH-06). The legacy `AdapterRegistry` singleton
  was removed.
- **Global state:** Module-level constants in `src/datasluice/config/defaults.py`
  and the `SENSITIVE_HEADERS` frozenset in `src/datasluice/logging.py` (shared
  with `transport/redirect.py`). No module-level mutable singletons.
- **Circular imports:** `datasluice/_version.py` is a separate module to break
  a circular import with `transport/user_agent.py` — do NOT move it into
  `__init__.py`. `runtime/session.py` uses deferred (`importlib`/function-local)
  imports for `discovery.detect`, `sync.sync_resources`,
  `data.access.DataPlaneResourceReader`, and `sync.state_store.InMemoryStateStore`.
- **Hard dependencies:** Only `typer` and `rich` (`pyproject.toml`
  `[project.dependencies]`). Everything else is an optional extra.
- **Coverage gate:** 50% (`fail_under` in `pyproject.toml`).
- **`get_organization` is intentionally NOT on `BaseAdapter`.** It lives only
  on the `OrganizationCatalog` Protocol so `runtime_checkable` `isinstance`
  does not short-circuit on the base class (python/typing#800). Socrata
  therefore correctly fails `isinstance(adapter, OrganizationCatalog)`.

## Anti-Patterns

### Leaking optional dependencies at import time

**What happens:** Importing pyarrow/httpx/pandas at module top level in
`data/`, `transforms/`, `integrations/`, or `transport/` would make
`import datasluice` fail on a bare install (only `typer`+`rich` installed).
**Why it's wrong:** Breaks the zero-config install contract; the CI
`type-check` job and `smoke-test` job both depend on lazy resolution.
**Do this instead:** Import inside the function body, or expose the symbol via
a PEP 562 `__getattr__` in the package `__init__.py` (see
`src/datasluice/data/__init__.py:18`, `src/datasluice/transport/__init__.py:31`).

### Declaring capability methods on `BaseAdapter`

**What happens:** Adding `get_organization` as an `@abstractmethod` (or default)
on `BaseAdapter` makes every adapter structurally satisfy `OrganizationCatalog`
under PEP 544 `runtime_checkable`.
**Why it's wrong:** Socrata has no organizations endpoint; advertising one via a
stub would be a lying capability, and the `isinstance` check would wrongly pass.
**Do this instead:** Keep capability methods ONLY on the dedicated Protocol
(`src/datasluice/ports/catalog.py:OrganizationCatalog`); implement them on the
adapter only when the portal truly supports them (see `src/datasluice/connectors/base.py:21`).

### Hand-rolling Arrow compute

**What happens:** Writing custom Python loops to filter/cast/flatten Arrow
batches.
**Why it's wrong:** Forfeits pyarrow's vectorized, zero-copy primitives and
introduces correctness bugs.
**Do this instead:** Delegate every hard operation to `pyarrow.compute` (see
every step in `src/datasluice/transforms/steps.py` — `Filter` uses
`RecordBatch.filter`, `CastSchema` uses `Table.cast(safe=True)`, etc.).

### Sending unsupported `Query` filters to the portal

**What happens:** Translating an unsupported filter field into a portal param
the portal silently ignores (Socrata's nonexistent `sort` zeroes the result set).
**Why it's wrong:** The caller believes their filter was applied; results are
silently wrong.
**Do this instead:** Call `_reject_unsupported_fields(query, capabilities,
portal_name)` at the top of every `search()` BEFORE any transport call
(`src/datasluice/connectors/_reject.py:34`, used in every adapter).

### Buffered reads when streaming is available

**What happens:** Using `transport.download()` (full body into memory) when the
transport satisfies `StreamingTransport`.
**Why it's wrong:** Breaks the bounded-memory contract for large open-data files.
**Do this instead:** Probe `isinstance(transport, StreamingTransport)` and wrap
`transport.stream(url)` in `IterableBytesIO`; fall back to buffered only when
the probe fails and log a WARNING recommending `datasluice[http]`
(`src/datasluice/data/access.py:251`).

## Error Handling

**Strategy:** Single exception hierarchy rooted at `DataSluiceError`
(`src/datasluice/exceptions.py`). Every public error type is a subclass;
callers can catch `DataSluiceError` for a library-wide boundary.

**Patterns:**
- **Transport errors** → `PortalError` and subclasses: `RateLimitError`
  (HTTP 429, carries `retry_after`), `RetryableHTTPError` (HTTP 5xx, carries
  `status_code`), `NotFoundError` (404). See `src/datasluice/transport/http_client.py:133`
  and `httpx_transport.py:257`.
- **Retry** → `with_retry(fn, RetryPolicy)` wraps each attempt; 429 honors
  `Retry-After` (`src/datasluice/transport/retry.py`).
- **Pre-flight reject** → `UnsupportedQueryFieldError` (sibling of
  `AdapterError`, NOT under `PortalError` because no portal contact occurred)
  lists supported alternatives in the message
  (`src/datasluice/exceptions.py:154`).
- **State-store corruption** → `StateStoreError` fails loud (never treats
  corrupt state as "no state"); `SyncStateConflictError` for lost CAS races.
- **Decompression** → `DecompressionError` (subclass of `FormatError`).
- **Transform** → `TransformError` (subclass of `FormatError`), names missing
  columns AND available ones.
- **Detection** → `PortalDetectionError` carries the `DetectionResult`
  evidence trail as `.detection_result` so callers can surface why detection
  failed without re-running it.
- **Plugin load failures** → recorded as `PluginFailure(name, error)`, never
  raised; queryable via `PluginManager.list_failures()`.

## Cross-Cutting Concerns

**Logging:** `src/datasluice/logging.py` — `get_logger(name)` returns
`logging.getLogger("datasluice.<name>")`. `configure_logging` attaches a
`StreamHandler` with a `RedactingFilter` that scrubs known sensitive keys
(`authorization`, `cookie`, `x-api-key`, `token`, `secret`, `password`, …) by
*key name* only (never value-pattern heuristics, so legitimate base64/open-data
payloads pass through). `DATASLUICE_NO_REDACT=1` disables redaction.

**Validation:** Domain models validate at construction via `__post_init__`
(e.g. `BatchCursor`, `ParquetRowGroupPosition`). `FileStateStore.put` runs
`_validate_state_for_write` on every write (watermark format, cursor shape,
checkpoint schema). SQL identifiers are regex-validated
(`src/datasluice/integrations/duckdb.py:_validate_table_name`,
SEC-03 boundary). `LocalStorage.write` rejects path traversal.

**Authentication:** `src/datasluice/auth/base.py:BaseAuth.apply(headers,
params) -> (headers, params)`. Five strategies: `NoAuth` (default),
`APIKeyAuth`, `BearerAuth`, `BasicAuth`, `HeadersAuth`. Host-scoped refresh is
`HostCredentialProvider` (`src/datasluice/credentials/host_provider.py`),
plugged into `HttpxTransport` via the `CredentialProvider` port; on 401/403 the
transport evicts and refreshes exactly once (single-flight under a per-host
lock). Object-storage credentials flow through `open_filesystem(uri,
credentials=)` and fsspec's own resolver — never through `BaseAuth`.

**Security:** Manual redirect loop (`follow_redirects=False`) applies a
`CredentialScope` policy per hop, stripping sensitive headers on any cross-host
redirect or `https`→`http` downgrade (`src/datasluice/transport/redirect.py`,
`httpx_transport.py:_should_strip_authorization`). The user-agent is built
once via `build_user_agent()` (`src/datasluice/transport/user_agent.py`).

**Conformance:** `src/datasluice/contracts/checks.py:run_contract_suite` runs
an 8-check matrix against a fixture-served connector; built-in connectors run
it in default CI, third-party authors import and parametrize it.

---

*Architecture analysis: 2026-07-30*
