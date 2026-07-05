<!-- refreshed: 2026-07-05 -->
# Architecture

**Analysis Date:** 2026-07-05

## System Overview

DataSluice is a portal-agnostic open-data toolkit built on a strict layered
architecture. A single `DataSluice` client wires together transport, adapters,
discovery, IO, and format readers behind one consistent interface. All
portal-native JSON is mapped into a shared `datasluice.domain` model surface
before reaching consumers.

```text
┌─────────────────────────────────────────────────────────────────┐
│                         Public API                               │
│                  `src/datasluice/client.py`                      │
│                    `DataSluice` (high-level facade)              │
├──────────────────┬──────────────────┬───────────────────────────┤
│      CLI         │   Integrations   │   Direct library import    │
│ `cli/app.py`     │ `integrations/`  │   `from datasluice import`│
│ (Typer)          │ pandas · polars  │   DataSluice              │
│ search/download/ │ dlt · airflow ·  │                           │
│ inspect/detect   │ duckdb           │                           │
├──────────────────┴──────────────────┴───────────────────────────┤
│                       Adapters Layer                             │
│ `adapters/__init__.py` (auto-registration side-effect import)    │
│ `adapters/base.py` · `adapters/factory.py` · `adapters/registry`│
├───────────────┬────────────────┬──────────────┬─────────────────┤
│    CKAN       │   data.gouv    │   Socrata    │   Custom        │
│ `adapters/    │ `adapters/     │ `adapters/   │ `adapters/      │
│   ckan/`      │   datagouv/`   │   socrata/`  │   custom/`      │
├───────────────┴────────────────┴──────────────┴─────────────────┤
│                      Domain Models                               │
│ `domain/dataset.py` · `domain/resource.py` · `domain/result.py` │
│ `domain/organization.py` · `domain/license.py` · `domain/query` │
├──────────────────────────────────────────────────────────────────┤
│                   Cross-cutting Concerns                         │
│ `transport/` (HTTP·retry·rate·UA·pagination)                     │
│ `auth/`     (none·api_key·bearer·basic·headers)                 │
│ `discovery/` (portal auto-detection via fingerprints)            │
│ `io/`       (downloader·cache·checksums·storage)                 │
│ `formats/`  (csv·json·parquet·xlsx·geojson)                      │
│ `config/`   (settings + defaults from env vars)                  │
└──────────────────────────────────────────────────────────────────┘
```

## Component Responsibilities

| Component | Responsibility | File |
|-----------|----------------|------|
| `DataSluice` | Unified facade; wires transport + adapter + downloader | `src/datasluice/client.py` |
| `BaseAdapter` | Abstract portal protocol (search/get_dataset/list_resources/get_organization) | `src/datasluice/adapters/base.py` |
| `AdapterRegistry` | Maps canonical portal-type names → adapter classes (module singleton `registry`) | `src/datasluice/adapters/registry.py` |
| `create_adapter` | Factory; falls back to discovery when portal_type is None | `src/datasluice/adapters/factory.py` |
| `HttpClient` | urllib wrapper with retry, rate-limiting, auth, UA injection | `src/datasluice/transport/http_client.py` |
| `RetryPolicy` / `with_retry` | Exponential backoff (retries `RateLimitError` and `OSError`) | `src/datasluice/transport/retry.py` |
| `RateLimiter` | Thread-safe token-bucket limiter | `src/datasluice/transport/rate_limit.py` |
| `detect_portal_type` | Probes portal endpoints against `PATH_FINGERPRINTS` | `src/datasluice/discovery/detector.py` |
| `Downloader` | Downloads resources with cache + checksum verification | `src/datasluice/io/downloader.py` |
| `Storage` / `LocalStorage` | Pluggable storage backend (local FS only today) | `src/datasluice/io/storage.py` |
| `FileCache` | TTL-based on-disk cache for raw downloaded bytes | `src/datasluice/io/cache.py` |
| `get_reader` / `READERS` | Format registry → `BaseFormatReader` instances | `src/datasluice/formats/__init__.py` |
| `Settings` / `load_settings` | Resolves env-var-driven configuration | `src/datasluice/config/settings.py` |
| `exceptions.py` | Single exception hierarchy rooted at `DataSluiceError` | `src/datasluice/exceptions.py` |

## Pattern Overview

**Overall:** Layered architecture with a Strategy/Plugin pattern for adapters,
auth, formats, and storage. A Registry + Factory combination resolves
implementations at runtime; side-effect imports auto-register built-in
adapters.

**Key Characteristics:**
- **Portal-agnostic domain models** — frozen dataclasses in `datasluice.domain`; consumers never see portal-native JSON.
- **Adapter isolation** — each portal lives in its own subpackage with `adapter.py` / `mapper.py` / `pagination.py` / `errors.py`.
- **Composable transport** — retry, rate-limiting, and pagination are decorators/helpers applied around `_do_request`, not baked into adapters.
- **Lazy optional imports** — heavy deps (pandas, polars, pyarrow, openpyxl, dlt, duckdb, airflow) are imported inside function bodies, never at module top-level.
- **Side-effect registration** — importing `datasluice.adapters` runs `registry.register(...)` for every built-in adapter.
- **Dependency direction** — outer layers (CLI, integrations) depend on `DataSluice` client; inner layers (domain, transport) depend only on themselves.

## Layers

**Public API / Facade:**
- Purpose: Single entry surface for users.
- Location: `src/datasluice/client.py`, `src/datasluice/__init__.py`, `src/datasluice/cli/app.py`.
- Contains: `DataSluice` class; Typer CLI commands; package re-exports.
- Depends on: adapters, transport, io, formats, config, domain.
- Used by: CLI, integrations, end-user code.

**Adapters Layer:**
- Purpose: Translate each portal's API into domain models.
- Location: `src/datasluice/adapters/` with subpackages `ckan/`, `datagouv/`, `socrata/`, `custom/`.
- Contains: `BaseAdapter` ABC, `AdapterRegistry`, `create_adapter`, per-portal `adapter.py` + `mapper.py` + `pagination.py` + `errors.py`.
- Depends on: domain, transport (lazy via `self.transport`), exceptions.
- Used by: `DataSluice` client, integrations indirectly.

**Domain Layer:**
- Purpose: Portal-agnostic data contracts.
- Location: `src/datasluice/domain/`.
- Contains: frozen dataclasses `Dataset`, `Resource`, `Organization`, `License`, `Query`, `SearchResult`.
- Depends on: nothing (pure dataclasses + stdlib).
- Used by: every other layer; this is the shared vocabulary.

**Transport Layer:**
- Purpose: All HTTP I/O with retry, rate-limiting, auth injection, UA, generic pagination.
- Location: `src/datasluice/transport/`.
- Contains: `HttpClient`, `RetryPolicy` / `with_retry`, `RateLimiter`, `PaginationConfig` / `paginate`, `build_user_agent`.
- Depends on: auth (injected), config.defaults, exceptions, logging.
- Used by: adapters, `DataSluice.read()`, discovery detector.

**Auth Layer:**
- Purpose: Strategy pattern for credential injection.
- Location: `src/datasluice/auth/`.
- Contains: `BaseAuth` ABC + `NoAuth`, `APIKeyAuth`, `BearerAuth`, `BasicAuth`, `HeadersAuth`.
- Depends on: stdlib only.
- Used by: `HttpClient` (composition), `BaseAdapter`, `DataSluice`.

**Discovery Layer:**
- Purpose: Auto-detect portal type from a URL.
- Location: `src/datasluice/discovery/`.
- Contains: `detect_portal_type`, `PATH_FINGERPRINTS`, `HTML_FINGERPRINTS`, `PortalMetadata`.
- Depends on: transport (lazy import), registry (lazy import), exceptions.
- Used by: `adapters/factory.py` when `portal_type` is None.

**IO Layer:**
- Purpose: Download, cache, verify, and persist resource bytes.
- Location: `src/datasluice/io/`.
- Contains: `Downloader`, `FileCache`, `Storage`/`LocalStorage`, `compute_hash`/`compute_sha256`/`compute_md5`/`verify_checksum`, `ensure_dir`/`save_bytes`/`safe_filename`.
- Depends on: transport (injected), domain (TYPE_CHECKING), exceptions.
- Used by: `DataSluice.downloader` / `download()` / `download_all()`.

**Formats Layer:**
- Purpose: Normalize file content into `list[dict[str, Any]]`.
- Location: `src/datasluice/formats/`.
- Contains: `BaseFormatReader` ABC, per-format readers, `READERS` registry, `get_reader` factory.
- Depends on: exceptions (lazy ImportError → `FormatError`), optional heavy deps imported lazily.
- Used by: `DataSluice.read()`, `integrations/pandas.py`.

**Integrations Layer:**
- Purpose: Bridge to external Python data ecosystem.
- Location: `src/datasluice/integrations/`.
- Contains: `pandas.py`, `polars.py`, `dlt.py`, `duckdb.py`, `airflow.py`.
- Depends on: lazy heavy deps; `DataSluice` and domain imported inside functions.
- Used by: end users opting into an integration.

**Config Layer:**
- Purpose: Resolve runtime settings from env vars with defaults.
- Location: `src/datasluice/config/`.
- Contains: `Settings` dataclass, `load_settings`, default constants (`DEFAULT_TIMEOUT`, `DEFAULT_RETRIES`, `DEFAULT_RATE_LIMIT`, `DEFAULT_PAGE_SIZE`, `DEFAULT_CACHE_DIR`, `DEFAULT_CACHE_TTL`, `DEFAULT_LOG_LEVEL`).
- Depends on: exceptions (`ConfigError`).
- Used by: `DataSluice.__init__`, `_build_transport`.

## Data Flow

### Primary Request Path — search → adapter → transport → domain

1. User invokes `DataSluice.search(query)` (`src/datasluice/client.py:85`) — accepts `str | Query | None`.
2. `Query` is constructed and forwarded to `self.adapter.search(q)` (`client.py:99`).
3. Adapter (e.g. `CKANAdapter.search`, `src/datasluice/adapters/ckan/adapter.py:28`) builds portal params via its `pagination.py` dataclass, then calls `self.transport.get_json(url, params=...)`.
4. `HttpClient.request` (`src/datasluice/transport/http_client.py:51`) injects UA, applies `auth.apply(headers, params)`, encodes query string, then runs `_do_request` inside `with_retry`.
5. `_do_request` calls `rate_limiter.acquire()` (if set), opens the URL via `urllib.request.urlopen`, maps HTTP 429 → `RateLimitError`, other non-2xx → `PortalError`.
6. Raw JSON bytes are parsed in `HttpClient.get_json`; adapter maps results through its `mapper.py` functions into `Dataset` / `Resource` / `Organization`.
7. Adapter returns a `SearchResult` (`src/datasluice/domain/result.py`) wrapping the dataset list plus pagination metadata.

### Read-and-Parse Flow — read resource into records

1. `DataSluice.read(resource)` (`src/datasluice/client.py:113`) — lazily imports `get_reader` from `datasluice.formats`.
2. Raw bytes fetched via `self._transport.download(resource.url)`.
3. Format reader resolved via `get_reader(fmt)` lookup in `READERS` (`src/datasluice/formats/__init__.py:22`).
4. Reader (e.g. `CSVReader.read`, `src/datasluice/formats/csv.py:32`) normalizes into `list[dict[str, Any]]`.

### Download Flow — persist resource to disk/storage

1. `DataSluice.download(resource, dest)` (`src/datasluice/client.py:124`) lazily builds `self.downloader` (`Downloader`).
2. `Downloader.download` (`src/datasluice/io/downloader.py:41`) checks `FileCache.get(cache_key=resource.url)` if a cache is configured.
3. On miss, `transport.download(resource.url)` fetches bytes and `cache.put(...)` stores them.
4. Optional `verify_hash` recomputes via `hashlib` and raises `ChecksumMismatchError` on mismatch.
5. If `storage` is set and `dest` is None, `Storage.write(data, fname)` returns a URI; otherwise `save_bytes` writes to the local FS (`src/datasluice/io/local.py:18`).

**State Management:**
- `DataSluice` holds `self.settings`, `self.auth`, `self._transport`, `self.adapter`, and a lazily-initialized `self._downloader`.
- `BaseAdapter._transport` is mutated post-construction by `DataSluice.__init__` (`client.py:61`) so adapter and client share one transport.
- `RateLimiter` keeps `_last_request` and a `threading.Lock` for thread-safe throttling.
- `FileCache` is stateless across calls (TTL derived from filesystem mtime).
- No global mutable singletons beyond the module-level `registry` in `adapters/registry.py` and the package logger configured by `configure_logging`.

## Key Abstractions

**`BaseAdapter`:**
- Purpose: Portal-agnostic contract for searching and fetching open-data.
- Examples: `src/datasluice/adapters/ckan/adapter.py`, `src/datasluice/adapters/datagouv/adapter.py`, `src/datasluice/adapters/socrata/adapter.py`, `src/datasluice/adapters/custom/adapter.py`.
- Pattern: ABC with `ClassVar[str] portal_type` and four abstract methods; subclasses compose a `mapper.py` (pure functions) and a `pagination.py` dataclass.

**`BaseAuth`:**
- Purpose: Strategy for decorating request headers and params with credentials.
- Examples: `src/datasluice/auth/{none,api_key,bearer,basic,headers}.py`.
- Pattern: ABC with `apply(headers, params) -> tuple[dict, dict]`; immutable returns.

**`BaseFormatReader`:**
- Purpose: Normalize file content into a list of record dicts.
- Examples: `src/datasluice/formats/{csv,json,parquet,xlsx,geojson}.py`.
- Pattern: ABC with `ClassVar[str] format_name` and `read(source) -> list[dict]`; readers accept `str | Path | bytes`.

**`Storage`:**
- Purpose: Pluggable backend for persisting downloaded bytes.
- Examples: `src/datasluice/io/storage.py` (`LocalStorage` only today).
- Pattern: ABC with `write / read / exists`; future S3/GCS backends slot in here.

**Frozen Domain Dataclasses:**
- Purpose: Immutable, hashable value objects representing portal-agnostic concepts.
- Examples: `Dataset`, `Resource`, `Organization`, `License`, `Query` (all `@dataclass(frozen=True)`).
- Pattern: `extra: dict[str, Any]` field on every model preserves portal-native fields not captured by named attributes.

## Entry Points

**Library import:**
- Location: `src/datasluice/__init__.py`
- Triggers: `from datasluice import DataSluice`
- Responsibilities: Re-exports `DataSluice`, domain models, and the full exception hierarchy.

**CLI:**
- Location: `src/datasluice/cli/app.py` (registered as `datasluice = "datasluice.cli.app:app"` in `pyproject.toml`).
- Triggers: `datasluice <command>` shell invocation.
- Responsibilities: Dispatches to `search`, `inspect`, `download`, `detect` subcommands; each lazily imports `DataSluice` inside the function body.

**Adapter factory:**
- Location: `src/datasluice/adapters/factory.py:create_adapter`.
- Triggers: `DataSluice.__init__`.
- Responsibilities: Resolves adapter class via `registry.get(portal_type)`; auto-detects via `discovery.detect_portal_type` when type is unknown.

## Architectural Constraints

- **Threading:** Single-threaded by default. `RateLimiter` is thread-safe (uses `threading.Lock`) so concurrent calls into a shared `HttpClient` are safe; no worker threads are spawned by the library.
- **Global state:**
  - Module-level singleton `registry` in `src/datasluice/adapters/registry.py:62` — mutated by side-effect imports in `adapters/__init__.py`.
  - Package logger configured once by `configure_logging` in `src/datasluice/logging.py`; `DataSluice.__init__` calls this on every construction (idempotent — only adds a handler if none exist).
- **Circular imports:** `_version.py` is intentionally separate from `__init__.py` to break a circular dependency with `transport/user_agent.py` (which imports `__version__`). Documented in `AGENTS.md` — do not move `__version__` into `__init__.py`.
- **Python version:** Requires Python 3.12+ (`requires-python = ">= 3.12"`); uses PEP 695 type params (`def with_retry[T]`, `def paginate[T]`) and `type | None` union syntax.
- **Lazy optional deps:** Heavy integrations and format readers (pandas, polars, pyarrow, openpyxl, dlt, duckdb, apache-airflow) MUST be imported inside function bodies, never at module top-level. `ty check --all-extras` is required to resolve these for type checking.
- **No third-party HTTP client:** Network I/O uses only `urllib.request` from the stdlib; `requests` / `httpx` are not dependencies.
- **Frozen models:** All domain dataclasses are `frozen=True`; mutating a model requires reconstruction.

## Anti-Patterns

### Direct transport mutation after construction

**What happens:** `DataSluice.__init__` sets `self.adapter._transport = self._transport` (`src/datasluice/client.py:61`) to share one transport between client and adapter after the adapter was already constructed by `create_adapter`.
**Why it's wrong:** Reaching into a private attribute (`_transport`) of another object couples the client to the adapter's internal layout and bypasses the lazy-init contract in `BaseAdapter.transport` (`adapters/base.py:39`).
**Do this instead:** Pass the transport through `create_adapter` (extend `adapters/factory.py` to accept and forward `transport=`), or construct the adapter with the transport already resolved.

### Bare `except Exception` swallowing detection errors

**What happens:** `detect_portal_type` (`src/datasluice/discovery/detector.py:55`) catches `Exception` broadly inside the fingerprint probe loop and silently `continue`s, making it impossible to distinguish "endpoint returned 404" from "network is down" or "transport misconfigured".
**Why it's wrong:** Users see only the final `PortalDetectionError` with no diagnostic context; transient auth or DNS failures look identical to "no fingerprint matched".
**Do this instead:** Catch `PortalError` specifically and log the failed probe at DEBUG via `logger.debug` (the logger is already imported), re-raising unexpected exceptions.

### Socrata stub for `get_organization`

**What happens:** `SocrataAdapter.get_organization` (`src/datasluice/adapters/socrata/adapter.py:59`) returns a bare `Organization(id=organization_id)` with no metadata because Socrata has no dedicated organizations endpoint.
**Why it's wrong:** Callers cannot distinguish a real (but empty) organization record from a stub; the contract of `BaseAdapter.get_organization` is implicitly weakened.
**Do this instead:** Either raise `NotFoundError` / emit a warning, or attach an `extra={"_stub": True}` flag so consumers can detect the placeholder. Document the limitation in the method docstring (currently only a one-line note exists).

### Inline URL construction vulnerable to injection

**What happens:** `integrations/duckdb.py:34` interpolates `resource_url` and `table_name` directly into SQL strings (`f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_csv_auto('{resource_url}')"`).
**Why it's wrong:** A malicious or malformed URL/table name can break the statement or, worst case, inject SQL.
**Do this instead:** Use DuckDB's parameterised query form or validate `table_name` against an identifier regex before interpolation. At minimum, escape single quotes in `resource_url`.

## Error Handling

**Strategy:** Single-rooted exception hierarchy in `src/datasluice/exceptions.py` rooted at `DataSluiceError`. Every public API raises a subclass of `DataSluiceError`; consumers can catch the root to handle any library failure.

**Patterns:**
- HTTP layer maps status codes to typed exceptions: 429 → `RateLimitError(retry_after=...)`, other non-2xx → `PortalError`, connection errors → `PortalError`.
- Adapter-specific error mapping helpers (e.g. `src/datasluice/adapters/ckan/errors.py:map_ckan_error`) translate portal-native error bodies into the shared hierarchy (404 → `NotFoundError`, 429 → `RateLimitError`).
- `RetryPolicy.retry_on = (RateLimitError, OSError)` — only transient failures are retried; permanent errors propagate immediately.
- `RateLimitError.retry_after` (parsed from the `Retry-After` header) overrides the exponential backoff delay in `with_retry`.
- Lazy optional-dep `ImportError`s are re-raised as `FormatError` (formats) or `ImportError` with install instructions (integrations) so callers get actionable messages.
- `ChecksumMismatchError` carries `expected` and `actual` digests for diagnostics.
- `ConfigError` raised for malformed env-var values (non-numeric where numeric expected).

## Cross-Cutting Concerns

**Logging:** Stdlib `logging` via `src/datasluice/logging.py`. `get_logger(name)` returns `logging.getLogger(f"datasluice.{name}")`; `configure_logging(level)` is called once per `DataSluice` construction and only installs a `StreamHandler` if none exist (format: `%(asctime)s [%(name)s] %(levelname)s: %(message)s`). Log level comes from `Settings.log_level` (env: `DATASLUICE_LOG_LEVEL`, default `INFO`).

**Validation:** Minimal runtime validation. `Settings` parsers (`_get_float`, `_get_int`) raise `ConfigError` on malformed env values. `RateLimiter.__post_init__` rejects non-positive `requests_per_second`. `Resource.normalize_format` canonicalizes format strings. Domain dataclasses rely on type hints (enforced statically by `ty`, not at runtime).

**Authentication:** Pluggable via `BaseAuth` strategy injected into `HttpClient`. `Settings` exposes `api_key` and `bearer_token` env vars but does not auto-construct an auth object — callers pass `APIKeyAuth(...)` / `BearerAuth(...)` explicitly to `DataSluice(auth=...)`. `NoAuth` is the default for public portals.

**Configuration:** Single `Settings` dataclass (`src/datasluice/config/settings.py`) populated from `DATASLUICE_*` env vars with defaults from `src/datasluice/config/defaults.py`. Key knobs: `http_timeout` (30s), `http_retries` (3), `rate_limit` (10 rps), `page_size` (100), `cache_dir` (`.datasluice/cache`), `cache_ttl` (3600s).

**User-Agent:** `build_user_agent()` (`src/datasluice/transport/user_agent.py`) emits `datasluice/{version} (Python {python}; {os})`; overridable via `DATASLUICE_USER_AGENT`.

---

*Architecture analysis: 2026-07-05*
