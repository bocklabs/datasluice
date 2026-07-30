# External Integrations

**Analysis Date:** 2026-07-30

## APIs & External Services

### Open-Data Portals (catalog discovery + metadata)

The three built-in portal connectors are registered via setuptools entry points in `pyproject.toml:84-87` under the `datasluice.connectors` group, resolved at runtime by `PluginManager` (`src/datasluice/runtime/plugin_manager.py`). Each has an identical subpackage layout: `adapter.py`, `mapper.py`, `pagination.py`, `errors.py`, `factory.py`.

**CKAN portals:**
- Service: CKAN Action API (`{base_url}/api/3/action/`)
- Adapter: `src/datasluice/connectors/ckan/adapter.py` (`CKANAdapter`)
- Factory entry point: `datasluice.connectors.ckan.factory:create_ckan_connector`
- Endpoints used: `package_search` (search), `package_show` (dataset), `organization_show` (org)
- Query mapping: filters translated to Solr `fq` clauses; supports text, tags, organizations, groups, res_format, license_id, sort
- Auth: optional, via injected `BaseAuth` (API key / bearer / basic)
- Example portals: data.gov, data.gov.uk, European Data Portal

**data.gouv.fr (udata):**
- Service: udata REST API (`{base_url}/api/1/`)
- Adapter: `src/datasluice/connectors/datagouv/adapter.py` (`DataGouvAdapter`)
- Factory entry point: `datasluice.connectors.datagouv.factory:create_datagouv_connector`
- Endpoints used: `datasets/` (search + by-id), `organizations/{slug}/`
- Query mapping: `res_format`→`format`, `license_id`→`license`, `tags`→`tag[]`, `organizations`→`organization`; `groups` NOT supported
- Example portal: `https://data.gouv.fr`

**Socrata:**
- Service: Socrata Discovery API (`{base_url}/api/catalog/v1`)
- Adapter: `src/datasluice/connectors/socrata/adapter.py` (`SocrataAdapter`)
- Factory entry point: `datasluice.connectors.socrata.factory:create_socrata_connector`
- Endpoints used: `/api/catalog/v1` (search + by 4x4 id)
- Query mapping: only `text`, `tags`, `sort` supported (ascending-only `order` token). `get_organization` intentionally NOT implemented — no stub.
- Example portal: `https://data.cityofnewyork.us`

### HTTP Transport Layer

Two interchangeable backends, both satisfying the `Transport` Protocol. Selection happens in `src/datasluice/runtime/defaults.py:create_default_transport`:

**Default (zero-config) — urllib:**
- Client: `HttpClient` in `src/datasluice/transport/http_client.py`
- Wraps stdlib `urllib.request` with `urllib.request.build_opener`
- Used when `httpx` is NOT importable (`importlib.util.find_spec("httpx") is None`)
- No streaming support

**Preferred (with `http` extra) — httpx:**
- Client: `HttpxTransport` in `src/datasluice/transport/httpx_transport.py`
- SDK: `httpx` (>=0.27, lockfile `0.28.1`), lazy-imported in `__init__`
- Adds: streaming responses (`stream()` context manager), conditional fetch (ETag/If-None-Match, `conditional_fetch()`), 401/403 credential eviction+refresh
- Single reusable `httpx.Client` instance (thread-safe, connection-pooled)

**Cross-cutting transport features (`src/datasluice/transport/`):**
- `retry.py` — `RetryPolicy` (max 3 attempts, full-jitter exponential backoff, retries `RateLimitError`/`RetryableHTTPError`/`OSError`; honors `Retry-After` headers)
- `rate_limit.py` — `RateLimiter` (thread-safe token bucket, default 10 req/s)
- `redirect.py` — `CredentialAwareRedirectHandler` (strips sensitive headers on cross-origin or https→http downgrade; enforces `CredentialScope` policy)
- `user_agent.py` — builds `datasluice/{version} (Python {py}; {os})` User-Agent
- `pagination.py` — shared pagination abstractions

### Data-Processing Integrations (`src/datasluice/integrations/`)

All are optional, lazy-imported terminals that materialize a `datasluice.data.BatchStream` into a target format. Arrow is the shared substrate — pandas/polars/duckdb all delegate through `to_arrow()`:

| Integration | Module | Extra | What it does |
|-------------|--------|-------|--------------|
| pyarrow (Arrow) | `integrations/arrow.py:to_arrow` | `parquet`/`streaming` | Materialize `BatchStream` → `pa.Table` (the substrate) |
| pandas | `integrations/pandas.py:to_pandas` | `pandas` | `pa.Table` → `pd.DataFrame` (zero-copy via Arrow) |
| polars | `integrations/polars.py:to_polars` | `polars` | `pa.Table` → `polars.DataFrame` (via `polars.from_arrow`) |
| DuckDB | `integrations/duckdb.py:to_duckdb` | `duckdb` | Register `pa.Table` as a named DuckDB relation (`conn.register`); SQL-identifier validation (SEC-03) |
| dlt | `integrations/dlt.py:datasluice_source` | `dlt` | Wraps a portal search as a `@dlt.source` yielding one `@dlt.resource` per portal resource; supports `state_store` watermarks |
| Apache Airflow | `integrations/airflow.py:DataSluiceOperator` | `airflow` | Factory returning an Airflow `BaseOperator` subclass (`template_fields = portal/query/dest_dir`) for DAGs |

## Data Storage

**Databases:**
- None for metadata/catalog. DuckDB is available only as an in-process analytical engine (`duckdb.connect()` in-memory by default), NOT as a persistent database. No SQLAlchemy/ORM.

**File Storage (downloaded data + sync output):**
- **fsspec** abstraction (`storage` extra, >=2025.1, lockfile `2026.6.0`)
  - Central factory: `src/datasluice/io/filesystem.py:open_filesystem` — delegates to `fsspec.core.url_to_fs` (single dispatch point for all protocols)
  - Adapter: `src/datasluice/io/fsspec_storage.py:FsspecStorage` — wraps an `AbstractFileSystem` to satisfy `StoragePort`; returns URI strings (never `pathlib.Path`, CORR-05)
  - Supported URI schemes: `file://`, `memory://`, `s3://`, `gs://`, `az://`, `abfs://`, `http://`, `https://`
  - Credential precedence: explicit `credentials=` dict → URI-embedded → env vars/config/IAM
- **Local filesystem** fallback: `src/datasluice/io/local.py` (`ensure_dir`, `safe_filename`, `save_bytes`), `src/datasluice/io/storage.py:Storage` default
- **Downloader**: `src/datasluice/io/downloader.py:Downloader` — fetches resources via transport, optional caching + checksum (SHA-256) verification

**Caching:**
- In-process content cache: `src/datasluice/io/content_cache.py:ContentCache` (disk-backed, TTL-based) + `src/datasluice/io/cache.py:FileCache`
- Default cache dir: `.datasluice/cache`, default TTL: 3600s (`src/datasluice/config/defaults.py`)
- Session wires cache lazily only when `cache_dir=` or `cache=` is passed (`src/datasluice/runtime/session.py:151`)

**State (sync progress / checkpoints):**
- In-memory default: `src/datasluice/sync/state_store.py:InMemoryStateStore`
- Hashing for change detection: `src/datasluice/sync/_hashing.py` (`logical_sha256`)
- Not persistent across processes by default (no external state DB)

## Authentication & Identity

**Auth Provider:** Custom pluggable strategies (no OAuth provider integration, no SaaS auth).

**Strategy registry (`src/datasluice/auth/`, re-exported from `src/datasluice/auth/__init__.py`):**
| Strategy | Module | Mechanism |
|----------|--------|-----------|
| `NoAuth` | `auth/none.py` | Default — no credentials (zero-config) |
| `APIKeyAuth` | `auth/api_key.py` | API key in header (default `X-Api-Key`) or query param |
| `BearerAuth` | `auth/bearer.py` | Bearer token / JWT in `Authorization` header |
| `BasicAuth` | `auth/basic.py` | HTTP Basic (base64 username:password) |
| `HeadersAuth` | `auth/headers.py` | Arbitrary custom headers |

All strategies extend `BaseAuth` (`src/datasluice/auth/base.py`) — an ABC with `apply(headers, params) -> (headers, params)`.

**Credential lifecycle (dynamic refresh):**
- `HostCredentialProvider` (`src/datasluice/credentials/host_provider.py`) — host-scoped resolver with cached expiry + single-flight refresh (per-host `threading.Lock`, double-checked expiry). The `refresher` callable `(host) -> (BaseAuth, expires_at|None)` is the v2 OAuth plug-in seam; v1 defaults to `None` (static creds never expire).
- HttpxTransport evicts+refreshes exactly once on 401/403 (`src/datasluice/transport/httpx_transport.py:233`).

**Credential scope (redirect safety):**
- `CredentialScope` domain model (`src/datasluice/domain/credentials.py`) defines `allowed_hosts`, `allowed_schemes`, `send_on_redirect`. When omitted, any cross-host redirect or https→http downgrade strips credentials (zero-config safety).

**Object-store credentials** (S3/GCS/Azure) flow through `open_filesystem(uri, credentials=)` and fsspec's own resolver — NEVER through `HostCredentialProvider` (separation of concerns).

## Monitoring & Observability

**Error Tracking:**
- None (no Sentry, no external APM)

**Logs:**
- Python stdlib `logging` via `src/datasluice/logging.py`
- Package logger: `datasluice` with sub-loggers (e.g. `datasluice.transport.http`, `datasluice.session`)
- `RedactingFilter` (`src/datasluice/logging.py:45`) — redacts known sensitive keys (`authorization`, `cookie`, `x-api-key`, `token`, `secret`, `password`, etc.) from log records. Targeted key matching, never value-pattern heuristics. Disable with `DATASLUICE_NO_REDACT=1`.
- Default format: `%(asctime)s [%(name)s] %(levelname)s: %(message)`

**Exceptions:** Custom hierarchy rooted at `DataSluiceError` (`src/datasluice/exceptions.py`): `PortalError`, `RateLimitError`, `RetryableHTTPError`, `AuthenticationError`, `DownloadError`, `ChecksumMismatchError`, `PortalDetectionError`, `StateStoreError`, `FormatError`, etc.

## CI/CD & Deployment

**Hosting (library):**
- Distributed via **PyPI** (`https://pypi.org/p/datasluice`) and **TestPyPI**
- No long-running server — this is a library/CLI package

**Hosting (docs):**
- **GitHub Pages** at `https://nitish-raj.github.io/datasluice/` (`.github/workflows/docs.yml`)
- DNS: `CNAME` file present (custom domain configured)

**CI Pipeline:**
- GitHub Actions (`.github/workflows/ci.yml`) — see STACK.md for full job breakdown

**Security CI:**
- CodeQL (`.github/workflows/codeql.yml`) — Python `security-extended` + Actions analysis, weekly schedule
- zizmor (`.github/workflows/zizmor.yml`) — workflow security analysis on `.github/`

**Publishing:**
- Automated via `release: published` trigger (`.github/workflows/publish.yml`) — TestPyPI → PyPI (gated by manual approval). Build provenance attested via `actions/attest-build-provenance`.

## Environment Configuration

**Required env vars:** None. The library runs zero-config with sensible defaults.

**Optional env vars (documented in `.env.example`, but not auto-read by code):**
- `DATASLUICE_HTTP_TIMEOUT` (default 30), `DATASLUICE_HTTP_RETRIES` (default 3), `DATASLUICE_HTTP_RATE_LIMIT` (default 10)
- `DATASLUICE_API_KEY`, `DATASLUICE_BEARER_TOKEN` (for portals requiring auth)
- `DATASLUICE_CACHE_DIR` (default `.datasluice/cache`), `DATASLUICE_CACHE_TTL` (default 3600)
- `DATASLUICE_LOG_LEVEL` (default `INFO`), `DATASLUICE_USER_AGENT`

**Actually-read env var (code):**
- `DATASLUICE_NO_REDACT=1` — disables log redaction (`src/datasluice/logging.py:57`)

**Secrets location:**
- GitHub Actions secrets: `TEST_PYPI_API_KEY`, `PYPI_API_KEY` (in `pypi`/`test-pypi` environments)
- No committed secrets. Cloud credentials (S3/GCS/Azure) resolved by fsspec from the runtime environment (env vars, IAM, config files) — never stored in-repo.

## Webhooks & Callbacks

**Incoming:** None. DataSluice is a pull-based client library, not a server.

**Outgoing:** None. All portal interaction is synchronous HTTP GET (catalog) and GET (resource download). No webhook registration, no callbacks to external systems.

## Entry Points & Plugin System

**CLI entry point:** `datasluice = "datasluice.cli.app:app"` (`pyproject.toml:82`) — Typer app with subcommands `search`, `inspect`, `download`, `detect` (`src/datasluice/cli/app.py`).

**Connector entry points (`[project.entry-points."datasluice.connectors"]`, `pyproject.toml:84-87`):**
- `ckan` → `datasluice.connectors.ckan.factory:create_ckan_connector`
- `datagouv` → `datasluice.connectors.datagouv.factory:create_datagouv_connector`
- `socrata` → `datasluice.connectors.socrata.factory:create_socrata_connector`

Third-party connectors can register new factories under the same entry-points group — `PluginManager` discovers them via installed package metadata. Each factory receives a `ConnectorContext` (`src/datasluice/runtime/context.py`) carrying injected transport/auth/infra ports.

---

*Integration audit: 2026-07-30*
