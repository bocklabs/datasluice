# External Integrations

**Analysis Date:** 2026-07-26

DataSluice is a Python library/CLI that **integrates with external open-data portals over HTTP** and bridges into data-processing ecosystems (pandas, polars, dlt, DuckDB, Airflow). It makes only **outbound** calls; it does not expose servers, webhooks, or inbound endpoints.

## APIs & External Services

### Open-data portals (catalog APIs)

All portal adapters extend `BaseAdapter` (`src/datasluice/connectors/base.py`) and translate portal-native JSON into `datasluice.domain` models. Each adapter has a sibling `mapper.py`, `pagination.py`, `errors.py`, and `factory.py`.

**CKAN:**
- Service: CKAN Action API at `{base_url}/api/3/action/`
- Implementation: `src/datasluice/connectors/ckan/adapter.py` (`CKANAdapter`)
- Endpoints used: `package_search`, `package_show`, `organization_show`
- Mapper: `src/datasluice/connectors/ckan/mapper.py`
- Entry-point: `ckan = "datasluice.connectors.ckan.factory:create_ckan_connector"` (`pyproject.toml`)

**data.gouv.fr / udata:**
- Service: udata REST API at `{base_url}/api/1/`
- Implementation: `src/datasluice/connectors/datagouv/adapter.py` (`DataGouvAdapter`)
- Endpoints used: `datasets/`, `organizations/{slug}/`
- Entry-point: `datagouv = "datasluice.connectors.datagouv.factory:create_datagouv_connector"`

**Socrata:**
- Service: Socrata Discovery API at `{base_url}/api/catalog/v1` (SODA2)
- Implementation: `src/datasluice/connectors/socrata/adapter.py` (`SocrataAdapter`)
- Note: Socrata exposes no dedicated organizations endpoint — `get_organization` returns a minimal stub.
- Entry-point: `socrata = "datasluice.connectors.socrata.factory:create_socrata_connector"`

**Custom (extension point):**
- Skeleton adapter at `src/datasluice/connectors/custom/adapter.py` (`CustomAdapter`) — all methods `raise NotImplementedError`. Copy, rename, and implement to support a new portal platform.

**Portal auto-detection:**
- `src/datasluice/discovery/detector.py` — `detect_portal_type(url)` probes well-known paths.
- Fingerprints in `src/datasluice/discovery/fingerprints.py` map API paths / HTML signatures to portal types (e.g. `/api/3/action/package_search` → `ckan`).

**Plugin discovery model:**
- Connectors are registered under the `datasluice.connectors` entry-points group in `pyproject.toml` (`[project.entry-points."datasluice.connectors"]`).
- `PluginManager` (`src/datasluice/runtime/plugin_manager.py`) loads them eagerly via `importlib.metadata.entry_points`. A broken third-party plugin is captured as a `PluginFailure` and never crashes session creation.
- `DataSluiceSession.portal(url)` (`src/datasluice/runtime/session.py`) auto-detects the portal type, resolves the factory through `PluginManager`, and constructs the adapter with an injected `ConnectorContext`.

### HTTP transport (the integration substrate)

Two interchangeable backends satisfy the `Transport` / `StreamingTransport` ports (`src/datasluice/ports/transport.py`):

- **urllib `HttpClient`** (`src/datasluice/transport/http_client.py`) — stdlib default for bare installs (no extras).
- **`HttpxTransport`** (`src/datasluice/transport/httpx_transport.py`) — preferred when the `http` extra is installed; auto-selected by `src/datasluice/runtime/defaults.py` via `importlib.util.find_spec("httpx")`.

Both backends share:
- `RetryPolicy` with full-jitter exponential backoff (`src/datasluice/transport/retry.py`) — retries on `RateLimitError`, `RetryableHTTPError`, `OSError`; honours `Retry-After` on HTTP 429.
- `RateLimiter` requests-per-second cap (`src/datasluice/transport/rate_limit.py`).
- `CredentialAwareRedirectHandler` (`src/datasluice/transport/redirect.py`) — manual redirect loop that strips sensitive headers (`authorization`, `cookie`, `x-api-key`, `x-auth-token`) on cross-host or `https`→`http` hops per the `CredentialScope` policy.
- `build_user_agent()` (`src/datasluice/transport/user_agent.py`) — sends `datasluice/{version} (Python {py}; {os})`.

The httpx backend additionally supports streaming responses via `HttpxTransport.stream()` → `StreamResponse` and a single-flight 401/403 credential eviction path.

## Data Storage

**Databases:**
- Embedded **SQLite** (stdlib `sqlite3`) used ONLY for the content-cache metadata index — no server, no external connection string.
  - Location: `src/datasluice/io/content_cache.py` (`ContentCache`)
  - File: `{cache_dir}/cache.db`, WAL journal mode, `busy_timeout=5000`.
  - Two-phase atomic writes (`writing` → `ready` status) with lazy sweep of stale/orphaned entries.

**File Storage:**
- **Local filesystem** (default): `src/datasluice/io/storage.py` (`LocalStorage`) + `src/datasluice/io/local.py`. Path-traversal guarded (`path.relative_to` check).
- **fsspec backends** (opt-in via `storage` extra): `src/datasluice/io/fsspec_storage.py` (`FsspecStorage`) wraps any `fsspec.AbstractFileSystem`. Supports URI schemes `s3://`, `gs://`, `az://`, `abfs://`, `file://`, `http://`, `https://`, `memory://`.
  - Centralised factory: `src/datasluice/io/filesystem.py` → `open_filesystem(uri, credentials=)` delegates to `fsspec.core.url_to_fs`.
  - Credential precedence: explicit `credentials=` dict → URI-embedded creds → backend defaults (env vars / config files / IAM).
- **Downloader**: `src/datasluice/io/downloader.py` (`Downloader`) — fetches resource bytes via the transport, optional checksum verification (`verify_hash`, `hash_algorithm`), optional cache + storage injection.

**Caching:**
- `FileCache` (`src/datasluice/io/cache.py`) — simple time-based, SHA-256-keyed file cache (TTL default 3600s).
- `ContentCache` (`src/datasluice/io/content_cache.py`) — content-addressed cache with SQLite WAL index + ETag/Last-Modified sidecar (enables future conditional GETs). Satisfies `CachePort`.

## Authentication & Identity

**Auth Provider:** Custom — no OAuth/SaaS identity provider integrated. All auth is pluggable and caller-supplied.

**Strategies** (`src/datasluice/auth/`):
- `NoAuth` (default) — `src/datasluice/auth/none.py`.
- `APIKeyAuth` — `src/datasluice/auth/api_key.py`; key via header (`X-Api-Key`) and/or query param.
- `BearerAuth` — `src/datasluice/auth/bearer.py`; OAuth 2.0 / JWT in `Authorization: Bearer <token>`.
- `BasicAuth` — `src/datasluice/auth/basic.py`; HTTP Basic.
- `HeadersAuth` — `src/datasluice/auth/headers.py`; arbitrary header injection.
- Base protocol: `BaseAuth` (`src/datasluice/auth/base.py`) with `apply(headers, params) -> (headers, params)`.

**Host-scoped credential resolution:**
- `HostCredentialProvider` (`src/datasluice/credentials/host_provider.py`) — caches `BaseAuth` per host with single-flight refresh (`threading.Lock` per host, double-checked expiry). Optional `refresher` callable `(host) -> (BaseAuth, expires_at | None)` is the future OAuth2 plug-in seam (v1 passes `None` → never expires).
- `HttpxTransport` evicts + refreshes exactly once on 401/403 (capability-checked via `isinstance`, lazily resolved via `importlib`).

**Auth wiring in the session:**
- `DataSluiceSession(auth=...)` wraps a static auth in `_StaticCredentialProvider`. Pass `credential_provider=` to override. No env-var-based auth lookup.

## Monitoring & Observability

**Error Tracking:**
- None (no Sentry, Datadog, or equivalent). Errors surface as the exception hierarchy in `src/datasluice/exceptions.py` (`PortalError`, `RateLimitError`, `RetryableHTTPError`, `AuthenticationError`, `DownloadError`, `ChecksumMismatchError`, `FormatError`, `PortalDetectionError`, `AdapterNotFoundError`, `ConfigError`, `NotFoundError`).

**Logs:**
- Python stdlib `logging` via `src/datasluice/logging.py`. Package logger name: `datasluice` (sub-loggers via `get_logger("subsystem")`).
- `RedactingFilter` redacts known sensitive keys (`authorization`, `cookie`, `x-api-key`, `token`, `secret`, `password`, …) from log records. Disable with `DATASLUICE_NO_REDACT=1`.
- `SENSITIVE_HEADERS` frozenset is the single source of truth shared with the redirect handler (lifted into `logging.py` to avoid a circular import).

## CI/CD & Deployment

**Hosting:**
- Source: GitHub (`https://github.com/nitish-raj/datasluice`).
- Distribution: [PyPI](https://pypi.org/project/datasluice/) (+ [TestPyPI](https://test.pypi.org/project/datasluice/)).
- Docs: GitHub Pages at `https://nitish-raj.github.io/datasluice/`.

**CI Pipeline:** GitHub Actions (`.github/workflows/`).
- `ci.yml` — lint, type-check, test matrix, coverage, build, smoke-test.
- `docs.yml` — build + deploy docs to GitHub Pages.
- `codeql.yml` — GitHub CodeQL scanning (`python` + `actions`).
- `zizmor.yml` — workflow-security analysis.
- `pr-agent.yml` + `ocr-review.yml` — AI PR tooling (see *External AI Tooling*).

**Release Pipeline:**
- Automated by **Release Please** (`release-please-config.json`, `.github/workflows/release-please.yml`). Conventional Commits required.
- Publishing (`.github/workflows/publish.yml`): on `release: published` → build → `twine check` → attest build provenance → TestPyPI (auto) → PyPI (await approval, secret `PYPI_API_KEY` / `TEST_PYPI_API_KEY`).

**External AI Tooling (CI integrations, not runtime):**
- **PR-Agent** (`the-pr-agent/pr-agent` v0.40.0) — auto PR descriptions. Configured to an OpenAI-compatible endpoint via secrets `OCR_LLM_URL`, `OCR_LLM_AUTH_TOKEN`, `OCR_LLM_MODEL`.
- **OpenCodeReview** (`alibaba/open-code-review` v1.7.7) — AI code review on PRs. Same `OCR_LLM_*` secret family; also `OCR_LLM_USE_ANTHROPIC` for Anthropic models.
- **Graphifyy** (`uv tool install graphifyy`) — installed in both AI workflows (knowledge-graph tooling).

## Environment Configuration

**Required env vars:** None at runtime. The library is zero-config (no `Settings`, no required env vars). Optional `DATASLUICE_NO_REDACT=1` toggles log redaction.

**CI secrets** (GitHub Actions, in repo settings — never in code):
- `PYPI_API_KEY`, `TEST_PYPI_API_KEY` — publishing.
- `OCR_LLM_URL`, `OCR_LLM_AUTH_TOKEN`, `OCR_LLM_MODEL`, `OCR_LLM_USE_ANTHROPIC` — AI review tooling.
- `GITHUB_TOKEN` — standard action token.

**Secrets location:**
- GitHub Actions encrypted secrets (referenced in workflow files).
- `.env.example` present at repo root (template only — contents intentionally not read for this audit).
- Cloud object-store credentials flow through `open_filesystem(uri, credentials=)` to fsspec's per-backend resolver (env vars / config files / IAM), never through `HostCredentialProvider`.

## Webhooks & Callbacks

**Incoming:**
- None. DataSluice is a library + CLI; it does not expose an HTTP server or webhook receiver.

**Outgoing:**
- Outbound HTTP GET requests to:
  - Portal catalog APIs (CKAN `/api/3/action/`, udata `/api/1/`, Socrata `/api/catalog/v1`).
  - Resource download URLs (arbitrary, discovered per-dataset).
  - Portal well-known endpoints during auto-detection (`src/datasluice/discovery/detector.py`).
- Outbound object-store I/O (when `storage` extra + cloud URI configured) via fsspec.

## Data-Processing Integrations (bridges, not external services)

These are opt-in adapter modules under `src/datasluice/integrations/` that hand portal data off to third-party libraries (installed via the matching extra):

| Integration | Module | Extra | What it does |
|-------------|--------|-------|--------------|
| pandas | `integrations/pandas.py` | `pandas` | `resource_to_dataframe`, `dataset_to_dataframes` |
| Polars | `integrations/polars.py` | `polars` | `resource_to_dataframe` (lazy/eager) |
| dlt | `integrations/dlt.py` | `dlt` | `datasluice_source(...)` → `dlt.resource` |
| DuckDB | `integrations/duckdb.py` | `duckdb` | `resource_to_relation`, `query_resource` (SQL-injection-safe table-name validation) |
| Apache Airflow | `integrations/airflow.py` | `airflow` | `DataSluiceOperator` — dynamic `BaseOperator` subclass |

## Format Readers (normalisation layer)

`src/datasluice/formats/` — read remote resource files into `list[dict]`. Registry in `formats/__init__.py` (`READERS` dict + `get_reader(name)`).

| Format | Reader | Extra |
|--------|--------|-------|
| CSV | `csv.py` (`CSVReader`) | stdlib |
| JSON / JSONL / NDJSON | `json.py` (`JSONReader`) | stdlib |
| XLSX / XLS | `xlsx.py` (`XLSXReader`) | `xlsx` (`openpyxl`) |
| Parquet | `parquet.py` (`ParquetReader`) | `parquet` (`pyarrow`) |
| GeoJSON | `geojson.py` (`GeoJSONReader`) | stdlib |

---

*Integration audit: 2026-07-26*
