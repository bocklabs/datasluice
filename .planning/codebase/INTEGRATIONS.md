# External Integrations

**Analysis Date:** 2026-07-05

## APIs & External Services

The library speaks to open-data portals over plain HTTPS using Python's stdlib `urllib` (no `requests`/`httpx`/`aiohttp`). All HTTP traffic flows through `src/datasluice/transport/http_client.py:HttpClient`.

**Open-Data Portals (built-in adapters, auto-registered in `src/datasluice/adapters/__init__.py`):**

- **CKAN** — Action API at `{base_url}/api/3/action/`
  - Adapter: `src/datasluice/adapters/ckan/adapter.py:CKANAdapter` (`portal_type = "ckan"`)
  - Endpoints used: `package_search`, `package_show`, `organization_show`
  - Mapper: `src/datasluice/adapters/ckan/mapper.py` · Pagination: `src/datasluice/adapters/ckan/pagination.py:CKANPage`
  - Detection fingerprints: `/api/3/action/package_search`, `/api/3/action/group_list` (`src/datasluice/discovery/fingerprints.py`)

- **data.gouv.fr / udata** — REST API at `{base_url}/api/1/`
  - Adapter: `src/datasluice/adapters/datagouv/adapter.py:DataGouvAdapter` (`portal_type = "datagouv"`)
  - Endpoints used: `datasets/`, `datasets/{id}/`, `organizations/{slug}/`
  - Mapper: `src/datasluice/adapters/datagouv/mapper.py` · Pagination: `DataGouvPage`
  - Detection fingerprints: `/api/1/datasets/`, `/api/1/organizations/`

- **Socrata** — Discovery API at `{base_url}/api/catalog/v1`
  - Adapter: `src/datasluice/adapters/socrata/adapter.py:SocrataAdapter` (`portal_type = "socrata"`)
  - Endpoints used: catalog search by `q`/`tags`/`ids`
  - Mapper: `src/datasluice/adapters/socrata/mapper.py` · Pagination: `SocrataPage`
  - Detection fingerprints: `/api/catalog/v1`, `/api/views.json`
  - Note: `get_organization()` returns a stub — Socrata has no dedicated org endpoint.

- **Custom (template)** — `src/datasluice/adapters/custom/adapter.py:CustomAdapter` (`portal_type = "custom"`). All methods raise `NotImplementedError`; intended as a copy-and-override base for unsupported portals.

**Portal Auto-Detection:**
- `src/datasluice/discovery/detector.py:detect_portal_type()` probes URL paths from `PATH_FINGERPRINTS` and returns the first match registered in the `registry`. Raises `PortalDetectionError` if none match. `HTML_FINGERPRINTS` are declared but not yet wired into the detector.

**No SDK clients:** Each adapter calls `self.transport.get_json(url, params=...)` directly; there is no CKAN/Socrata SDK dependency.

## Data Storage

**Databases:**
- None. DataSluice is stateless and stores no persistent metadata.

**File Storage:**
- Local filesystem only via `src/datasluice/io/storage.py:LocalStorage` (writes under a configurable `base_dir`). The `Storage` ABC (`write`/`read`/`exists`) is designed so S3, GCS, etc. can be plugged in without touching the download pipeline — see the module docstring in `src/datasluice/io/storage.py`.
- Local helpers: `src/datasluice/io/local.py` (`ensure_dir`, `save_bytes`, `safe_filename`).
- Downloader: `src/datasluice/io/downloader.py:Downloader` orchestrates fetch → optional cache → optional checksum verify → storage/disk.

**Caching:**
- `src/datasluice/io/cache.py:FileCache` — time-based (TTL), key = resource URL, stored on local disk under `DATASLUICE_CACHE_DIR` (default `.datasluice/cache`). Not injected by default; callers pass an instance to `Downloader`.

## Authentication & Identity

**Auth Provider:**
- Custom, pluggable strategy pattern (`src/datasluice/auth/base.py:BaseAuth` ABC). Strategies mutate request headers and/or query params via `apply(headers, params) -> (headers, params)`.
- Implementations (`src/datasluice/auth/__init__.py`):
  - `NoAuth` — default, no-op (`src/datasluice/auth/none.py`).
  - `APIKeyAuth` — header (default `X-Api-Key`) and/or query param (`src/datasluice/auth/api_key.py`).
  - `BearerAuth` — `Authorization: Bearer <token>` (`src/datasluice/auth/bearer.py`).
  - `BasicAuth` — HTTP Basic via base64 (`src/datasluice/auth/basic.py`).
  - `HeadersAuth` — arbitrary static header bag (`src/datasluice/auth/headers.py`).
- Auth strategy is passed to `DataSluice(portal_url, auth=...)` or configured via `DATASLUICE_API_KEY` / `DATASLUICE_BEARER_TOKEN` env vars (`src/datasluice/config/settings.py`).
- No OAuth flow, no token refresh, no session management.

## Monitoring & Observability

**Error Tracking:**
- None (no Sentry, no external APM). Errors surface through the typed exception hierarchy in `src/datasluice/exceptions.py` (`DataSluiceError` base → `PortalError`, `AdapterError`, `AuthenticationError`, `RateLimitError`, `NotFoundError`, `DownloadError`, `ChecksumMismatchError`, `FormatError`, `ConfigError`, `PortalDetectionError`, `AdapterNotFoundError`).

**Logs:**
- Python `logging` stdlib via `src/datasluice/logging.py`. All modules obtain a sub-logger with `get_logger("<area>")` (e.g. `transport.http`, `io.downloader`, `discovery`).
- `configure_logging(level)` attaches a single `StreamHandler` to the `datasluice` root logger the first time it runs; format `%(asctime)s [%(name)s] %(levelname)s: %(message)s`. Level driven by `DATASLUICE_LOG_LEVEL`.
- HTTP 429 responses are decoded and surfaced as `RateLimitError` with `retry_after` honoured by `RetryPolicy` (`src/datasluice/transport/retry.py`).

## CI/CD & Deployment

**Hosting:**
- Source: GitHub (`github.com/nitish-raj/datasluice`).
- Distribution: [PyPI](https://pypi.org/project/datasluice/) via trusted publishing (OIDC `id-token: write`).
- Docs: GitHub Pages at https://nitish-raj.github.io/datasluice/ and custom domain https://datasluice.rajnitish.com (`CNAME` file).

**CI Pipeline (GitHub Actions, `.github/workflows/`):**
- `ci.yml` — on push to `main` and PRs. Jobs:
  - **lint** — `uv run ruff format --check .` and `ruff check .`
  - **type-check** — `uv run --all-extras ty check .`
  - **test** — matrix Python 3.12 / 3.13 / 3.14 with `coverage run -m pytest`, uploads `.coverage.*` artifacts
  - **coverage** — combines artifacts, posts report to `$GITHUB_STEP_SUMMARY`
  - **all-checks-pass** — `re-actors/alls-green` gate
- `publish.yml` — on `v*` tags. Builds with `uv build`, attests build provenance (`actions/attest-build-provenance`), publishes to PyPI environment `pypi` (requires reviewer approval).
- `docs.yml` — on push to `main`. Builds Zensical docs, deploys to GitHub Pages (`site/` artifact).
- `release.yml` — manual `workflow_dispatch` with `version` input; runs `scripts/release.py` to tag and create GitHub release notes.
- `codeql.yml`, `zizmor.yml` — security scanning (CodeQL + GitHub Actions security analyzer).
- All workflows pin third-party actions by SHA digest and use `permissions: {}` (least privilege). `publish.yml` disables the uv cache to prevent cache-poisoning.

**Local CI equivalent:**
- `just qa` / `make qa` → `ruff format` → `ruff check --fix` → `ty check` (concise) → `pytest`.

## Environment Configuration

**Required env vars:**
- None strictly required — every value has a default in `src/datasluice/config/defaults.py`.

**Optional env vars (see `.env.example` — do not read `.env`):**
- `DATASLUICE_HTTP_TIMEOUT`, `DATASLUICE_HTTP_RETRIES`, `DATASLUICE_HTTP_RATE_LIMIT`
- `DATASLUICE_PAGE_SIZE`
- `DATASLUICE_CACHE_DIR`, `DATASLUICE_CACHE_TTL`
- `DATASLUICE_LOG_LEVEL`
- `DATASLUICE_API_KEY`, `DATASLUICE_BEARER_TOKEN` (portal credentials)
- `DATASLUICE_USER_AGENT` (override the auto-built UA string)

**Secrets location:**
- Secrets are supplied via process environment variables at runtime; no secret files are committed. `.env.example` is a template only — the real `.env` is gitignored.

## Webhooks & Callbacks

**Incoming:**
- None. DataSluice is a pull-only client library; it does not expose an HTTP server.

**Outgoing:**
- None. No outbound webhooks are dispatched. The only outbound network calls are HTTPS GETs to open-data portal APIs and resource download URLs (driven entirely by `HttpClient` in `src/datasluice/transport/http_client.py`).

---

*Integration audit: 2026-07-05*
