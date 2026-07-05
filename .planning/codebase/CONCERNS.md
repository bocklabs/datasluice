# Codebase Concerns

**Analysis Date:** 2026-07-05

## Tech Debt

### CLI commands violate the project's own B008 / Annotated rule

- Issue: Four of the five CLI modules use the deprecated `param: T = typer.Option(...)` / `typer.Argument(...)` call-in-default pattern that `AGENTS.md` and the ruff `B008` selection explicitly forbid. Only `src/datasluice/cli/download.py` follows the prescribed `Annotated[str, typer.Option(...)]` form. This is inconsistent and silently regresses whenever someone copies the wrong file as a template.
- Files: `src/datasluice/cli/app.py:25`, `src/datasluice/cli/search.py:15-17`, `src/datasluice/cli/inspect.py:14-15`, `src/datasluice/cli/detect.py:12`
- Impact: Type checker + lint signal is split across the CLI surface; future contributors copy the wrong pattern; eventual wholesale refactor needed.
- Fix approach: Convert every `typer.Option`/`typer.Argument` default in `app.py`, `search.py`, `inspect.py`, `detect.py` to `Annotated[T, typer.Option(...)]` / `Annotated[T, typer.Argument(...)]` matching `src/datasluice/cli/download.py:15-19`.

### `TYPE_CHECKING: pass` dead block in client

- Issue: `src/datasluice/client.py:20-21` declares `if TYPE_CHECKING: pass` — an empty guard that exists only to be filled. It signals an unfinished refactor and confuses readers.
- Files: `src/datasluice/client.py:20-21`
- Impact: Misleading dead code; no functional impact.
- Fix approach: Either remove the block, or move the genuine TYPE_CHECKING-only imports it was meant to host (e.g. `BaseAdapter`, `Downloader`) into it.

### Query filter fields are advertised but never honoured

- Issue: `Query` exposes `tags`, `organizations`, `groups`, `res_format`, `license_id` as first-class search filters. Only `text`, `sort`, `limit`, `offset` are used by the CKAN adapter. Socrata and data.gouv adapters honour at most one tag and one organization (see "Silent filter dropping" under Known Bugs). `res_format`, `license_id`, and `groups` are accepted but discarded by every adapter.
- Files: `src/datasluice/domain/query.py:24-29`, `src/datasluice/adapters/ckan/adapter.py:28-44`, `src/datasluice/adapters/socrata/adapter.py:27-45`, `src/datasluice/adapters/datagouv/adapter.py:27-50`
- Impact: Library silently returns un-filtered results — users believe they filtered by license or group when they did not. Trust-eroding bug class.
- Fix approach: Either wire each filter into every adapter's API params (CKAN supports `fq`, Socrata supports `categories`/`tags`, udata supports `license`/`theme`), or remove the unused fields from `Query` and document the supported subset per adapter.

### Settings loaded but never wired into auth

- Issue: `Settings.api_key`, `Settings.bearer_token`, and `Settings.user_agent` are loaded from environment (`DATASLUICE_API_KEY`, `DATASLUICE_BEARER_TOKEN`, `DATASLUICE_USER_AGENT`) in `src/datasluice/config/settings.py:55-57`, but `DataSluice.__init__` (`src/datasluice/client.py:46-64`) never constructs an auth strategy from them. Users who set `DATASLUICE_API_KEY` will be silently unauthenticated.
- Files: `src/datasluice/config/settings.py:55-57`, `src/datasluice/client.py:46-64`
- Impact: Feature looks configured but does nothing; users discover the omission only when their first authenticated request fails.
- Fix approach: In `DataSluice.__init__`, when `auth is None`, derive a strategy from settings: `APIKeyAuth(self.settings.api_key)` if set, else `BearerAuth(self.settings.bearer_token)` if set, else `NoAuth()`.

### `paginate()` helper is unused dead code

- Issue: `src/datasluice/transport/pagination.py` defines `PaginationConfig` and `paginate()` as the canonical pagination abstraction (and tests them in `tests/unit/transport/test_transport.py:53-61`), but every adapter implements pagination inline with its own page dataclass (`CKANPage`, `SocrataPage`, `DataGouvPage`). There is no iterator/auto-pagination story for callers.
- Files: `src/datasluice/transport/pagination.py`, `src/datasluice/adapters/ckan/pagination.py`, `src/datasluice/adapters/socrata/pagination.py`, `src/datasluice/adapters/datagouv/pagination.py`
- Impact: Two pagination models coexist; the public one is dead. Callers cannot iterate across all pages of a result set.
- Fix approach: Wire `paginate()` into a `DataSluice.search_all()` method that lazily walks pages, or remove `paginate`/`PaginationConfig` and their exports.

### `PortalMetadata` and `HTML_FINGERPRINTS` are dead exports

- Issue: `PortalMetadata` (`src/datasluice/discovery/portal_metadata.py`) is defined and exported but never instantiated anywhere in src. `HTML_FINGERPRINTS` (`src/datasluice/discovery/fingerprints.py:24-29`) is defined but the detector only consults `PATH_FINGERPRINTS`. The docstring in `fingerprints.py:9-11` even describes "check_function" pairs that were never implemented.
- Files: `src/datasluice/discovery/portal_metadata.py`, `src/datasluice/discovery/fingerprints.py:9-11`, `src/datasluice/discovery/fingerprints.py:24-29`
- Impact: Misleading surface area; tests assert these exist (`tests/unit/discovery/test_discovery.py:14-16`) but the runtime never uses them.
- Fix approach: Either implement HTML-signature-based detection as a fallback when path probing fails, or delete `PortalMetadata` and `HTML_FINGERPRINTS` and update the test.

### `create_adapter` ignores transport; client monkey-patches private attribute

- Issue: `create_adapter()` (`src/datasluice/adapters/factory.py:42-43`) instantiates the adapter without accepting or forwarding a `transport` argument. `DataSluice.__init__` then side-steps this by directly assigning `self.adapter._transport = self._transport` (`src/datasluice/client.py:61`), reaching into the adapter's private attribute. This couples the client to the adapter's internal naming and bypasses the public constructor contract.
- Files: `src/datasluice/adapters/factory.py:14-43`, `src/datasluice/client.py:60-61`
- Impact: Fragile integration — renaming `_transport` inside `BaseAdapter` silently breaks the client; external callers using `create_adapter` directly get a different (lazily-built) transport per adapter instance.
- Fix approach: Add `transport: HttpClient | None = None` parameter to `create_adapter()` and forward it to the adapter constructor.

### Mutable module-level registry singleton

- Issue: `registry = AdapterRegistry()` at `src/datasluice/adapters/registry.py:62` is process-global mutable state. `tests/unit/adapters/test_registry.py:41-56` mutates it at test time. Concurrent tests or plugins that register/unregister adapters can collide; there is no per-instance isolation.
- Files: `src/datasluice/adapters/registry.py:62`, `src/datasluice/adapters/__init__.py:18-21`, `tests/unit/adapters/test_registry.py:41-56`
- Impact: Test-order-dependent failures; plugin ecosystem cannot run multiple isolated registries in one process.
- Fix approach: Accept an optional `registry` parameter on `create_adapter` and `DataSluice.__init__`, defaulting to the global; ensure tests `unregister` in a `finally` block or use a fresh `AdapterRegistry()` instance.

### `SearchResult` not frozen, breaking consistency

- Issue: Every other domain model is `@dataclass(frozen=True)` (`Resource`, `Dataset`, `Organization`, `License`, `Query`), but `SearchResult` (`src/datasluice/domain/result.py:13`) is mutable. This inconsistency plus its `__iter__` method makes it look like a streaming/collection type when it is a single-page snapshot.
- Files: `src/datasluice/domain/result.py:13-35`
- Impact: Subtle aliasing bugs (callers mutate `result.datasets` and break downstream consumers); misleading `for x in result` only walks the current page.
- Fix approach: Freeze `SearchResult`, and either drop `__iter__` (force `.datasets`) or document that iteration is page-local only.

### Hardcoded `DEFAULT_CACHE_DIR` is a relative path

- Issue: `DEFAULT_CACHE_DIR = ".datasluice/cache"` (`src/datasluice/config/defaults.py:9`) is relative to the process CWD. The `FileCache` constructor calls `mkdir(parents=True, exist_ok=True)` (`src/datasluice/io/cache.py:25`), so importing the library and constructing a `DataSluice` client can silently create a `.datasluice/` directory in the caller's working directory.
- Files: `src/datasluice/config/defaults.py:9`, `src/datasluice/io/cache.py:22-25`
- Impact: Library pollutes host application directories; surprising side effect for a library import.
- Fix approach: Default to an OS-appropriate cache location (`~/.cache/datasluice` on POSIX via `os.access`/`tempfile.gettempdir()` fallback), or require explicit opt-in for cache usage.

## Known Bugs

### Format filter ignored in `datasluice download` CLI

- Symptoms: Running `datasluice download -p <portal> <id> -f CSV` downloads ALL resources regardless of format.
- Files: `src/datasluice/cli/download.py:26-37`
- Trigger: Invoke the CLI `download` command with `--format`/`-f`. The filtered list is computed into a local `resources` variable (line 28) but never threaded into `ds.download_all(dataset, dest)` (line 36), which downloads `dataset.resources` directly.
- Workaround: Filter the dataset object's resources before calling `download_all`, or download resources one-by-one via `ds.download(resource, dest)`.

### Silent filter dropping in Socrata / data.gouv adapters

- Symptoms: Multi-valued `tags`/`organizations` filters silently reduce to the first element only.
- Files: `src/datasluice/adapters/socrata/adapter.py:34-35` (`params["tags"] = query.tags[0]`), `src/datasluice/adapters/datagouv/adapter.py:37-40`
- Trigger: `ds.search(Query(tags=["climate", "weather"]))` against a Socrata or data.gouv portal — only `"climate"` is sent.
- Workaround: Combine filters into `query.text` until the adapter supports multi-value params.

### `pandas` integration reads files instead of URLs

- Symptoms: `resource_to_dataframe(resource)` for a remote `Resource` raises `FileNotFoundError` or returns empty when given a URL.
- Files: `src/datasluice/integrations/pandas.py:28-34`
- Trigger: Pass any `Resource` whose `url` is an HTTP URL. Line 31 calls `reader.read(resource.url)` — the format readers (`CSVReader`, `JSONReader`, etc.) treat `str` inputs as local file paths (`src/datasluice/formats/csv.py:27-30`, `src/datasluice/formats/json.py:21-24`), not URLs.
- Workaround: Download the resource first with `ds.download(resource)` and pass the local path.

### Hardcoded US Socrata host in resource URLs

- Symptoms: Socrata resource download URLs always point at `api.us.socrata.com` even for EU/APAC Socrata instances.
- Files: `src/datasluice/adapters/socrata/mapper.py:16`
- Trigger: `map_resource()` against any non-US Socrata portal produces a download URL on the wrong region.
- Workaround: Read the canonical URL from `resource.extra` rather than `resource.url`.

### `float(retry_after)` can raise on HTTP-date values

- Symptoms: A portal returning `Retry-After: Wed, 05 Jul 2026 12:00:00 GMT` (an HTTP-date) crashes the retry path with `ValueError`.
- Files: `src/datasluice/transport/http_client.py:93-96`
- Trigger: Any 429 response whose `Retry-After` header is an HTTP-date rather than a delta-seconds integer.
- Workaround: Catch `ValueError` around the `float(retry_after)` conversion and fall back to exponential backoff.

### `RateLimiter` serializes threads by sleeping under the lock

- Symptoms: Under concurrent use, only one thread can be in `acquire()` at a time; the others block on the lock even just to compute their own wait, defeating the throughput purpose of a token bucket.
- Files: `src/datasluice/transport/rate_limit.py:27-34`
- Trigger: Multiple threads sharing a `RateLimiter` instance.
- Workaround: None within the current API; release the lock before `time.sleep`.

### `_normalize_base_url` silently truncates path/query

- Symptoms: Passing `https://data.gov.uk/some/path?x=1` to `detect_portal_type` drops `/some/path?x=1` and probes only `https://data.gov.uk`.
- Files: `src/datasluice/discovery/detector.py:18-23`
- Trigger: Any portal URL that requires a path prefix to reach its API.
- Workaround: Pass the bare origin URL.

### `safe_filename` allows spaces

- Symptoms: Resources named `My Data.csv` produce files literally named `My Data.csv`, which break downstream shell pipelines that don't quote.
- Files: `src/datasluice/io/local.py:43-45`
- Trigger: Any download where `resource.name` contains a space.
- Workaround: Pass an explicit `filename=` to `Downloader.download`.

## Security Considerations

### SQL injection in DuckDB integration

- Risk: Arbitrary SQL execution if a caller passes user-controlled `resource_url` or `table_name`.
- Files: `src/datasluice/integrations/duckdb.py:33-38`, `src/datasluice/integrations/duckdb.py:44-47`
- Current mitigation: None. `table_name` and `resource_url` are f-string-interpolated directly into `CREATE OR REPLACE VIEW ... AS SELECT * FROM read_csv_auto('{resource_url}')` and into `query_resource`'s `sql` argument with no quoting, allow-listing, or parameterization.
- Recommendations: Quote identifiers with `duckdb`'s identifier-quoting helper or wrap `table_name` in double quotes after rejecting names containing `"`. For `resource_url`, use DuckDB's parameterised `read_csv_auto` invocation via `duckdb.sql("... FROM read_csv_auto(?)", [url])`. Document that `query_resource(sql=...)` is intentionally arbitrary SQL and is the caller's responsibility.

### Secret material leaks through default `__repr__`

- Risk: `APIKeyAuth`, `BearerAuth`, `BasicAuth`, and `HeadersAuth` rely on the default dataclass-style `__repr__` produced by `object.__repr__`. None define `__repr__`, so debugger inspection, `print(auth)`, or accidental log emission exposes the API key, bearer token, base64-encoded `user:pass`, or custom header secrets.
- Files: `src/datasluice/auth/api_key.py:14-50`, `src/datasluice/auth/bearer.py:10-27`, `src/datasluice/auth/basic.py:11-29`, `src/datasluice/auth/headers.py:13-28`
- Current mitigation: None.
- Recommendations: Define `__repr__` on each auth class that redacts the secret (e.g. `f"<APIKeyAuth header={self.header_name!r} key=***>"`) or store secrets in a non-repr attribute.

### No TLS verification configuration surface

- Risk: Users have no documented way to disable certificate verification for self-signed portals, leading them to either fail silently or monkey-patch `ssl._create_unverified_context` globally.
- Files: `src/datasluice/transport/http_client.py:84`
- Current mitigation: `urllib.request.urlopen` uses the default verified SSL context, but no `ssl_context` / `verify` parameter exists on `HttpClient`.
- Recommendations: Add an `ssl_context: ssl.SSLContext | None = None` (or `verify: bool = True`) parameter to `HttpClient.__init__`, document it loudly, and never default to disabling verification.

### `LocalStorage.write` is vulnerable to path traversal

- Risk: If `key` is constructed from user-supplied data (e.g. `dataset_id`), a value like `../../../etc/passwd` escapes the storage base directory.
- Files: `src/datasluice/io/storage.py:42-44`
- Current mitigation: None. The key is concatenated with `base_dir` directly.
- Recommendations: Resolve the final path and assert it remains inside `base_dir` (`Path.resolve().relative_to(self.base_dir.resolve())`), rejecting traversal attempts with a `DownloadError`.

### Portal errors and response bodies logged at debug level

- Risk: Error responses from authenticated endpoints may contain echoed query parameters or tokens; they are concatenated into the `PortalError` message (`src/datasluice/transport/http_client.py:91-97`) and logged.
- Files: `src/datasluice/transport/http_client.py:88-99`
- Current mitigation: None.
- Recommendations: Truncate response bodies before interpolating into exception messages; avoid logging headers.

## Performance Bottlenecks

### Entire HTTP response read into memory

- Problem: Every download and every JSON fetch loads the full response body into a single `bytes` object via `resp.read()` (`src/datasluice/transport/http_client.py:85`, `src/datasluice/io/downloader.py:74-77`).
- Files: `src/datasluice/transport/http_client.py:78-101`, `src/datasluice/io/downloader.py:74-97`
- Cause: `urllib.request.urlopen` is used synchronously with a single-shot `read()`.
- Improvement path: Add a streaming download path that writes chunks to disk in a loop (use `resp.read(CHUNK_SIZE)`), expose it on `Downloader.download` for large resources, and consider `urllib3` or `httpx` for connection reuse.

### Sequential multi-resource downloads

- Problem: `Downloader.download_many` and `DataSluice.download_all` fetch resources strictly sequentially with no concurrency.
- Files: `src/datasluice/io/downloader.py:99-113`, `src/datasluice/client.py:128-130`
- Cause: Plain `for resource in resources: self.download(...)` loop.
- Improvement path: Use a bounded `ThreadPoolExecutor` or `asyncio` task pool that respects the configured `RateLimiter`. Default concurrency should be small (e.g. 4) to stay friendly to portals.

### Format readers materialise entire datasets into memory

- Problem: Every reader returns `list[dict[str, Any]]` — `ParquetReader.read` calls `table.to_pylist()`, `XLSXReader.read` walks every row into a dict, `JSONReader.read` accumulates the full record list. None provide a streaming/iterator API.
- Files: `src/datasluice/formats/parquet.py:34`, `src/datasluice/formats/xlsx.py:40-42`, `src/datasluice/formats/json.py:31-54`, `src/datasluice/formats/csv.py:32-35`, `src/datasluice/formats/base.py:19-25`
- Cause: The `BaseFormatReader.read` contract fixes the return type to `list[dict[str, Any]]`.
- Improvement path: Add an `iter_rows(source) -> Iterator[dict[str, Any]]` method to `BaseFormatReader` and refactor consumers to consume lazily; keep `read` as a convenience wrapper that materialises.

### Portal auto-detection probes endpoints sequentially and downloads full bodies

- Problem: `detect_portal_type` walks `PATH_FINGERPRINTS` in dict order, issues a full `client.request(probe_url)` (which reads the entire body) per probe, and stops on the first success.
- Files: `src/datasluice/discovery/detector.py:45-58`
- Cause: Sequential `for ... client.request(...)`.
- Improvement path: Probe endpoints concurrently (or use `HEAD` requests) and short-circuit on first 2xx.

## Fragile Areas

### Adapter ↔ transport wiring depends on private attribute assignment

- Files: `src/datasluice/client.py:60-61`, `src/datasluice/adapters/base.py:37-46`
- Why fragile: `DataSluice.__init__` writes `self.adapter._transport = self._transport` after construction, then `BaseAdapter.transport` (the property) returns the lazily-initialised fallback if `_transport` is `None`. Any change to either side silently breaks the optimisation.
- Safe modification: Until the factory accepts a `transport` argument, do not rename `_transport` and do not construct adapters via `create_adapter` from paths that also build a transport.
- Test coverage: No tests cover the "client injects transport into adapter" wiring.

### Socrata mapper uses chained ternary for `themes`

- Files: `src/datasluice/adapters/socrata/mapper.py:54-58`
- Why fragile: The expression nests three ternaries to coerce `domain_category` (string | list | None) into `list[str]`. A portal returning an unexpected shape (e.g. dict) silently produces `[]` or a confusing error.
- Safe modification: Extract into an explicit helper function with `isinstance` branches.
- Test coverage: No unit tests for `map_dataset` edge cases on the Socrata mapper (only CKAN has mapper tests).

### JSON reader silently drops malformed JSONL lines and non-dict array items

- Files: `src/datasluice/formats/json.py:32-44` (`except json.JSONDecodeError: continue`), `src/datasluice/formats/json.py:51-52` (`[r for r in data if isinstance(r, dict)]`)
- Why fragile: A single corrupted line in a JSONL stream is dropped without a log message; the caller cannot distinguish "10 records" (clean) from "10 records after dropping 5 malformed". Same for non-dict items in JSON arrays.
- Safe modification: Add a `strict: bool = False` parameter that, when `True`, raises `FormatError` on the first malformed line; always emit a `logger.warning` with the dropped line count.
- Test coverage: `tests/unit/formats/test_formats.py` has no test for malformed JSONL or heterogeneous arrays.

### Rate limiter state is mutable on a frozen-looking dataclass

- Files: `src/datasluice/transport/rate_limit.py:10-25`
- Why fragile: `@dataclass` without `frozen=True` plus a `__post_init__` that adds three new attributes (`_min_interval`, `_last_request`, `_lock`) — dataclass-generated `__eq__`/`__repr__` will not see them, and equality comparisons between limiters are surprising.
- Safe modification: Treat `RateLimiter` as opaque; do not compare instances or rely on dataclass-generated `__repr__`.
- Test coverage: Only a happy-path throttle test exists (`tests/unit/transport/test_transport.py:47-51`); no concurrency or `__post_init__` validation tests.

## Scaling Limits

### In-memory cache with no size cap

- Current capacity: Unbounded — `FileCache` (`src/datasluice/io/cache.py`) only enforces TTL on read; there is no max-bytes or max-entries eviction. `clear()` is the only way to reclaim space.
- Limit: Filesystem fills up; large portals with many resources can exhaust disk.
- Scaling path: Add an LRU eviction policy keyed on access time (use `path.stat().st_atime` or maintain an access log) plus a configurable `max_bytes`.

### Single-process registry with no plugin discovery

- Current capacity: Only the four built-in adapters; custom adapters must be registered imperatively at runtime.
- Limit: Distribution as a library that needs N third-party adapters requires every consumer to wire imports.
- Scaling path: Adopt a `datasluice.adapters` entry-point group so installed packages can self-register without application code changes.

### No async API surface

- Current capacity: All IO is synchronous via `urllib`.
- Limit: Cannot integrate natively with `asyncio`/`FastAPI`/`Airflow` async operators without thread-pool wrapping.
- Scaling path: Either add an `AsyncHttpClient` parallel to `HttpClient`, or document thread-pool usage and provide a `concurrent.futures`-based helper.

## Dependencies at Risk

### `urllib`-only HTTP stack

- Risk: No connection pooling, no HTTP/2, no first-class retry/observability hooks, no `Response` object abstraction. Maintaining feature parity with `httpx`/`requests` means reimplementing well-tested behaviour.
- Impact: Every performance improvement (streaming, keep-alive, async) requires either rewriting `HttpClient` or bolting on a parallel implementation.
- Migration plan: Introduce `httpx` as an optional dependency behind a `HttpxHttpClient` adapter that satisfies the same surface, then migrate the default once the optional path is proven.

### Optional-dependency sprawl without a compatibility matrix

- Risk: Seven optional extras (`pandas`, `polars`, `dlt`, `duckdb`, `airflow`, `parquet`, `xlsx`) with independent version ranges unset in `pyproject.toml`. CI installs `--all-extras` so drift is invisible until a user installs one extra against an incompatible version.
- Impact: User-facing `ImportError` messages are good, but no upper bounds means a breaking release in any dependency becomes a datasluice bug.
- Migration plan: Add loose upper bounds (`<2.0` style) per optional dep and add a CI matrix job that installs each extra individually.

## Missing Critical Features

### No automatic pagination iterator

- Problem: `DataSluice.search()` returns a single `SearchResult` page. There is no `search_all()` or generator that walks every page up to `total`. `paginate()` exists but is unused.
- Blocks: Bulk harvest use cases; the `dlt` integration can only harvest one page per resource.

### No streaming download progress reporting

- Problem: `Downloader.download` returns only after the full file is on disk. There is no callback/hook for progress bars, no `rich.progress` integration in the CLI, no `Content-Length` awareness.
- Blocks: Friendly UX for large file downloads; cancellation mid-stream.

### No retry budget / deadline

- Problem: `RetryPolicy` controls per-request attempts but there is no overall deadline (e.g. "give up after 60s total"). A flaky portal can consume `max_attempts * timeout` per request.
- Blocks: Predictable wall-clock budgets in pipelines.

### No request/response logging context

- Problem: `HttpClient.request` logs only `%s %s` (method, URL). There is no request ID, no elapsed-time log, no correlation with the calling adapter/action.
- Blocks: Debugging production issues; observability for ops teams.

## Test Coverage Gaps

### HttpClient has zero unit tests

- What's not tested: `HttpClient.request`, error mapping (`HTTPError` → `PortalError`/`RateLimitError`), `get_json`, `download`, auth-application branching at `src/datasluice/transport/http_client.py:69-72`.
- Files: `src/datasluice/transport/http_client.py` (entire file)
- Risk: The 429 → `RateLimitError.retry_after` parsing bug, the auth-apply branching, and the URL-encoding path are all untested.
- Priority: High — this is the most important untested module.

### Downloader and IO pipeline untested

- What's not tested: `Downloader.download`, `Downloader.download_many`, cache hit/miss integration with download, checksum verification path (`src/datasluice/io/downloader.py:79-90`), the `verify_hash` parameter.
- Files: `src/datasluice/io/downloader.py` (entire file)
- Risk: The documented checksum verification feature is untested; the format-filter CLI bug went unnoticed because no end-to-end test exercises download.
- Priority: High.

### CLI commands have no tests

- What's not tested: `search`, `inspect`, `download`, `detect`, `app` (Typer entry). The format-filter bug in `cli/download.py` is a direct consequence.
- Files: `src/datasluice/cli/app.py`, `src/datasluice/cli/search.py`, `src/datasluice/cli/inspect.py`, `src/datasluice/cli/download.py`, `src/datasluice/cli/detect.py`
- Risk: Any CLI regression ships undetected.
- Priority: High — use `typer.testing.CliRunner`.

### Socrata and data.gouv mappers untested

- What's not tested: `src/datasluice/adapters/socrata/mapper.py` and `src/datasluice/adapters/datagouv/mapper.py` (only CKAN mapper has tests at `tests/unit/adapters/test_ckan_mapper.py`).
- Risk: Theme-coercion ternary, multi-value filter dropping, and language coercion in datagouv mapper are untested.
- Priority: Medium.

### `tests/integration/` and `tests/fixtures/` are empty

- What's not tested: Any live adapter behaviour against recorded fixtures. The directories exist only as `.gitkeep` placeholders.
- Files: `tests/integration/ckan/.gitkeep`, `tests/integration/socrata/.gitkeep`, `tests/integration/datagouv/.gitkeep`, `tests/fixtures/ckan/.gitkeep`, `tests/fixtures/socrata/.gitkeep`, `tests/fixtures/datagouv/.gitkeep`
- Risk: Mappers and adapters can ship with shape mismatches that only manifest against real portal responses.
- Priority: Medium — record fixture JSON from each portal's docs and add fixture-driven mapper tests.

### CI does not enforce the coverage threshold

- What's not tested: The `fail_under = 50` setting in `pyproject.toml:50` is informational only. The CI coverage job (`/.github/workflows/ci.yml:92-100`) runs `coverage combine` and `coverage report` but never passes `--fail-under=50`, so coverage can drop below 50% without failing CI.
- Risk: Coverage regression is invisible.
- Priority: Medium — append `--fail-under=50` to the CI `coverage report` step.

### Concurrency and retry timing untested

- What's not tested: `with_retry` backoff timing, `RateLimiter` under contention, `RetryPolicy.retry_on` matching.
- Files: `src/datasluice/transport/retry.py`, `src/datasluice/transport/rate_limit.py`
- Risk: The lock-while-sleeping bug and the `Retry-After` HTTP-date parsing bug both went unnoticed.
- Priority: Medium.

---

*Concerns audit: 2026-07-05*
