# Codebase Concerns

**Analysis Date:** 2026-07-26

## Tech Debt

**Stale AGENTS.md / docs reference a renamed package**
- Issue: `AGENTS.md` (lines 44–49), `docs/architecture.md` (line 40), and `docs/adapters.md` (lines 9, 37) describe `datasluice.adapters` with a **module-level `registry` singleton** and "auto-registration on import." The actual code ships `datasluice.connectors` backed by an entry-points-driven `PluginManager` (`src/datasluice/runtime/plugin_manager.py`) with no module-level singleton (intentionally, ARCH-06). `pyproject.toml:81-84` declares the `[project.entry-points."datasluice.connectors"]` group.
- Files: `AGENTS.md`, `docs/architecture.md`, `docs/adapters.md`
- Impact: New contributors following AGENTS.md look in the wrong directory and expect an import side effect that no longer exists; doc-driven code review against `docs/adapters.md` will propose patterns the codebase has explicitly removed.
- Fix approach: Rewrite the three doc files to reference `datasluice.connectors`, the entry-points group, and `PluginManager`. Remove the "module-level `registry`" language and the "importing triggers side-effect registration" claim.

**Dual HTTP transports with divergent security semantics**
- Issue: Two transports coexist — `HttpClient` (`src/datasluice/transport/http_client.py`, urllib-backed) and `HttpxTransport` (`src/datasluice/transport/httpx_transport.py`, httpx-backed). Only `HttpxTransport` implements the 401/403 credential-eviction-and-refresh path (D-P3-15). The lazy default in `BaseAdapter.transport` (`src/datasluice/connectors/base.py:43-45`) constructs `HttpClient`, so the eviction path is **off by default** unless a caller explicitly injects an `HttpxTransport`.
- Files: `src/datasluice/transport/http_client.py`, `src/datasluice/transport/httpx_transport.py`, `src/datasluice/connectors/base.py:39-46`
- Impact: Security/correctness behaviour depends on which transport is wired; the zero-config path is the less-secure one.
- Fix approach: Either promote `HttpxTransport` to the default in `BaseAdapter.transport` (gated by the `http` extra, falling back to `HttpClient` only when httpx is absent), or document the divergence loudly and add a runtime warning when `HttpClient` is used with a `HostCredentialProvider`.

**Duplicated redirect-credential-stripping logic**
- Issue: The "strip sensitive headers on cross-origin/scheme-downgrade redirect" policy is implemented twice: once for urllib in `CredentialAwareRedirectHandler.redirect_request` (`src/datasluice/transport/redirect.py:30-60`) and once for httpx in `HttpxTransport._should_strip_authorization` (`src/datasluice/transport/httpx_transport.py:129-150`). The two implementations must be kept in sync by hand — there is no shared predicate.
- Files: `src/datasluice/transport/redirect.py`, `src/datasluice/transport/httpx_transport.py`
- Impact: A future fix to one (e.g. a new sensitive header, a `port`-aware same-origin check) will silently drift from the other. Both are security-relevant (SEC-01/SEC-02).
- Fix approach: Extract a single pure function `should_strip_credentials(old_url, new_url, scope) -> bool` in `redirect.py` (or a new `redirect_policy.py`) and call it from both transports. Add a shared parametric test that exercises the function against the SEC-01/SEC-02 matrix.

**`__version__` resolves to `"0.0.0"` in source checkouts**
- Issue: `src/datasluice/_version.py:12-15` reads the version from installed package metadata via `importlib.metadata.version("datasluice")` and falls back to `"0.0.0"` on `PackageNotFoundError`. In a bare `git clone` without `uv sync`/`uv install`, every `datasluice --version` call and `User-Agent` (which embeds the version via `transport/user_agent.py`) reports `0.0.0`.
- Files: `src/datasluice/_version.py`, `src/datasluice/transport/user_agent.py`
- Impact: Misleading version strings in dev and in any environment where the package isn't installed under its metadata name; user-agent-based portal analytics undercount real versions.
- Fix approach: Keep the `importlib.metadata` lookup as primary, but fall back to reading `version` from `pyproject.toml` (via `tomllib` when `__file__` resolution permits) before defaulting to `"0.0.0"`. Document the fallback.

**`RetryableHTTPError` defined but not exported from the package root**
- Issue: `src/datasluice/exceptions.py:38` defines `RetryableHTTPError`, but `src/datasluice/__init__.py:26-39` omits it from both the import block and `__all__`. Every other exception in the hierarchy is exported.
- Files: `src/datasluice/__init__.py`, `src/datasluice/exceptions.py`
- Impact: Callers cannot `from datasluice import RetryableHTTPError` to catch 5xx retries uniformly; they must reach into `datasluice.exceptions` — inconsistent with the rest of the exception API surface.
- Fix approach: Add `RetryableHTTPError` to the `from datasluice.exceptions import (...)` block and to `__all__` in `__init__.py`.

**`CustomAdapter` raises `NotImplementedError` instead of staying abstract**
- Issue: `src/datasluice/connectors/custom/adapter.py:13-28` subclasses `BaseAdapter` (an `ABC`) but overrides every abstract method with a concrete `raise NotImplementedError`. This defeats the ABC: instantiation succeeds, and the failure surfaces only at call time rather than at construction.
- Files: `src/datasluice/connectors/custom/adapter.py`
- Impact: A user who instantiates `CustomAdapter` directly gets no error until they call a method — confusing for the "template to copy" use case the module documents.
- Fix approach: Either leave the methods abstract (don't override them) so `CustomAdapter` itself is uninstantiable, or document explicitly that it is a skeleton and raise `TypeError`/`NotImplementedError` from `__init__` with a clear message pointing at the copy instructions.

**CLI `--version` uses `typer.Option(...)` as a default argument**
- Issue: `src/datasluice/cli/app.py:25-31` writes `version: bool = typer.Option(False, "--version", "-V", ...)`. The project's `AGENTS.md` style guide (and ruff's `B008` from flake8-bugbear, which is in the selected rule set) forbids function calls in argument defaults and mandates the `Annotated[bool, typer.Option(...)]` form.
- Files: `src/datasluice/cli/app.py`
- Impact: Inconsistent with the documented convention; will trip B008 if/when the rule is enforced on the CLI module, and sets a bad template for new CLI subcommands.
- Fix approach: Rewrite the signature as `version: Annotated[bool, typer.Option(False, "--version", "-V", help=..., is_eager=True)] = False`.

## Known Bugs

**DuckDB format detection rejects URLs with query strings**
- Symptoms: `resource_to_relation("https://portal.example/dataset.csv?token=abc")` raises `ValueError("Unsupported resource format for DuckDB: ...")`.
- Files: `src/datasluice/integrations/duckdb.py:60-68`
- Trigger: Any resource URL whose path ends in `.csv`/`.parquet`/`.json` but carries a query string (signed S3 URLs, Socrata tokens, CKAN `?download=1`) — common in open-data portals.
- Workaround: None without stripping the query string before calling.
- Fix: Strip the query/fragment before the suffix check, e.g. `path = urllib.parse.urlparse(resource_url).path; lowered = path.lower()`.

**`Downloader.download_many` silently swallows per-resource failures**
- Symptoms: Returns only the paths of resources that downloaded successfully; logs the failures at ERROR and continues. The caller cannot distinguish "1 of 10 succeeded" from "10 of 10 succeeded" without counting.
- Files: `src/datasluice/io/downloader.py:99-113`, and the same pattern in `src/datasluice/integrations/airflow.py:62-73`.
- Trigger: Any `download_many` / Airflow operator run where one resource 404s or its checksum fails.
- Workaround: Call `download` individually and handle `DownloadError` yourself.
- Fix: Return a structured result (e.g. `DownloadBatch(results=[...], failures=[...])`) or raise an `AggregateError` after collecting all failures. At minimum, log the count of failures at WARNING and document the partial-success contract in the docstring.

**Adapters return stub objects on not-found instead of raising `NotFoundError`**
- Symptoms: `CKANAdapter.get_organization` returns `Organization(id=organization_id)` when `map_organization` returns `None` (`src/datasluice/connectors/ckan/adapter.py:59-61`); `SocrataAdapter.get_dataset` returns `Dataset(id=dataset_id)` when the catalog returns no results (`src/datasluice/connectors/socrata/adapter.py:51-53`). The caller cannot distinguish a real (but sparse) record from a 404.
- Files: `src/datasluice/connectors/ckan/adapter.py:55-61`, `src/datasluice/connectors/socrata/adapter.py:47-53`
- Trigger: Any lookup against an ID that the portal does not have.
- Workaround: Inspect `extra` for emptiness — fragile and undocumented.
- Fix: Raise `NotFoundError` (already declared in `src/datasluice/exceptions.py:46` but currently unused) on genuine misses. If the stub behaviour is intentional for some portals, gate it behind a `strict=False` flag.

**`int(result.get("count", ...))` can raise unhandled `ValueError`**
- Symptoms: An uncaught `ValueError` if a portal returns `count`/`resultSetSize` as a non-numeric string.
- Files: `src/datasluice/connectors/ckan/adapter.py:37`, `src/datasluice/connectors/socrata/adapter.py:38`, and likely `src/datasluice/connectors/datagouv/adapter.py`.
- Trigger: Malformed portal response (rare but observed on some CKAN forks that return `"123"` as a string, which `int()` accepts, versus `"unknown"`, which it does not).
- Fix: Wrap in a `try/except (TypeError, ValueError)` and fall back to `len(datasets)` with a DEBUG log, or coerce explicitly via `str(...).isdigit()`.

**Mapper crashes on non-dict tag/group entries**
- Symptoms: `AttributeError: 'str' object has no attribute 'get'` when a CKAN package's `tags` or `groups` is a list of strings (some CKAN forks serialise tags as `["health", "climate"]` rather than `[{"name": "health"}, ...]`).
- Files: `src/datasluice/connectors/ckan/mapper.py:65-66`
- Trigger: A portal whose CKAN serialisation differs from the canonical `package_search` shape.
- Fix: Guard with `isinstance(t, dict)` before `.get`, falling back to `str(t)`.

**Pandas integration reads parquet/xlsx via dict records, not native readers**
- Symptoms: `resource_to_dataframe` for `PARQUET`/`XLSX` goes `get_reader(fmt).read(url) -> list[dict] -> pd.DataFrame(records)`, losing dtype information, datetime parsing, and column-level options, and is significantly slower than `pd.read_parquet`/`pd.read_excel`.
- Files: `src/datasluice/integrations/pandas.py:28-34`
- Trigger: Any parquet/xlsx resource loaded through the pandas integration.
- Fix: Branch on format and call `pd.read_parquet`/`pd.read_excel`/`pd.read_csv`/`pd.read_json` directly with `storage_options`/`**kwargs`, bypassing the `formats/` readers for the DataFrame path.

## Security Considerations

**Path traversal in `FsspecStorage._resolve` when `base_uri` is empty**
- Risk: `src/datasluice/io/fsspec_storage.py:64-74` returns `path` unchanged when it does not start with a known URI scheme and `base_uri` is empty. A caller-supplied `path` like `../../etc/passwd` (or a resource `name` containing `../` that flows through `Downloader.download -> storage.write`) is resolved by the local fsspec backend against the process CWD, allowing escape from the intended storage root.
- Files: `src/datasluice/io/fsspec_storage.py`, call site `src/datasluice/io/downloader.py:92-94` (filename derived from `safe_filename(resource.name ...)`, which permits `.` and spaces).
- Current mitigation: `safe_filename` (`src/datasluice/io/local.py:43-45`) strips most punctuation but explicitly **allows** `.`, `/` is not in the allowed set so path separators are dropped — `..` becomes `..` (two dots, no separator) which is harmless. The residual risk is direct calls to `FsspecStorage.write(data, path)` with an attacker-influenced `path`.
- Recommendations: (1) In `FsspecStorage._resolve`, normalise the joined path with `posixpath.normpath` and reject any result that escapes `base_uri` when `base_uri` is set. (2) When `base_uri` is empty, refuse bare paths that contain `..` segments. (3) Add a security-focused test for the traversal vector.

**`duckdb.query_resource` is a raw-SQL passthrough**
- Risk: `src/datasluice/integrations/duckdb.py:73-91` executes the `sql` argument verbatim via `con.execute(sql)`. DuckDB SQL can read arbitrary local files (`read_csv_auto('/etc/passwd')`, `read_blob(...)`) and make network requests — effectively RCE-equivalent for the DuckDB process. The docstring warns the caller, but nothing enforces it.
- Files: `src/datasluice/integrations/duckdb.py`
- Current mitigation: Documented as "intentionally opt-in raw-SQL passthrough"; `_validate_table_name` protects the *table name* but not the SQL body.
- Recommendations: (1) Keep the passthrough but rename to `query_resource_raw` / require an explicit `allow_raw_sql=True` kwarg so callers cannot trigger it by accident. (2) Document a safe alternative that builds queries from the relation API (`resource_to_relation(...).filter(...).select(...)`).

**No CRLF / control-char validation on header names and values**
- Risk: `APIKeyAuth` (`src/datasluice/auth/api_key.py:54`) and `HeadersAuth` (`src/datasluice/auth/headers.py:20-30`) accept arbitrary strings as header names and values. A header value containing `\r\n` enables HTTP header injection / response splitting on transports that don't sanitise (urllib does not; httpx does some validation).
- Files: `src/datasluice/auth/api_key.py`, `src/datasluice/auth/headers.py`
- Current mitigation: None. The `api_key` itself is caller-supplied, but portal-discovered credential values (e.g. from a `HostCredentialProvider` refresher) could carry attacker-controlled bytes.
- Recommendations: Add a `_validate_header(name, value)` helper that rejects any name/value containing `\r`, `\n`, or other ASCII control chars, and call it from both `apply` methods.

**`logging.RedactingFilter` only scans top-level log-record attributes**
- Risk: `src/datasluice/logging.py:56-71` walks `record.__dict__` and `record.args` dicts, redacting only values whose **key** appears in `_SENSITIVE_KEYS`. Structured log records that nest a credential inside a dict value (e.g. `logger.info("resp: %s", {"headers": {"authorization": "Bearer ..." }})`) are **not** redacted — the outer key is `"resp"` / positional, not a sensitive key.
- Files: `src/datasluice/logging.py`
- Current mitigation: Targeted key-list (no value-pattern heuristics) is an explicit design choice (RESEARCH Pitfall 6) to avoid corrupting base64 payloads.
- Recommendations: Document this limitation in the docstring. Consider a recursive walker with a depth cap (e.g. 2 levels) that redacts nested sensitive keys. Add a regression test for the nested-dict case.

**Domain models retain the entire raw portal payload in `extra`**
- Risk: `map_dataset`/`map_resource`/`map_organization` store the full upstream JSON dict in the model's `extra` field (`src/datasluice/connectors/ckan/mapper.py:33, 49, 70`; same in socrata/datagouv mappers). If a portal response includes credentials, PII, or internal metadata, those bytes are carried in the domain object, survive `__repr__`/logging by default, and can be serialised by integrations (dlt/airflow) into downstream stores.
- Files: `src/datasluice/connectors/*/mapper.py`
- Current mitigation: None.
- Recommendations: Add an opt-in `strip_extra=True` flag on the mappers (or a session-level `redact_extra_fields` allowlist) that drops or redacts unknown keys before they enter the domain model. At minimum, exclude `extra` from `__repr__`.

**Portal-detection probe swallows TLS errors silently**
- Risk: `src/datasluice/discovery/detector.py:48-57` wraps each portal probe in `try: client.request(...) except Exception: continue`. A TLS verification failure (expired cert, hostname mismatch, MITM) on the *real* portal is indistinguishable from "this isn't a CKAN portal," so the detector silently moves on and ultimately raises a generic `PortalDetectionError`.
- Files: `src/datasluice/discovery/detector.py`
- Current mitigation: None.
- Recommendations: Narrow the swallowed exception set to `PortalError` (transport-level) and re-raise/propagate TLS errors as a distinct `PortalDetectionError` subtype (e.g. `PortalSecurityError`) so users see the real cause.

## Performance Bottlenecks

**Portal detection constructs a throwaway transport + plugin manager per call**
- Problem: `detect_portal_type` (`src/datasluice/discovery/detector.py:45-46`) builds a fresh `HttpClient()` and a fresh `PluginManager()` (which triggers an `entry_points()` scan + plugin loads) on every invocation. `DataSluiceSession.portal()` calls it (`src/datasluice/runtime/session.py:185`) and the session already holds its own `PluginManager`, so detection duplicates the work and ignores the session's configured transport, auth, timeout, and retry policy.
- Files: `src/datasluice/discovery/detector.py`, `src/datasluice/runtime/session.py:183-190`
- Cause: Detection is a free function with no access to the session.
- Improvement path: Make detection a method on the session (or accept an injected `transport` + `plugin_manager`), reusing the session's already-built instances. Memoise `entry_points()` results if profiling shows the scan is hot.

**`entry_points()` scanned eagerly on every `PluginManager()` construction**
- Problem: `src/datasluice/runtime/plugin_manager.py:45` iterates `entry_points(group=group)` in `__init__`. With the `detector.py` throwaway above, a single `session.portal(url)` triggers **two** full entry-point scans + plugin loads.
- Files: `src/datasluice/runtime/plugin_manager.py`, `src/datasluice/discovery/detector.py`
- Cause: Eager discovery in the constructor (ARCH-05).
- Improvement path: Cache the parsed entry points at module level keyed by group (they don't change at runtime), or move discovery behind a `PluginManager.discover()` classmethod that memoises.

**`ContentCache` opens a new SQLite connection on every method call**
- Problem: `src/datasluice/io/content_cache.py:79-84` documents that "SQLite connections are short-lived: each method opens a fresh connection." Every `get`/`put`/`delete`/`get_metadata` therefore pays `sqlite3.connect` + two `PRAGMA` round-trips (WAL mode, busy_timeout).
- Files: `src/datasluice/io/content_cache.py`
- Cause: Explicit design choice (no global connection held, no `close()` needed) to avoid lifecycle bugs.
- Improvement path: Acceptable for low-frequency catalog access. For the Phase 4 download path with many `put`s in a loop, add a `ContentCache.bulk_put(iterable)` that holds one connection for the batch, or thread a connection through a context manager. Profile before changing.

**CSV / Parquet readers load the entire file into memory**
- Problem: `CSVReader.read` (`src/datasluice/formats/csv.py:27-35`) decodes the whole file then constructs `csv.DictReader` over an in-memory `StringIO`; `ParquetReader.read` (`src/datasluice/formats/parquet.py:21-34`) calls `pq.read_table(...).to_pylist()`. For multi-GB open-data files (common for geospatial and census datasets), this OOMs the process.
- Files: `src/datasluice/formats/csv.py`, `src/datasluice/formats/parquet.py`
- Cause: Return-type contract is `list[dict]`.
- Improvement path: Add a streaming variant returning an iterator (`read_iter`), or document an upper size limit and raise `FormatError` above it. For parquet, prefer `RecordBatchReader` and yield per-batch.

**`os.environ.get("DATASLUICE_NO_REDACT")` read on every log record**
- Problem: `RedactingFilter.filter` (`src/datasluice/logging.py:57`) calls `os.environ.get(...)` for every log record emitted. `os.environ.get` is a dict lookup but involves string interning and is on the hot path of every `logger.debug` call in the transport layer.
- Files: `src/datasluice/logging.py`
- Improvement path: Read the env var once at filter construction (or module import) into a boolean `_REDACT_ENABLED`, and provide a `set_redaction(bool)` escape hatch for tests. Re-read only if explicitly refreshed.

**`HostCredentialProvider` credential and lock dicts grow unboundedly**
- Problem: `_cache` and `_host_locks` (`src/datasluice/credentials/host_provider.py:65-66`) are plain dicts keyed by host. Every unique host ever queried adds an entry that is never pruned. A long-running process that discovers many portal hosts (crawler, Airflow scheduler) leaks memory linearly with host count.
- Files: `src/datasluice/credentials/host_provider.py`
- Improvement path: Cap the cache (e.g. `cachetools.LRUCache(maxsize=1024)`) or add a `prune(inactive_since: float)` method the session can call periodically. Lock dict pruning is harder (a lock may be in flight); consider a `WeakValueDictionary` for `_host_locks`.

**Portal-detection probe downloads the full response body**
- Problem: `src/datasluice/discovery/detector.py:53` calls `client.request(probe_url)` (a GET) and reads the entire body just to confirm reachability. Some CKAN `/api/3/action/package_show` endpoints return large JSON.
- Files: `src/datasluice/discovery/detector.py`
- Improvement path: Issue a `HEAD` (or `GET` with `Range: bytes=0-0`) for the probe; only fall back to a body-bearing request if the portal rejects HEAD.

## Fragile Areas

**Airflow integration reaches into `DataSluiceSession._transport`**
- Files: `src/datasluice/integrations/airflow.py:71` — `Downloader(cast("Any", ds._transport))`.
- Why fragile: Accesses a private attribute from outside the class. Renaming `_transport` (a private symbol the session is free to rename) silently breaks the Airflow operator at runtime, with no type-checker or test coverage to catch it (Airflow integration is 23% covered). The `cast("Any", ...)` hides the access from `ty`.
- Safe modification: Promote a public accessor on the session (e.g. `session.transport` property, or a `session.build_downloader()` method) and use it here. Until then, any refactor of `DataSluiceSession` internals must grep for `_transport` references in `integrations/`.
- Test coverage: None for the download branch (`airflow.py:62-73` uncovered).

**`HttpxTransport` leaks its `httpx.Client` connection pool**
- Files: `src/datasluice/transport/httpx_transport.py:123-127`. The `httpx.Client` is created in `__init__` but `HttpxTransport` defines no `close()`, no `__enter__`/`__exit__`, and no `__del__`.
- Why fragile: GC of an `httpx.Client` does close its pool eventually, but in long-running processes (crawlers, Airflow workers) that construct many transports, connections accumulate until GC runs. Combined with the per-call transport construction in `detector.py`, this can exhaust file descriptors under load.
- Safe modification: Add `close()` + `__enter__`/`__exit__` to `HttpxTransport`, and have the session own/close the default transport it constructs.

**`configure_logging()` is a side effect of `DataSluiceSession.__init__`**
- Files: `src/datasluice/runtime/session.py:147` — `configure_logging(DEFAULT_LOG_LEVEL)` runs on every session construction.
- Why fragile: A library user who has already configured `logging` (custom handlers, structured formatters, log aggregation) gets a `StreamHandler` with a `RedactingFilter` added to the `"datasluice"` logger on the first `DataSluiceSession()` call, and the level clobbered to `INFO` on every subsequent call. This violates the "library code should not configure logging" convention.
- Safe modification: Remove the `configure_logging` call from `__init__`; move it into the CLI entry point (`src/datasluice/cli/app.py`) where datasluice owns the process. Document that library users should call `configure_logging()` themselves if they want the default handler.

**Silent no-cache fallback when `ContentCache` import fails**
- Files: `src/datasluice/runtime/session.py:150-164` — `_build_default_cache` catches `ImportError` and returns `None`, logging only at DEBUG.
- Why fragile: A caller passing `cache_dir=...` expects caching. If the import fails (rare — `sqlite3` is stdlib — but possible in stripped environments or during refactors), the session silently operates without a cache and re-downloads every resource. The only signal is a DEBUG log line.
- Safe modification: Log at WARNING when `cache_dir` is provided but the cache cannot be constructed. Consider raising `ConfigError` unless an explicit `allow_no_cache=True` is passed.

**`with_retry` carries a placeholder `RuntimeError`**
- Files: `src/datasluice/transport/retry.py:57, 73` — `last_exc` is initialised to `RuntimeError("No retries attempted")` and the final `raise last_exc` carries a `# type: ignore[misc]` to silence the type checker.
- Why fragile: If `policy.max_attempts == 0` (misconfiguration), the function raises the placeholder `RuntimeError` rather than a `ValueError` explaining the bad config. The `type: ignore` masks the genuine type-soundness gap (`Exception` vs `T`).
- Safe modification: Validate `max_attempts >= 1` in `RetryPolicy.__post_init__`. Replace the placeholder with an `assert last_exc is not ...` or restructure so the loop is guaranteed to assign.

**`_host_credential_provider_type()` importlib indirection**
- Files: `src/datasluice/transport/httpx_transport.py:49-67`. The 401/403 eviction capability check does `importlib.import_module("datasluice.credentials.host_provider")` on **every** 401/403 response, wrapped in try/except ImportError.
- Why fragile: This was a wave-ordering workaround so transport could land before plan 03-04. Now that `host_provider.py` exists, the lazy import is dead weight on the hot path and obscures the real dependency. If `host_provider.py` is ever moved/renamed, the `ImportError` is swallowed and the eviction path silently degrades to "no refresh."
- Safe modification: Replace with a normal `from datasluice.credentials.host_provider import HostCredentialProvider` import under `TYPE_CHECKING` + a module-level `_HostCredentialProviderType = ...` alias, dropping the `importlib` dance.

## Scaling Limits

**Single-process session; no connection pooling across sessions**
- Current capacity: One `httpx.Client` (or urllib opener) per transport instance; pooled within that instance.
- Limit: Crawling thousands of portals from one process requires either one shared transport (no per-portal auth scoping) or N transports (N connection pools). There is no built-in pool manager.
- Scaling path: Introduce a `TransportPool` keyed by `CredentialScope`, or document the shared-transport pattern.

**Token-bucket rate limiter is global per instance, not per host**
- Current capacity: One `RateLimiter(requests_per_second=N)` throttles all requests that share the limiter instance.
- Limit: Different portals have different quotas. A single shared limiter either under-utilises lenient portals or over-stresses strict ones.
- Scaling path: Per-host `RateLimiter` map keyed on `urlparse(url).hostname`, owned by the transport.

**Content cache has no size bound**
- Current capacity: `ContentCache` grows without limit; only `ttl`-based expiry applies, and only on read (`get`) / sweep.
- Limit: A crawler downloading many large resources fills the disk; there is no `max_bytes` eviction.
- Scaling path: Add an LRU-by-size sweep that evicts the oldest `ready` entries when total content size exceeds a configured cap.

## Dependencies at Risk

**`apache-airflow` optional extra is heavy and pins a wide constraint surface**
- Risk: `apache-airflow` (in `[project.optional-dependencies].airflow` and `.all`, `pyproject.toml:55, 65`) drags in a large, opinionated dependency tree with its own Python-version and Flask-version constraints. It frequently conflicts with the rest of the datasluice environment, making `uv sync --all-extras` fragile on newer Pythons.
- Impact: CI type-check job (`uv run --all-extras ty check .`) and local dev installs can break on Airflow's release cadence, independent of datasluice's own code. The `airflow` integration is only 23% covered, so the cost/benefit of bundling it is poor.
- Migration plan: Consider moving Airflow support to a separate companion package (`datasluice-airflow`) that depends on `datasluice` and `apache-airflow`, installed only by users who need it. Until then, pin a known-good Airflow range and exclude it from `--all-extras` in CI type-check.

**`httpx>=0.27` lower bound with no upper cap**
- Risk: `pyproject.toml:58` pins `httpx>=0.27` but no upper bound. A future httpx major release (httpx 1.0 has been discussed) could break `HttpxTransport` (e.g. `iter_raw`, `build_request`, `response.next_request` APIs).
- Impact: A transitive bump could break production downloads with no compile-time signal.
- Migration plan: Cap with `httpx>=0.27,<1.0` (or `<2.0` if conservative). Add a smoke test that imports `HttpxTransport` and exercises a single request against the in-process test HTTP server (`tests/helpers/http_server.py`).

**Python 3.14 in the CI matrix ahead of ecosystem support**
- Risk: `.github/workflows/ci.yml` runs the test matrix on Python 3.12, 3.13, and 3.14 (per AGENTS.md). At analysis date, several optional deps (notably `apache-airflow`, `dlt`, and some `pyarrow` releases) lag behind 3.14.
- Impact: The 3.14 leg can fail for reasons unrelated to datasluice's own code, masking real regressions.
- Migration plan: Mark the 3.14 leg `allow-failure: false` but `continue-on-error: true` until the optional-deps ecosystem declares 3.14 support; or split the matrix into "core" (3.12–3.14) and "extras" (3.12–3.13) jobs.

**Coverage threshold of 50% is well below actual coverage**
- Risk: `[tool.coverage.report].fail_under = 50` (`pyproject.toml:112`) gates CI on 50% line coverage. Actual coverage is 85% overall.
- Impact: A regression that drops coverage from 85% to 55% would still pass CI. The threshold provides no meaningful guard.
- Migration plan: Raise `fail_under` to 80% (or the current actual minus 2pp) and add per-package thresholds for the low-coverage areas listed below.

## Missing Critical Features

**No HEAD / conditional-GET support on the download path**
- Problem: `Downloader.download` (`src/datasluice/io/downloader.py:41-97`) always issues a full GET. `ContentCache.get_metadata` (`src/datasluice/io/content_cache.py:232-256`) already stores `etag`/`last_modified`, but nothing on the read path sends `If-None-Match` / `If-Modified-Since` to revalidate.
- Blocks: Bandwidth-efficient incremental sync (the documented Phase 7 / SYNC-06 goal).

**No retries on the streaming path**
- Problem: `HttpxTransport.stream` (`src/datasluice/transport/httpx_transport.py:271-298`) explicitly does NOT wrap in `with_retry` — a transient 5xx mid-stream fails the whole download. Resumable streaming is documented as Phase 4's concern but is not yet implemented.
- Blocks: Reliable download of large resources over flaky connections.

**No structured/batch download result**
- Problem: `Downloader.download_many` returns `list[Path]` only (see Known Bugs). There is no `DownloadReport` exposing successes, failures, bytes downloaded, and timing.
- Blocks: Observability and idempotent retry of just the failed resources.

## Test Coverage Gaps

Actual overall coverage is 85% (265 tests passing), but it is unevenly distributed. The threshold (`fail_under=50`) hides these gaps.

**Portal adapters — the core product surface — are barely tested**
- What's not tested: Live search, `get_dataset`, `list_resources`, `get_organization` for all three built-in adapters. Only the mappers have focused unit tests; the adapter orchestration is uncovered.
- Files & coverage:
  - `src/datasluice/connectors/ckan/adapter.py` — 33% (lines 24-26, 30-38, 48-49, 53, 57-61 uncovered)
  - `src/datasluice/connectors/socrata/adapter.py` — 32% (lines 24-25, 29-39, 49-53, 57, 64)
  - `src/datasluice/connectors/datagouv/adapter.py` — 28% (lines 24-25, 29-44, 54-55, 59, 63-67)
  - `src/datasluice/connectors/socrata/mapper.py` — 35%
  - `src/datasluice/connectors/datagouv/mapper.py` — 37%
- Risk: A regression in search/pagination/`has_next` computation — the most user-visible behaviour — ships without a failing test.
- Priority: **High.** Add contract tests using recorded fixtures (or the existing `tests/helpers/http_server.py`) per adapter.

**Connector `errors.py` modules are 0% covered**
- What's not tested: The custom exception classes/mappers in `src/datasluice/connectors/ckan/errors.py`, `.../socrata/errors.py`, `.../datagouv/errors.py` (all 0%).
- Risk: These modules may be entirely dead code (nothing imports them) — or, worse, are intended to be wired in and never were. Either way, the current state is undefined.
- Priority: **High** (clarify intent — wire in or delete).

**`io/downloader.py` — the download path — is 21% covered**
- What's not tested: `Downloader.download` body (lines 63-97), `download_many` (105-113). Hash verification, cache hit/miss, and storage-write branches are all uncovered.
- Files: `src/datasluice/io/downloader.py`
- Risk: The checksum-mismatch and storage-failure paths — security/correctness critical — are untested. A bug in `verify_hash` comparison (case sensitivity, algorithm selection) would ship silently.
- Priority: **High.**

**`discovery/detector.py` is 25% covered**
- What's not tested: The actual probe loop (lines 40-59) — i.e. detection never exercises a real (mocked) portal response in tests.
- Risk: The "auto-detect portal from URL" UX — a headline feature — has no regression test.
- Priority: **High.**

**Integration modules (pandas, polars, dlt, airflow) are 16–25% covered**
- What's not tested: The happy path of every integration except DuckDB (89%).
- Files: `src/datasluice/integrations/polars.py` (16%), `pandas.py` (22%), `airflow.py` (23%), `dlt.py` (25%).
- Risk: Public integration APIs that users call directly are untested. A pandas/polars API change (these libraries release often) breaks the integration with no signal.
- Priority: **Medium** (add smoke tests gated on optional-dep presence).

**`formats/xlsx.py` (27%) and `formats/geojson.py` (30%)**
- What's not tested: The read paths for Excel and GeoJSON.
- Risk: Format readers silently break on valid files; users hit confusing errors.
- Priority: **Medium.**

**`connectors/custom/` is 0% covered**
- What's not tested: The skeleton adapter entirely.
- Risk: Low — it raises `NotImplementedError` everywhere — but the 0% number means even instantiation isn't asserted.
- Priority: **Low.**

---

*Concerns audit: 2026-07-26*
