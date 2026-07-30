# Codebase Concerns

**Analysis Date:** 2026-07-30

Severity legend: **Critical** (security/data-loss/blocker), **High** (likely bug or active fragility),
**Medium** (works today but maintenance hazard), **Low** (polish / tech-debt marker).

This project is mid-refactor (v0.1.0 → v1.0.0, Phases 1–7 done; Phase 8 pending). Many issues below are
*intentional, documented compromises* tied to a numbered RESEARCH Pitfall or broken window. Each entry
states whether it is a genuine defect vs. an accepted deviation so a refactorer knows where to invest.
Canonical pitfall reference: `.planning/research/PITFALLS.md`. Open defect register: `.planning/WINDOWS.md`.

---

## Tech Debt

### dlt source ignores caller-injected dependencies  · **Critical**

- Issue: `datasluice_source()` constructs its own `DataSluiceSession()` (line 59) and a fresh
  `DataPlaneResourceReader(transport=HttpxTransport())` (line 62) *inside* the source closure. It does
  not accept or forward the caller's `auth`, `credential_provider`, `rate_limit`, `retry_policy`,
  `cache`, or `transport`. The entire Phase 2/3 dependency-injection design is bypassed for the dlt path.
- Files: `src/datasluice/integrations/dlt.py:59`, `src/datasluice/integrations/dlt.py:62`
- Impact: Authenticated/private portals cannot be loaded through dlt. Rate limits, retry policy, and the
  content cache are silently unused. Any credential scope/host stripping configured on a user's session
  does not apply to dlt-initiated downloads. This also forces dlt users to install `httpx` even when they
  intend to read from object storage.
- Fix approach: Accept an optional `session: DataSluiceSession | None = None` (and/or `reader=` /
  `transport=`) on `datasluice_source()`; default to `DataSluiceSession()` only when none is supplied.
  Reuse the injected reader for resource bodies instead of hard-coding `DataPlaneResourceReader`.

### dlt yields a fully materialized Table, not streaming RecordBatches  · **Medium**

- Issue: Each resource body calls `to_arrow(stream)` (full `pa.Table.from_batches(...)`), then yields the
  whole table into dlt. RESEARCH Pitfall 11 explicitly warns against dict/Table materialization and
  prescribes yielding `RecordBatch` / dlt `ArrowItem` so dlt and DataSluice both speak Arrow natively.
- Files: `src/datasluice/integrations/dlt.py:93`
- Impact: Large resources are buffered into one Arrow Table in memory before dlt sees them — defeats the
  bounded-memory promise of the `BatchStream` data plane (Phase 4) for the dlt integration specifically.
- Fix approach: Refactor `_resource_body` to iterate `stream.iter_batches()` and yield batches (or
  `dlt.mark`/`ArrowItem`) lazily. Confirm dlt's resource contract accepts a batch generator.

### Airflow operator reaches into a private attribute via `cast("Any", ...)`  · **High**

- Issue: `DataSluiceOperator` accesses `ds._transport` (a private, underscore-prefixed attribute) through
  `cast("Any", ds._transport)` to feed the legacy `Downloader`. This is a deliberate type-checker escape
  hatch that hides a layering violation from `ty`.
- Files: `src/datasluice/integrations/airflow.py:71`
- Impact: Couples the Airflow provider to `DataSluiceSession` internals; any rename of `_transport` breaks
  it silently at runtime (not at type-check time). The operator also instantiates its own `Downloader`
  rather than using the session's data plane / sync facilities, so it bypasses streaming, checksums, and
  the content-addressed cache.
- Fix approach: Expose a public accessor on `DataSluiceSession` (e.g. `session.transport` property) or
  route the operator through `session.sync_resources(...)`. Remove the `cast`.

### Legacy `Downloader` / `FileCache` diverge from the new content-addressed cache  · **Medium**

- Issue: `io/downloader.py` and `io/cache.py` predate the Phase 3 `ContentCache`. They are still wired into
  the Airflow operator. Divergences: (a) `Downloader` uses `cache_key = resource.url` verbatim
  (`io/downloader.py:68`) — the very anti-pattern RESEARCH Pitfall 9 flags (URL-derived keys). The new
  `ContentCache` hashes the key with SHA-256. (b) `FileCache.put` returns `Path` while `ContentCache.put`
  returns `None` — the `CachePort` return contract is inconsistent across the two implementations.
- Files: `src/datasluice/io/downloader.py:68`, `src/datasluice/io/cache.py:43`, `src/datasluice/io/content_cache.py:146`
- Impact: Two parallel cache implementations with different semantics. The Airflow path can collide /
  mis-key where the rest of the library cannot. `FileCache` has no size bound (Pitfall 9), no metadata
  sidecar, and no atomic two-phase write.
- Fix approach: Retire `FileCache`/`Downloader` once the Airflow operator is rebuilt on `sync_resources`.
  Delete the legacy modules or mark them deprecated. Unify the `put` return type.

### `_READY = True` feature-flag scaffolding left in production modules  · **Low**

- Issue: Several modules carry module-level booleans that were flags during incremental feature landing:
  `_CONDITIONAL_SYNC_READY`, `_WITHIN_RESOURCE_RESUME_READY`, `_FAILURE_BOUNDARY_READY`,
  `_SECRET_FREE_STATE_READY`, `_SESSION_SYNC_READY`, `_IDEMPOTENT_MATERIALIZE_READY`. All are now `True`
  and are never read.
- Files: `src/datasluice/sync/sync.py:18-20`, `src/datasluice/sync/materialize.py:12`,
  `src/datasluice/sync/state_store.py:29`, `src/datasluice/runtime/session.py:47`
- Impact: Dead code; mild reader confusion ("is this still gated?").
- Fix approach: Delete the flags. They served their purpose during Phase 7 landings.

---

## Known Bugs

### `with result.stream: pass` is not a valid context-manager call  · **High**

- Symptoms: In the conditional-fetch fallback branch of `sync_resources`, when the reader lacks
  `open_response`, the code does `with result.stream: pass` to "close" the stream.
- Files: `src/datasluice/sync/sync.py:84-86`
- Trigger: Inject any `reader` that does not implement `open_response` together with a
  `ConditionalTransport` whose `conditional_fetch` returns a non-304 result. `result.stream` is a
  `StreamResponse` (`src/datasluice/transport/httpx_transport.py:71`) that implements only `__iter__` and
  `close()` — it has no `__enter__`/`__exit__`, so `with result.stream:` raises `AttributeError: __enter__`.
- Workaround: None today. The path is currently dead because the default `DataPlaneResourceReader` *does*
  implement `open_response`, so the branch is untested and the bug is latent. Registered as a deviation
  surface (broken window #7 covers the broader conditional-body reuse).
- Fix approach: Replace `with result.stream: pass` with `result.stream.close()`. Add a test that injects a
  reader without `open_response` and asserts the stream is closed, not that an `AttributeError` is raised.

### pandas 3.0.3 ArrowStringArray Index bug skips integration tests  · **Medium** (broken windows #1, #2)

- Symptoms: `tests/unit/integrations/test_to_pandas.py` and `test_equivalence.py` skip in the full suite
  but pass in isolation.
- Files: `tests/unit/integrations/test_to_pandas.py`, `tests/unit/integrations/test_equivalence.py:66`
- Trigger: Running the complete pytest suite under pandas 3.0.3 (an upstream pandas bug in
  ArrowStringArray Index handling).
- Workaround: Run those files individually. The broken-windows ledger (`/.planning/WINDOWS.md` items 1, 2)
  records this as an accepted deviation; `/gsd-ship` tolerates it.

### `test_peak_rss_bounded` flaky under full-suite memory pressure  · **Medium** (broken windows #3, #5)

- Symptoms: Peak-RSS test fails at the 200MB threshold (observed 208MB) when run as part of the full
  suite, passes in isolation.
- Files: `tests/unit/data/test_peak_rss.py:122`
- Cause: Inherited allocator state / Arrow shutdown interactions when the test shares a process with the
  rest of the suite. The test deliberately uses `ru_maxrss` in a subprocess (RESEARCH Pitfall 5) because
  `tracemalloc` cannot see pyarrow's native mimalloc allocations — the isolation is incomplete.
- Fix approach: Run the peak-RSS measurement in a stricter subprocess harness or raise the threshold and
  document the headroom. Broken-window #5 tracks the isolation work.

### dlt tests resolve the active module at execution time via `importlib`  · **Medium** (broken windows #8, #9)

- Symptoms: `tests/unit/integrations/test_dlt.py` resolves `datasluice.integrations.dlt` through
  `importlib.import_module` at module load and resolves `dlt.current.resource_state()` inside patched
  callables via `importlib` again, rather than a normal import.
- Files: `tests/unit/integrations/test_dlt.py:19`, `:177`, `:285`, `:290`
- Cause: A "purity" module purge step re-imports the dlt module fresh for some tests; the late binding is
  needed so the patched `arrow.to_arrow` is observed. This is fragile to import-ordering changes.
- Fix approach: Eliminate the purity-purge step (broken window #9) or formalize it behind a fixture so
  the late binding is intentional rather than an `importlib` workaround.

---

## Security Considerations

### DuckDB SQL injection — mitigated but the boundary is a single regex  · **Low** (residual after fix)

- Risk: An attacker controlling the `table_name` argument could attempt SQL injection.
- Files: `src/datasluice/integrations/duckdb.py:21-38` (`_validate_table_name`), `:76` (call site)
- Current mitigation: `_validate_table_name` rejects anything not matching `^[A-Za-z_][A-Za-z0-9_]*$`
  before `connection.register(table_name, table)` / `connection.table(table_name)`. The relation API is
  used (no f-string SQL interpolation), so this was the RESEARCH Pitfall 5 fix. Regression tests exist
  (`tests/unit/integrations/test_to_duckdb.py` imports `_validate_table_name`).
- Recommendations: The guard is correct but lives as the *sole* SEC-03 boundary. Any future API that
  re-introduces string-interpolated SQL must re-apply the regex. Keep the guard centralized and add a
  lint/test asserting `to_duckdb` is the only DuckDB call site.

### Cross-host credential leakage on redirect — mitigated  · **Low** (residual after fix)

- Risk: Credentials forwarded to a third-party CDN / attacker host on 3xx.
- Files: `src/datasluice/transport/redirect.py:30-59`, `src/datasluice/transport/httpx_transport.py:140-192`
- Current mitigation: Both the urllib `CredentialAwareRedirectHandler` and the httpx manual redirect loop
  strip `SENSITIVE_HEADERS` (`authorization`, `cookie`, `x-api-key`, `x-auth-token`) on cross-origin or
  https→http downgrade hops. With a `CredentialScope`, stripping is host-allowlist-driven. This was the
  RESEARCH Pitfall 1 fix.
- Recommendations: `SENSITIVE_HEADERS` (`src/datasluice/logging.py:17`) is a closed set. If a new auth
  strategy introduces a new sensitive header name, it must be added here *and* to the redirect handlers,
  or it will be forwarded on cross-host redirects. Add a test that fails when a new `BaseAuth` subclass
  emits a header not in the set.

### Secret redaction is key-name-based, not value-pattern-based  · **Low** (intentional)

- Risk: Secrets in URL query strings (`?api_key=...`, presigned `X-Amz-Signature`) and in arbitrary
  non-allowlisted log keys are NOT redacted.
- Files: `src/datasluice/logging.py:45-71`
- Current mitigation: `RedactingFilter` replaces values for keys in `_SENSITIVE_KEYS`. RESEARCH Pitfall 6
  documents the deliberate choice: value-pattern heuristics would false-positive on legitimate base64 /
  open-data payloads. `DATASLUICE_NO_REDACT=1` disables redaction entirely.
- Recommendations: Logged URLs at DEBUG still carry embedded tokens (Pitfall 19). If DEBUG logging of
  request URLs is enabled in production, presigned signatures and query-string API keys reach logs.
  Consider a URL-sanitizing formatter step that strips known signature query params before emission.

### Plugin entry-point `.load()` executes arbitrary import-time code  · **Low** (accepted)

- Risk: A malicious or buggy third-party connector registered under `datasluice.connectors` runs at
  `PluginManager.__init__` time.
- Files: `src/datasluice/runtime/plugin_manager.py:45-59`
- Current mitigation: `.load()` is wrapped in `except Exception` (see "Broad exception catches" below);
  failures are recorded as `PluginFailure` and never crash session creation (RESEARCH Pitfall 4). Discovery
  is eager in `__init__`, not at package import — but `DataSluiceSession()` construction triggers it.
- Recommendations: Document the trust model for third-party connectors in the contributor guide. The eager
  discovery means merely constructing a session executes all installed connector entry points.

### Credentials never persisted to disk by DataSluice  · **Low** (verified)

- Risk: Tokens leaking to disk via cache/state files.
- Files: `src/datasluice/io/content_cache.py`, `src/datasluice/sync/state_store.py`
- Current mitigation: `FileStateStore.put` validates that watermark strings are SHA-256 / ETag / HTTP-date
  shaped (`_is_completed_watermark`, `state_store.py:258`) and rejects anything else; `last_synced_at` is
  constrained to tz-aware ISO-8601. The content cache stores only resource bytes + ETag/Last-Modified.
  No code path writes raw credentials (`BearerAuth`/`APIKeyAuth` reprs are redacted: `auth/bearer.py:22`,
  `auth/api_key.py:42`).

---

## Performance Bottlenecks

### Parquet over HTTP buffers the entire file into `BytesIO`  · **High** (documented compromise)

- Problem: Non-seekable Parquet sources (HTTP downloads via `IterableBytesIO`) are spooled with
  `io.BytesIO(source.read())` before `ParquetFile` can seek to the footer.
- Files: `src/datasluice/data/readers/parquet.py:55-57`
- Cause: Parquet footers live at EOF; `ParquetFile.iter_batches` issues `seek()`. The spool is bounded by
  total file size, NOT by `batch_size` — explicitly the "unavoidable compromise" of RESEARCH Pitfall 1.
- Improvement path: HTTP Range requests to fetch the footer + requested row groups directly. Documented as
  out of Phase 4 scope; large Parquet resources fetched over HTTP remain an OOM risk until landed.
  Mitigation today: route large Parquet through `object_storage` / `local_file` access kinds (seekable,
  no spool) and checkpoint via `read_batches_from_row_group`.

### ZIP decompression spools the full body and materializes the largest member  · **Medium**

- Problem: `_zip_largest_member` does `source.read()` into `io.BytesIO`, then `zf.read(largest)` returns
  the member bytes in full.
- Files: `src/datasluice/data/compression.py:245-274`
- Cause: ZIP central directory lives at EOF (RESEARCH Pitfall 2), so the body must be seekable; the
  largest-member selection (OQ5) then reads that member wholesale. Two full copies of the member can be
  resident (the `BytesIO` body + the extracted `member_bytes`).
- Improvement path: Stream the selected member via `zf.open(largest)` instead of `zf.read(largest)` so
  downstream format readers can consume it lazily. Keep the body spool (unavoidable) but avoid the second
  full copy.

### XLSX "streaming" buffers the whole workbook  · **Medium** (documented)

- Problem: `XLSXReader` calls `load_workbook(..., read_only=True)`, but XLSX is itself a ZIP so openpyxl
  decodes it in one pass; per-batch memory is not byte-bounded for wide rows.
- Files: `src/datasluice/data/readers/xlsx.py:7-13`, `:52-55`
- Cause: Format limitation (D-P4-14 acknowledges this). Open-data XLSX rows are modest width in practice.
- Improvement path: None planned; document the memory profile for users loading large XLSX.

### dlt terminal materializes the full table per resource  · **Medium**

(See "Tech Debt" — `to_arrow(stream)` in `dlt.py:93` buffers the entire resource.)

---

## Fragile Areas

### `pc.__dict__["assume_timezone"]` / `pc.__dict__["struct_field"]` bypass type checking  · **High**

- Files: `src/datasluice/transforms/steps.py:167`, `src/datasluice/transforms/steps.py:216`
- Why fragile: PyArrow compute functions `assume_timezone` and `struct_field` are accessed via dict lookup
  (`pc.__dict__[...]`) and assigned to an `Any`-typed local. This is a workaround for the `ty` type
  checker flagging these functions' signatures. Consequences: (a) no IDE autocomplete or go-to-definition;
  (b) a pyarrow rename/removal would raise `KeyError` at *runtime* inside the transform, not a clear
  ImportError; (c) the type checker can no longer verify argument correctness for these calls.
- Safe modification: If touching `NormalizeTimestamps` or `Flatten`, re-verify whether the `ty`
  conformance issue is still present on the current pyarrow version. If resolved, switch back to direct
  `pc.assume_timezone` / `pc.struct_field` attribute access. Keep a unit test that exercises both code
  paths so a pyarrow upgrade surfaces the change.
- Test coverage: Covered by `tests/unit/data/test_schema_unification.py` and transform tests, but only the
  happy path — a pyarrow rename would not be caught until runtime.

### `transport/__init__.py` PEP 562 lazy `__getattr__`  · **Medium**

- Files: `src/datasluice/transport/__init__.py:31-42`
- Why fragile: `HttpxTransport` and `StreamResponse` are resolved on first attribute access via a module
  `__getattr__` so bare installs (without the `http` extra) never import httpx. Unusual pattern; static
  analyzers and some IDEs do not see these as exported. Carries `# type: ignore[no-untyped-def]`.
- Safe modification: Anyone adding a new httpx-backed export must extend `__getattr__` and `__all__`
  together, or the symbol will import at runtime but be invisible to tooling.

### Detection-only optimistic CAS in `FileStateStore`  · **Medium** (documented)

- Files: `src/datasluice/sync/state_store.py:129-179`
- Why fragile: `put(..., expected_prior=...)` re-reads the file and SHA-256-compares *before* the atomic
  rename, but there is no true compare-and-swap — a writer can commit between the hash check and the
  `mv`. RESEARCH Pitfall 5 documents this as the portable fsspec workaround (no advisory locks / etags
  across backends).
- Safe modification: Concurrent writers to the same state key on a shared remote backend can still lose
  updates in the race window. For single-writer workflows (the current design) this is safe. Document the
  single-writer assumption if multi-writer sync is ever considered.

### `iter_batches_with_cursors` assumes monotonic batch indexes  · **Medium**

- Files: `src/datasluice/data/batch_stream.py:101-110`
- Why fragile: The cursor generator enforces `next_batch_index > previous` and pins the cursor position to
  a `ParquetRowGroupPosition` with `next_batch_index == row_group_index`. This coupling (batch index MUST
  equal row-group index) is correct only for the Parquet-row-group reader path
  (`read_batches_from_row_group`). A format reader that yields multiple batches per row group, or batches
  not aligned to row groups, would trip the `DataSluiceError("Batch cursor indexes must increase
  monotonically")` guard or produce meaningless cursors.
- Safe modification: Only the Parquet cursor stream uses this; do not wire non-Parquet formats through
  `iter_batches_with_cursors` without generalizing the position type.

### httpx connection/timeout errors propagate from detection  · **Medium** (open question OQ-1)

- Files: `src/datasluice/discovery/detector.py:23-37`
- Why fragile: `_PROBE_EXCEPTIONS = (NotFoundError, PortalError, OSError)`. `httpx.ConnectError` and
  `httpx.TimeoutException` are NOT `OSError` subclasses, so injecting an `HttpxTransport` into `detect()`
  means a connection failure propagates as an uncaught exception instead of becoming a detection miss.
- Safe modification: The CLI defaults to urllib `HttpClient` where this is moot. If you inject
  `HttpxTransport` into detection, either translate httpx exceptions in the transport or extend
  `_PROBE_EXCEPTIONS`. Documented as out of Phase 5 scope.

### `RecordBatchReader` use-after-close + GIL/native-resource edge cases  · **Medium**

- Files: `src/datasluice/data/batch_stream.py:81-99`
- Why fragile: `BatchStream.iter_batches()` catches `StopIteration` from `read_next_batch()` (RESEARCH
  Pitfall 2 — PyArrow readers hold native resources; GC may not finalize promptly). `__arrow_c_stream__`
  materializes the full batch list for bare iterators via `pa.RecordBatchReader.from_batches`. Resource
  cleanup depends on the caller using the context manager.
- Safe modification: Always use `with open(...) as stream:`. The `_StreamClosingBytesIO`
  (`data/access.py:46-60`) chains response + transport cleanup into `close()`; any new reader wrapper must
  preserve that discipline or leak file descriptors / httpx connections under load.

---

## Scaling Limits

### httpx connection pool defaults  · **Low**

- Current capacity: `HttpxTransport.__init__` constructs `httpx.Client(timeout=httpx.Timeout(timeout))`
  with default `Limits` (httpx defaults: 20 keepalive / 100 max connections). No `pool` timeout slot is
  set — `timeout` is a single scalar.
- Limit: RESEARCH Pitfall 13 warns this yields `PoolTimeout` under batch downloads. The single-scalar
  timeout conflates connect/read/write/pool wait.
- Scaling path: Expose `httpx.Limits` and `httpx.Timeout(connect, read, write, pool)` on `HttpxTransport`
  / `DataSluiceSession` when batch/concurrent download support lands.

### Content cache is local-filesystem-oriented  · **Low**

- Current capacity: `ContentCache` uses a SQLite WAL index (`cache.db`) with short-lived connections and a
  lazy sweep. Works on local FS; on remote fsspec backends the per-method `cat_file`/`pipe_file` round
  trips and SQLite-over-remote are untested.
- Limit: SQLite WAL on a non-local backend is not supported reliably.
- Scaling path: Restrict `ContentCache` to `file://` / local; use a different `CachePort` implementation
  for remote backends if needed.

### `HostCredentialProvider` is synchronous only  · **Low**

- Current capacity: Single-flight refresh via `threading.Lock` + double-checked expiry.
- Limit: No async refresh path. Fine for the current single-threaded sync design; an async transport would
  need an `asyncio`-native provider.
- Scaling path: Add an async-capable credential provider if/when async fsspec / httpx is adopted
  (RESEARCH Pitfall 8 reserves async as opt-in).

---

## Dependencies at Risk

### Heavy optional deps imported lazily inside function bodies  · **Low** (intentional, maintain carefully)

- Risk: `pandas`, `polars`, `dlt`, `duckdb`, `apache-airflow`, `pyarrow`, `openpyxl`, `httpx`, `fsspec`,
  `zstandard` are all optional extras. The codebase deliberately imports them inside function bodies
  (AGENTS.md "Lazy imports" rule) so `import datasluice` works on a bare install.
- Impact: If a future change moves any of these to a module-top-level import, `import datasluice` breaks
  for users without that extra. `ty` type-checking (CI) requires `uv sync --all-extras` precisely because
  these lazy imports are otherwise unresolved.
- Migration plan: Keep the lazy-import discipline. The `# noqa: F401 — lazy import gate` markers in
  `integrations/pandas.py:36` and `integrations/polars.py:35` are the established pattern — they import to
  trigger `ImportError` early, then discard the name and re-import at use.

### pandas 3.0.3 upstream bug  · **Medium** (broken windows #1, #2)

- Risk: The pinned/newer pandas ships an ArrowStringArray Index bug that causes integration tests to skip
  in the full suite.
- Impact: Equivalent between `to_pandas` / DuckDB / polars is not continuously verified in CI's full run.
- Migration plan: Track the upstream pandas fix; unskip the tests when fixed. Until then, equivalence is
  only validated by running the two test files in isolation.

### `pyarrow` is the load-bearing substrate  · **Medium**

- Risk: The entire data plane (`BatchStream`, all format readers, all transforms, all terminals, dlt)
  depends on pyarrow. RESEARCH Pitfall 7 warns the Python-level API evolves across major versions.
- Impact: A pyarrow major bump could break `RecordBatchReader` semantics, `pc.assume_timezone`/
  `pc.struct_field` availability (see Fragile Areas), or the `__arrow_c_stream__` PyCapsule protocol.
- Migration plan: The CI matrix runs Python 3.12/3.13/3.14 but pyarrow is not matrixed. Consider adding a
  pyarrow floor/ceiling test matrix before v1.0.0.

---

## Missing Critical Features

### Application-layer facade and use cases  · **Medium** (Phase 8 scope)

- Problem: `DataSluiceSession` exposes `portal()`, `search()`, `sync_resources()` but the full
  application-layer use cases and the complete CLI surface are Phase 8 work.
- Files: `src/datasluice/runtime/session.py`, `src/datasluice/cli/` (only `search`/`inspect`/`download`/
  `detect` commands today)
- Blocks: A user cannot drive the full sync/materialize flow from the CLI today; `sync_resources` is API-
  only.

### 80% coverage gate  · **Medium** (Phase 8 scope)

- Problem: `pyproject.toml` sets `fail_under = 50` — the floor, not the target.
- Files: `pyproject.toml:115`
- Blocks: The v1.0.0 release gate (ROADMAP Phase 8, success criterion 5: "CI enforces coverage ≥ 80%").
  Until Phase 8 plan 08-04 lands, coverage at 50% means new untested code does not fail CI.

### Query / stream access kinds  · **Low** (explicitly out of scope)

- Problem: `DataPlaneResourceReader.open()` raises `UnsupportedAccessError` for `query` and `stream`
  access kinds (CKAN datastore, Socrata SoQL). dlt's `datasluice_source` skips resources with these kinds.
- Files: `src/datasluice/data/access.py:124-132`, `src/datasluice/integrations/dlt.py:67-71`
- Blocks: Resources exposed only via query/stream APIs cannot be read. Documented as future work / out of
  scope for the current milestone.

---

## Test Coverage Gaps

### Conditional-fetch fallback branch is untested (latent bug)  · **High**

- What's not tested: The `elif result.stream is not None:` branch in `sync_resources` that does
  `with result.stream: pass` (see Known Bugs). Because the default reader has `open_response`, no test
  injects a reader without it.
- Files: `src/datasluice/sync/sync.py:84-86`
- Risk: The latent `AttributeError` is not caught. A reader implementation that legitimately lacks
  `open_response` would crash on a 200 conditional fetch.
- Priority: High — add a test injecting a minimal reader without `open_response` to expose the bug, then
  fix to `result.stream.close()`.

### Broad exception catches are intentional but under-asserted  · **Medium**

- What's not tested: The intentional broad catches in `runtime/plugin_manager.py:48` (`except Exception`
  around `.load()`), `io/content_cache.py:279,286,288` (`except Exception` in sweep),
  `data/readers/parquet.py:119` (`except Exception` in `_safe_seekable`), `data/readers/xlsx.py:54`
  (`except Exception` around `load_workbook`).
- Files: as above
- Risk: Each is documented as intentional isolation, but none have a test asserting they swallow only the
  intended failure classes. A future regression that broadens the catch further (or narrows it wrongly)
  would not be caught.
- Priority: Medium — add tests that (a) a broken plugin is recorded in `list_failures()`, (b) a sweep
  failure does not break `get`/`put`, (c) `_safe_seekable` returns False on a reader whose `seekable()`
  raises.

### Cross-host redirect stripping under a `CredentialScope`  · **Medium**

- What's not tested (verify): The host-allowlist path of `_should_strip_authorization` /
  `CredentialAwareRedirectHandler` when a redirect target IS in `allowed_hosts` (should NOT strip).
  RESEARCH Pitfall 1's "same-host-but-not-in-scope" case is the subtle one.
- Files: `src/datasluice/transport/httpx_transport.py:140-161`, `src/datasluice/transport/redirect.py:49-55`
- Risk: A regression that over-strips (breaking legitimate same-scope redirects) or under-strips (leaking
  to an allowed-but-unexpected host) would not be caught.
- Priority: Medium — confirm coverage of the four scope branches (downgrade / host-not-allowed /
  scheme-not-allowed / `send_on_redirect=False`) and the same-host positive case.

### Materialize checkpoint-resume corrupt-state paths  · **Medium**

- What's not tested (verify): `materialize_checkpointed` corrupt-continuation branches — missing shard,
  non-monotonic cursor — are exercised; confirm the `DataSluiceError` paths in
  `sync/materialize.py:98-103`, `:108-112`, `:118-119`, `:121-125` each have coverage.
- Files: `src/datasluice/sync/materialize.py`
- Risk: A crash mid-materialize that leaves a partial-shard gap could silently produce an incomplete
  artifact if the guards regress.
- Priority: Medium.

### Overall coverage at the 50% floor  · **Medium**

- What's not tested: Branch coverage (`branch = true` is set in `pyproject.toml:108`) is collected but the
  gate is `fail_under = 50`. RESEARCH Pitfall 16 explicitly warns that line coverage can hit a threshold
  while error paths and streaming edge cases stay untested.
- Files: `pyproject.toml:107-125`
- Risk: New code with no tests passes CI as long as aggregate coverage stays ≥ 50%.
- Priority: Medium — Phase 8 plan 08-04 raises the gate to 80%. Until then, rely on the targeted
  per-pitfall tests the phases added.

---

*Concerns audit: 2026-07-30*
