# Graph Report - datasluice  (2026-08-01)

## Corpus Check
- 125 files · ~37,413 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1332 nodes · 2877 edges · 75 communities (69 shown, 6 thin omitted)
- Extraction: 94% EXTRACTED · 6% INFERRED · 0% AMBIGUOUS · INFERRED: 170 edges (avg confidence: 0.6)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `ffca989c`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- sync/state_store.py
- ContentCache
- TransformContext
- sync.py
- HostCredentialProvider
- datagouv/mapper.py
- DataSluice (Public API class)
- APIKeyAuth
- FormatError
- datasluice_source
- discovery/detector.py
- ParquetReader
- _byte_source.py
- session.py
- Transport
- FileCache
- .read_batches
- PeekableReader
- Adapters Layer (datasluice.adapters)
- ConditionalTransport
- materialize.py
- DataGouvAdapter
- Query
- configure_logging
- CI Workflow
- load_fixture
- CSVReader
- batch_stream.py
- HttpClient
- ckan/mapper.py
- PortalError
- FsspecStorage
- BasicAuth
- compression.py
- BatchStream
- BearerAuth
- HeadersAuth
- StoragePort
- PortalDetector
- io/__init__.py
- _effective_origin
- CachePort
- exceptions.py
- StateStore
- .read_batches
- downloader.py
- httpx_transport.py
- Datasluice Brand Logo (no background)
- xlsx.py
- DownloadError
- domain/__init__.py
- Pre-commit Configuration
- data/access.py
- SECURITY.md
- OpenCodeReview PR Review Workflow (alibaba open-code-review)
- Formats Layer (datasluice.formats)
- Defense-in-depth CI security scanning pattern
- Datasluice Brand Identity
- Pull Request Template (affected areas + AI provenance)
- SocrataAdapter
- connectors/__init__.py
- integrations/__init__.py
- Funding Config (Buy Me a Coffee: nitishraj)
- Issue Template Config (blank issues enabled)
- CKANPage
- BaseFormatReader
- .apply
- DataSluiceSession
- socrata/mapper.py
- DataGouvPage
- IterableBytesIO
- data/schema.py
- transport/pagination.py
- Downloader
- ports/__init__.py

## God Nodes (most connected - your core abstractions)
1. `Resource` - 41 edges
2. `DataSluiceError` - 41 edges
3. `BatchStream` - 40 edges
4. `BaseAuth` - 33 edges
5. `SyncState` - 29 edges
6. `Query` - 28 edges
7. `BaseAdapter` - 27 edges
8. `DataPlaneResourceReader` - 26 edges
9. `Dataset` - 25 edges
10. `FormatError` - 25 edges

## Surprising Connections (you probably didn't know these)
- `DataSluice Logo (Brand Mark)` --semantically_similar_to--> `Datasluice Brand Logo (no background)`  [INFERRED] [semantically similar]
  docs/assets/datasluice-logo.png → docs/assets/datasluice-logo-nbg.png
- `APIKeyAuth` --uses--> `BaseAuth`  [INFERRED]
  src/datasluice/auth/api_key.py → src/datasluice/auth/base.py
- `BasicAuth` --uses--> `BaseAuth`  [INFERRED]
  src/datasluice/auth/basic.py → src/datasluice/auth/base.py
- `BearerAuth` --uses--> `BaseAuth`  [INFERRED]
  src/datasluice/auth/bearer.py → src/datasluice/auth/base.py
- `HeadersAuth` --uses--> `BaseAuth`  [INFERRED]
  src/datasluice/auth/headers.py → src/datasluice/auth/base.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Conventional Commits Release Flow** — github_workflows_release_please, github_workflows_publish_build_job, github_workflows_publish_testpypi_job, github_workflows_publish_pypi_job [INFERRED 0.95]
- **CI Quality Gate Pipeline** — github_workflows_ci_lint_job, github_workflows_ci_typecheck_job, github_workflows_ci_test_job, github_workflows_ci_coverage_job, github_workflows_ci_build_job, github_workflows_ci_smoke_test_job, github_workflows_ci_all_checks_pass_job [INFERRED 0.95]
- **AI PR Automation Stack (shared LLM secrets + renovate exclusion)** — github_workflows_ocr_review, github_workflows_pr_agent, concept_ai_pr_automation, concept_renovate_bot_exclusion [INFERRED 0.85]
- **QA pipeline (just qa / make qa)** — agents_md, ruff_linter, ty_typechecker, pytest_framework [EXTRACTED 1.00]
- **Automated release flow** — conventional_commits, release_please, changelog_md, contributing_md [EXTRACTED 1.00]
- **Adapter layer core components** — baseadapter_protocol, registry_concept, factory_concept, adapters_layer [EXTRACTED 1.00]
- **DataSluice unified open-data portal access pattern** — datasluice_class, portal_ckan, portal_datagouv, portal_socrata [INFERRED 0.95]
- **Pre-commit quality gate pipeline (format, lint, typecheck, test)** — precommit_config, lib_ruff, lib_ty, lib_pytest [EXTRACTED 1.00]

## Communities (75 total, 6 thin omitted)

### Community 0 - "sync/state_store.py"
Cohesion: 0.07
Nodes (49): RLock, SyncState model for incremental sync cursors and watermarks (SYNC-03)., Incremental synchronization state for a resource or connector. Attributes:…, SyncState, Raised when a state store cannot read or write durable sync state (D-P7-26). A…, Raised when a state write loses an optimistic compare-and-swap race (D-P7-27).…, StateStoreError, SyncStateConflictError (+41 more)

### Community 1 - "ContentCache"
Cohesion: 0.13
Nodes (15): Connection, ContentCache, Any, Return the SHA-256 hexdigest of *key* (SEC-06 carry-forward, D-P3-09)., Return the absolute content-file path for a SHA-256 digest., Return cached bytes for *key*, or ``None`` on miss / expiry / writing., Store *data* under *key* (CachePort signature UNCHANGED, D-P3-12)., Store *data* under *key* with ETag/Last-Modified sidecar (D-P3-12). Phase 4's… (+7 more)

### Community 2 - "TransformContext"
Cohesion: 0.06
Nodes (41): __getattr__(), Composable transform pipeline package (TRANS-01..09). Re-exports are resolved…, Lazily export transform symbols (mirrors datasluice.data.__getattr__).…, _build_batch_stream(), _chain(), compose(), Pipeline, Any (+33 more)

### Community 3 - "sync.py"
Cohesion: 0.11
Nodes (39): Exception, ParquetRowGroupPosition, Logical position at the next unread Parquet row group., DataSluiceError, Base exception for all DataSluice errors., canonical_destination_identity(), Canonical resource identity (CR-01 blocker fix, SYNC-05/07). Portal-controlled…, Return a secret-free SHA-256 identity for a destination URI. (+31 more)

### Community 4 - "HostCredentialProvider"
Cohesion: 0.15
Nodes (11): Lock, Refresher, HostCredentialProvider, Drop the cached credential for *host* (off-port; D-P3-15). Called by…, Resolve a :class:`BaseAuth` per host with cached expiry and single-flight…, Return the per-host lock, creating it if necessary. The dict-level lock is held…, Return whether *expires_at* has passed. ``None`` means the credential never…, Return the cached :class:`BaseAuth` for *host*, refreshing if expired. Args:… (+3 more)

### Community 5 - "datagouv/mapper.py"
Cohesion: 0.12
Nodes (25): map_dataset(), map_license(), map_organization(), map_resource(), Any, Mapping functions to convert data.gouv.fr (udata) JSON into domain models., Convert a udata license dict into a :class:`License`., Resolve the resource access descriptor per D-P5-02 for udata resources. udata… (+17 more)

### Community 6 - "DataSluice (Public API class)"
Cohesion: 0.10
Nodes (28): Conventional Commits, DataSluice (Public API class), datasluice_source (dlt source factory), DataSluiceOperator (Airflow operator), Discovery Layer (datasluice.discovery), Airflow Integration Example, CKAN Example, data.gouv.fr Example (+20 more)

### Community 7 - "APIKeyAuth"
Cohesion: 0.33
Nodes (3): APIKeyAuth, Any, Authenticate requests using an API key. Args: api_key: The API key value.…

### Community 8 - "FormatError"
Cohesion: 0.36
Nodes (7): _first_non_whitespace_byte(), JSONReader, Any, Stream a JSON / JSONL ``BinaryIO`` source into Arrow ``RecordBatch`` objects., Yield ``RecordBatch`` objects from a JSON array or JSONL source. Args: source:…, FormatError, Raised when a resource cannot be parsed in the expected format.

### Community 9 - "datasluice_source"
Cohesion: 0.29
Nodes (9): datasluice_source(), Any, Return a dlt source yielding one Arrow-backed table per resource. Args: portal:…, _encode(), logical_sha256(), Any, Serialization-stable logical hashing for Arrow tables., Return a SHA-256 digest over an Arrow table's schema and logical rows. (+1 more)

### Community 10 - "discovery/detector.py"
Cohesion: 0.12
Nodes (18): detect(), _normalize_base_url(), Evidence-based portal type detection (D-P5-15/16/17/18). The detector probes…, Ensure *url* has a scheme and no trailing slash., Probe *url* for every registered portal fingerprint (D-P5-15/16/17/18).…, Portal type fingerprints for auto-detection. Each entry maps a fingerprint (URL…, Portal type discovery and auto-detection., PortalMetadata (+10 more)

### Community 11 - "ParquetReader"
Cohesion: 0.23
Nodes (9): ParquetReader, Any, Streaming Parquet reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-10).…, Read one complete Parquet row group as one RecordBatch., Return ``source.seekable()`` if available; ``False`` on any error., Stream a Parquet ``BinaryIO`` source into Arrow ``RecordBatch`` objects. On a…, Yield ``RecordBatch`` objects by streaming Parquet row groups. Args: source: A…, Yield ``(row_group_index, batch)`` tuples for each non-empty row group. Each… (+1 more)

### Community 12 - "_byte_source.py"
Cohesion: 0.29
Nodes (5): HTTPMessage, IO, Request, IterableBytesIO — adapt an ``Iterable[bytes]`` into a non-seekable BinaryIO…, Return the follow-up request, stripping sensitive headers when required.

### Community 13 - "session.py"
Cohesion: 0.07
Nodes (33): API-key authentication strategy. Supports passing the key via a header (default…, BaseAuth, ABC, Abstract base for authentication strategies., Protocol for pluggable authentication. Each strategy knows how to decorate a…, HTTP Basic authentication strategy., Bearer-token (OAuth 2.0 / JWT) authentication strategy., Custom-headers authentication strategy. Useful for portals that expect a non-… (+25 more)

### Community 14 - "Transport"
Cohesion: 0.28
Nodes (4): Lazily initialised HTTP transport., Any, Transport boundary Protocol satisfied structurally by HTTP clients., Transport

### Community 15 - "FileCache"
Cohesion: 0.22
Nodes (7): FileCache, Path, A time-based file cache. Args: cache_dir: Directory to store cached files. ttl:…, Return cached bytes for *key*, or ``None`` if missing/expired., Store *data* under *key* and return the cache file path., Return ``True`` if *key* is cached and not expired., Remove all entries from the cache.

### Community 16 - ".read_batches"
Cohesion: 0.31
Nodes (8): _batch_from_rows(), _fmt_coord(), Any, Format a coordinate value: integers without trailing ``.0``; floats as-is., Yield ``RecordBatch`` objects by flattening GeoJSON Features. Args: source: A…, Build a single ``RecordBatch`` from a chunk of feature rows., Encode a GeoJSON ``geometry`` object as WKT (or fall back to raw JSON). Handles…, _to_wkt()

### Community 17 - "PeekableReader"
Cohesion: 0.15
Nodes (4): PeekableReader, WriteableBuffer, Read up to *n* bytes into the buffer WITHOUT consuming them. The peeked bytes…, Wrap a ``BinaryIO`` byte source with one-chunk lookahead. Buffers peeked bytes…

### Community 18 - "Adapters Layer (datasluice.adapters)"
Cohesion: 0.11
Nodes (17): Adapter isolation principle, Adapter 4-module pattern (adapter/mapper/pagination/errors), Adapters Layer (datasluice.adapters), Auth Layer (datasluice.auth), BaseAdapter protocol, CKAN adapter, CKAN portal platform, Composable transport (decorators) principle (+9 more)

### Community 19 - "ConditionalTransport"
Cohesion: 0.25
Nodes (6): AbstractContextManager, ConditionalTransport, Protocol, Streaming transport boundary Protocol (D-P3-06/D-P3-07). `stream(url)` returns…, Optional transport capability for ETag/Last-Modified conditional GETs…, StreamingTransport

### Community 20 - "materialize.py"
Cohesion: 0.12
Nodes (31): AbstractFileSystem, open_filesystem(), Any, Instantiate the correct fsspec backend for *uri* (INFRA-05, D-P3-20). Delegates…, canonical_identity(), Extract the origin scope for canonical identity hashing. HTTP(S) resources…, Return the SHA-256 canonical identity for *resource*. The identity is…, _url_origin() (+23 more)

### Community 21 - "DataGouvAdapter"
Cohesion: 0.14
Nodes (11): DataGouvAdapter, Fetch organization metadata via ``/organizations/{slug}/``., Adapter for data.gouv.fr and other udata-powered portals. Uses the udata REST…, Call a udata API endpoint and return the parsed JSON., Search datasets via ``/datasets/``. Translates every set supported ``Query``…, Fetch a dataset via ``/datasets/{id}/``., Return resources for *dataset_id*., create_datagouv_connector() (+3 more)

### Community 22 - "Query"
Cohesion: 0.07
Nodes (38): BaseAdapter, ABC, Protocol that every portal adapter must implement. Subclasses translate portal-…, Search for datasets matching *query*., Fetch a single dataset by its portal-native *dataset_id*., Return all downloadable resources for *dataset_id*., _check_dataset_ids_stable(), _check_get_dataset_returns_dataset_with_resources() (+30 more)

### Community 23 - "configure_logging"
Cohesion: 0.29
Nodes (6): LogRecord, configure_logging(), Any, Redact known sensitive keys from log records (INFRA-06, D-P3-18). Walks…, Configure the package-level logger. Args: level: Logging level (e.g.…, RedactingFilter

### Community 24 - "CI Workflow"
Cohesion: 0.16
Nodes (19): Conventional Commits Release Pipeline (release-please -> publish), uv package manager (used across all workflows), Dependabot Config (github-actions + pip weekly), CI Workflow, CI All checks pass gate (alls-green), CI Build & validate package job (uv build + twine), CI Coverage job (combine + report), CI Lint & format job (ruff) (+11 more)

### Community 25 - "load_fixture"
Cohesion: 0.36
Nodes (7): load_fixture(), load_fixture_set(), Any, Path, Fixture loading helpers for the conformance suite. Hand-authored portal-…, Load a single hand-authored portal-response fixture from *path*. Args: path:…, Load a keyed fixture set: ``{fixture_name: parsed fixture JSON}``.

### Community 26 - "CSVReader"
Cohesion: 0.29
Nodes (6): CSVReader, Any, Stream a CSV ``BinaryIO`` source into Arrow ``RecordBatch`` objects. Args:…, Yield ``RecordBatch`` objects by delegating to ``pyarrow.csv.open_csv``. Args:…, Drain a ``pa.RecordBatchReader`` and re-chunk into ``batch_size``-row batches., _rechunk_reader()

### Community 27 - "batch_stream.py"
Cohesion: 0.10
Nodes (20): BatchStream — context-managed Arrow RecordBatch stream (DATA-01, DATA-02,…, Streaming data-plane package: BatchStream, byte-source adapter, schema mapper.…, Any, to_arrow terminal: materialize a BatchStream into a pa.Table (INTG-01,…, Materialize *stream* into a ``pa.Table`` (INTG-01, D-P6-02). The shared…, to_arrow(), Any, DuckDB integration (read paths removed per D-P4-18; SEC-03 utility preserved).… (+12 more)

### Community 28 - "HttpClient"
Cohesion: 0.23
Nodes (9): HttpClient, Any, Perform an HTTP request and return the raw response body. Raises: PortalError:…, GET *url* and return the response as text., GET *url* and return the response as parsed JSON., GET *url* and return the raw bytes (for file downloads)., Render *body* as text, truncating to *limit* characters., Thin HTTP client wrapping :mod:`urllib` with auth, retry, and rate-limiting.… (+1 more)

### Community 29 - "ckan/mapper.py"
Cohesion: 0.09
Nodes (25): CKANAdapter, Fetch organization metadata via ``organization_show``., Adapter for CKAN-powered open-data portals. Uses the CKAN Action API at…, Call a CKAN Action API endpoint and return the ``result`` dict., Search datasets via ``package_search``. Translates every set supported…, Fetch a dataset via ``package_show``., Return resources for *dataset_id*., create_ckan_connector() (+17 more)

### Community 30 - "PortalError"
Cohesion: 0.07
Nodes (43): Response, map_ckan_error(), CKAN-specific error mapping., Map a CKAN HTTP error response to a DataSluice exception. Args: status_code:…, map_datagouv_error(), data.gouv.fr (udata) specific error mapping., Map a udata HTTP error response to a DataSluice exception., map_socrata_error() (+35 more)

### Community 31 - "FsspecStorage"
Cohesion: 0.16
Nodes (9): FsspecStorage, Any, fsspec-backed storage adapter satisfying :class:`StoragePort` (INFRA-02,…, Adapter wrapping an fsspec ``AbstractFileSystem`` to satisfy ``StoragePort``…, Persist *data* under *path* and return the resulting URI string. Args: data:…, Read and return the bytes stored under *path*., Return ``True`` if *path* exists in this filesystem., Return the backend-native path for *path*. Absolute URIs pass through… (+1 more)

### Community 32 - "BasicAuth"
Cohesion: 0.33
Nodes (3): BasicAuth, Any, Authenticate requests using HTTP Basic credentials. Args: username: Basic-auth…

### Community 33 - "compression.py"
Cohesion: 0.17
Nodes (16): BaseException, apply_compression(), _detect_format(), _ErrorTranslatingReader, Any, Transparent decompression decorator pipeline (DATA-06, D-P4-12). Sits BETWEEN…, Return the compression format key from magic bytes or content-encoding hint.…, Spool ZIP body to BytesIO, extract the largest member (RESEARCH Pitfall 2 +… (+8 more)

### Community 34 - "BatchStream"
Cohesion: 0.11
Nodes (20): BatchCursor, BatchStream, Any, Yield batches with the closed cursor for the next unread row group. In…, Release the underlying reader and any owned closeables; idempotent (WR-02).…, Return a PyCapsule for zero-copy Arrow interop (Phase 6 terminals). Delegates…, Closed continuation cursor for the next unread batch. ``next_batch_index``…, Context-managed Arrow RecordBatch stream. Wraps a ``pa.RecordBatchReader``… (+12 more)

### Community 35 - "BearerAuth"
Cohesion: 0.33
Nodes (3): BearerAuth, Any, Authenticate requests using a bearer token in the ``Authorization`` header.…

### Community 36 - "HeadersAuth"
Cohesion: 0.33
Nodes (3): HeadersAuth, Any, Authenticate requests using arbitrary static headers. Args: headers: A mapping…

### Community 37 - "StoragePort"
Cohesion: 0.25
Nodes (4): Protocol, Storage port Protocol returning URI references (CORR-05)., Boundary protocol for byte storage addressed by path/URI strings., StoragePort

### Community 38 - "PortalDetector"
Cohesion: 0.50
Nodes (3): PortalDetector, Protocol, Detection seam protocol returning evidence-based portal identification.

### Community 39 - "io/__init__.py"
Cohesion: 0.19
Nodes (14): compute_hash(), compute_md5(), compute_sha256(), Path, Checksum (hash) computation and verification., Compute the hex digest of *file_path* using *algorithm*., Return the SHA-256 hex digest of *file_path*., Return the MD5 hex digest of *file_path*. (+6 more)

### Community 40 - "_effective_origin"
Cohesion: 0.50
Nodes (4): _default_port(), _effective_origin(), Return the IANA default port for *scheme*, or ``None`` when unknown (CR-06)., Return ``(scheme, hostname, effective_port)`` for a parsed URL (CR-06).…

### Community 41 - "CachePort"
Cohesion: 0.20
Nodes (5): CachePort, Protocol, Cache port Protocol for content-addressed byte caching., Boundary protocol for a simple key/byte cache., Lazily construct the default ContentCache (plan 03-03) if importable. Resolved…

### Community 42 - "exceptions.py"
Cohesion: 0.18
Nodes (10): AdapterError, AdapterNotFoundError, ConfigError, Exception hierarchy for DataSluice., Raised when configuration is invalid or incomplete., Raised when an adapter cannot fulfil a request., Raised when no adapter is registered for a portal type., Raised when a transform step cannot be applied (D-P6-15). Transform failures… (+2 more)

### Community 43 - "StateStore"
Cohesion: 0.13
Nodes (9): AtomicStateStore, Protocol, State store port Protocols for incremental sync state (SYNC-01). The base…, Boundary protocol for persisting incremental sync state., Additive capability Protocol for compare-and-swap (CAS) state writes…, Return the raw envelope bytes for *key*, or ``None`` if absent. The returned…, Atomically load ``(state, version)`` from one backend read (CR-01). Returns…, Persist *state* under *key* only if the current version matches… (+1 more)

### Community 45 - "downloader.py"
Cohesion: 0.22
Nodes (10): Resource downloader with caching and checksum verification., ensure_dir(), Path, Local filesystem helpers for saving downloaded resources., Create *path* (and parents) if it does not exist; return as :class:`Path`., Write *data* to *dest* / *filename* and return the file path. Raises:…, Sanitise *name* for use as a filename., safe_filename() (+2 more)

### Community 47 - "httpx_transport.py"
Cohesion: 0.06
Nodes (40): datetime, Logger, Default configuration constants., Configuration for DataSluice., Host-scoped credential resolver with single-flight refresh (INFRA-04).…, CredentialScope, Credential scoping model for host-bound credential policy., Host-scoped policy controlling where credentials may be sent. Attributes:… (+32 more)

### Community 49 - "Datasluice Brand Logo (no background)"
Cohesion: 0.29
Nodes (10): DataSluice Logo (Brand Mark), Datasluice Brand Logo (no background), Circuit Pathways Connectivity Element, Code Brackets (</>) Programming Icon, Funnel Processing Element, Filtered Output Basin, Padlock Security Icon, Table/Grid Tech Icon (+2 more)

### Community 50 - "xlsx.py"
Cohesion: 0.28
Nodes (7): _batch_from_rows(), Any, Streaming XLSX reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-10).…, Stream an XLSX ``BinaryIO`` source into Arrow ``RecordBatch`` objects., Yield ``RecordBatch`` objects by chunking openpyxl ``iter_rows``. Args: source:…, Build a single ``RecordBatch`` from a chunk of row dicts., XLSXReader

### Community 54 - "DownloadError"
Cohesion: 0.14
Nodes (11): DownloadError, Raised when a resource download fails., LocalStorage, ABC, Storage abstraction for reading and writing resource files. Currently supports…, Abstract storage backend., Persist *data* under *key* and return the storage URI/path., Read and return the bytes stored under *key*. (+3 more)

### Community 59 - "domain/__init__.py"
Cohesion: 0.09
Nodes (34): Abstract base class for all portal adapters., CKAN adapter implementation. Communicates with the CKAN Action API…, data.gouv.fr (udata) adapter implementation. Communicates with the udata REST…, _is_set(), Pre-flight reject policy for unsupported ``Query`` filter fields (D-P5-06). The…, Raise ``UnsupportedQueryFieldError`` if *query* uses a field not in…, Return ``True`` when *value* counts as a set filter field. ``None``, empty…, _reject_unsupported_fields() (+26 more)

### Community 60 - "Pre-commit Configuration"
Cohesion: 0.50
Nodes (5): pre-commit-hooks (basic hooks), pytest testing framework, Ruff linter and formatter, ty type checker (Astral), Pre-commit Configuration

### Community 61 - "data/access.py"
Cohesion: 0.13
Nodes (23): _chain(), _close_source(), _content_encoding_from_headers(), DataPlaneResourceReader, Any, Concrete ``ResourceReader`` implementation with access-kind dispatch (DATA-04,…, Open *resource* as a :class:`BatchStream` of Arrow ``RecordBatch``. Dispatches…, Open an already-fetched streaming response through the data plane. (+15 more)

### Community 65 - "SECURITY.md"
Cohesion: 0.67
Nodes (3): CodeQL (security scanning), Dependabot (dependency updates), Zizmor (workflow audit)

### Community 66 - "OpenCodeReview PR Review Workflow (alibaba open-code-review)"
Cohesion: 0.83
Nodes (4): AI-assisted PR automation pattern, Renovate bot exclusion pattern (skip AI review for bots), OpenCodeReview PR Review Workflow (alibaba open-code-review), PR Agent Workflow (/describe auto-generate title & description)

### Community 67 - "Formats Layer (datasluice.formats)"
Cohesion: 0.50
Nodes (4): Formats Layer (datasluice.formats), Integrations Layer (datasluice.integrations), IO Layer (datasluice.io), Lazy imports of optional deps principle

### Community 73 - "Defense-in-depth CI security scanning pattern"
Cohesion: 1.00
Nodes (3): Defense-in-depth CI security scanning pattern, CodeQL Workflow (actions + python security analysis), Zizmor Workflow Security Analysis

### Community 74 - "Datasluice Brand Identity"
Cohesion: 1.00
Nodes (3): Datasluice Brand Identity, Datasluice Logo / Brand Mark, Datasluice Documentation Logo Asset

### Community 75 - "Pull Request Template (affected areas + AI provenance)"
Cohesion: 0.67
Nodes (3): Bug Report Issue Template, Feature Request Issue Template, Pull Request Template (affected areas + AI provenance)

### Community 76 - "SocrataAdapter"
Cohesion: 0.09
Nodes (17): Fetch a dataset (view) by its 4x4 identifier., Return resources for *dataset_id*., Translate a ``Query.sort`` spec into a Socrata catalog ``order`` token. The…, Adapter for Socrata-powered open-data portals. Uses the Socrata Discovery API…, Call the Socrata Discovery API and return parsed JSON., Search datasets via the Discovery API., SocrataAdapter, _translate_sort() (+9 more)

### Community 88 - "CKANPage"
Cohesion: 0.25
Nodes (5): CKANPage, Pagination helpers for the CKAN Action API., Parameters for a single CKAN search results page. CKAN uses ``start`` (offset)…, Return the parameters for the following page., Convert to query-string parameters for the CKAN API.

### Community 96 - "BaseFormatReader"
Cohesion: 0.21
Nodes (12): BaseFormatReader, ABC, Abstract base class for streaming format readers (D-P4-10). Each reader…, Protocol for decoding a single file format into Arrow ``RecordBatch`` objects.…, Streaming CSV reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-10).…, GeoJSONReader, Streaming GeoJSON reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-11).…, Stream a GeoJSON ``BinaryIO`` source into Arrow ``RecordBatch`` objects. Each… (+4 more)

### Community 109 - "DataSluiceSession"
Cohesion: 0.06
Nodes (35): BaseOperator, callback, main(), Main Typer application for the DataSluice CLI., DataSluice — unified open-data toolkit., detect(), Argument, help (+27 more)

### Community 110 - "socrata/mapper.py"
Cohesion: 0.14
Nodes (18): map_dataset(), map_organization(), map_resource(), Any, Mapping functions to convert Socrata-native JSON into domain models., Resolve the resource access descriptor per D-P5-02 for Socrata views. Socrata…, Best-effort schema extraction per D-P5-03 for Socrata views. Socrata's catalog…, Convert a Socrata view/resource dict into a :class:`Resource`. Populates… (+10 more)

### Community 112 - "DataGouvPage"
Cohesion: 0.25
Nodes (5): DataGouvPage, Pagination helpers for the data.gouv.fr (udata) API. udata uses page-number /…, Parameters for a single udata results page (1-based)., Return the parameters for the following page., Convert to query-string parameters for the udata API.

### Community 115 - "IterableBytesIO"
Cohesion: 0.13
Nodes (7): RawIOBase, IterableBytesIO, WriteableBuffer, Mark the stream closed and exhaust the iterator., Wrap an ``Iterable[bytes]`` into a non-seekable ``BinaryIO``. The canonical use…, Read up to ``len(b)`` bytes into the provided buffer. Args: b: A writable…, Read up to ``n`` bytes; ``n=-1`` reads all remaining bytes. Args: n: Maximum…

### Community 116 - "data/schema.py"
Cohesion: 0.22
Nodes (10): __getattr__(), Lazily export BatchStream, IterableBytesIO, to_arrow_schema,…, Any, Domain Schema → Arrow Schema mapper and batch unification helper. The…, Derive a ``pa.Schema`` from a domain :class:`Schema` for display. Maps known…, Concatenate ``RecordBatch`` objects under a unified ``pa.Schema`` (DATA-08,…, to_arrow_schema(), unify_batches() (+2 more)

### Community 117 - "transport/pagination.py"
Cohesion: 0.29
Nodes (6): paginate(), PaginationConfig, T, Generic pagination helpers shared across adapters., Common pagination parameters. Attributes: page_size: Number of items per page.…, Lazily yield pages of results. Args: fetch_page: Callable taking…

### Community 121 - "Downloader"
Cohesion: 0.18
Nodes (7): ChecksumMismatchError, Raised when a downloaded file's checksum does not match., Downloader, Path, Download multiple *resources* into *dest*., Downloads resources to local or pluggable storage. Args: transport: HTTP client…, Download a single *resource* and return the local file path. Args: resource:…

### Community 122 - "ports/__init__.py"
Cohesion: 0.27
Nodes (9): CatalogPort, OrganizationCatalog, Protocol, Catalog port Protocols for portal catalog connectors., Marker base protocol all catalog connectors share. Attributes: portal_type:…, Capability protocol for dataset search., Capability protocol for organization lookup., SearchableCatalog (+1 more)

## Knowledge Gaps
- **21 isolated node(s):** `Funding Config (Buy Me a Coffee: nitishraj)`, `Bug Report Issue Template`, `Issue Template Config (blank issues enabled)`, `Feature Request Issue Template`, `CI Lint & format job (ruff)` (+16 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **6 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Resource` connect `domain/__init__.py` to `BatchStream`, `sync.py`, `datagouv/mapper.py`, `SocrataAdapter`, `downloader.py`, `socrata/mapper.py`, `data/access.py`, `materialize.py`, `DataGouvAdapter`, `Query`, `Downloader`, `ckan/mapper.py`?**
  _High betweenness centrality (0.078) - this node is a cross-community bridge._
- **Why does `FormatError` connect `FormatError` to `BaseFormatReader`, `compression.py`, `sync.py`, `exceptions.py`, `ParquetReader`, `.read_batches`, `xlsx.py`, `CSVReader`, `domain/__init__.py`?**
  _High betweenness centrality (0.070) - this node is a cross-community bridge._
- **Why does `DownloadError` connect `DownloadError` to `ContentCache`, `sync.py`, `exceptions.py`, `downloader.py`, `httpx_transport.py`, `materialize.py`, `Downloader`, `domain/__init__.py`, `data/access.py`, `FsspecStorage`?**
  _High betweenness centrality (0.066) - this node is a cross-community bridge._
- **Are the 4 inferred relationships involving `Resource` (e.g. with `Dataset` and `License`) actually correct?**
  _`Resource` has 4 INFERRED edges - model-reasoned connections that need verification._
- **Are the 8 inferred relationships involving `DataSluiceError` (e.g. with `DataPlaneResourceReader` and `_StreamClosingBytesIO`) actually correct?**
  _`DataSluiceError` has 8 INFERRED edges - model-reasoned connections that need verification._
- **Are the 8 inferred relationships involving `BatchStream` (e.g. with `DataPlaneResourceReader` and `_StreamClosingBytesIO`) actually correct?**
  _`BatchStream` has 8 INFERRED edges - model-reasoned connections that need verification._
- **Are the 6 inferred relationships involving `BaseAuth` (e.g. with `APIKeyAuth` and `BasicAuth`) actually correct?**
  _`BaseAuth` has 6 INFERRED edges - model-reasoned connections that need verification._
