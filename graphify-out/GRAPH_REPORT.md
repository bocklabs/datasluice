# Graph Report - datasluice  (2026-07-29)

## Corpus Check
- 118 files · ~27,197 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1504 nodes · 2628 edges · 125 communities (82 shown, 43 thin omitted)
- Extraction: 91% EXTRACTED · 9% INFERRED · 0% AMBIGUOUS · INFERRED: 229 edges (avg confidence: 0.73)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `77c819de`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- HTTP Transport Layer
- Content Cache
- CLI Search & Runtime
- Authentication Strategies
- Bearer Auth & Credentials
- Base Adapter & Connectors
- Project Docs & Contributing
- API Key Auth
- Resource Mapping
- Access & Detection Models
- Artifact & Catalog Models
- DataGouv Adapter
- Credentials & Redirect
- Transport Retry & Pagination
- Dataset & License Models
- File Cache
- Cache Port Protocol
- Rate Limit & Config
- Architecture Docs
- CKAN Mapper
- Filesystem Layer
- Socrata Adapter
- Checksums & Errors
- Logging & Redaction
- CI/CD Workflows
- Format Readers Base
- Storage & Downloaders
- DuckDB Integration
- CLI App Core
- CKAN Adapter & Factory
- Portal Error Mapping
- Fsspec Storage
- Discovery & Airflow
- Host Credential Provider
- DataGouv Mapper
- Plugin Manager
- Port Protocol Tests
- Transport Port
- Fsspec Storage Tests
- Local Storage
- Portal Detection Fingerprints
- Adapter Exceptions
- Retry & Backoff
- Sync State Model
- HTTP Retry Errors
- test_fsspec_storage.py
- FormatError
- test_retry.py
- Download CLI Tests
- Brand Logo Assets
- Detect CLI Command
- HTTPX Transport Client
- Transport Protocol Tests
- host_provider.py
- Abstract Storage Backend
- JSONReader
- Socrata Factory & Context
- CSVReader
- Custom Adapter Skeleton
- Pre-commit Tooling
- BasicAuth
- Test Fixtures Config
- Dead Settings Tests
- Security Tooling
- PR Automation Workflows
- Layer Architecture Concepts
- Release Please Config
- Domain Purity Tests
- Integration Import Tests
- No Global State Tests
- CI Security Scanning
- Documentation Logo
- Issue & PR Templates
- Auth Apply Method
- Connectors Package Init
- Integrations Package Init
- Test Helpers Init
- Funding Config
- Issue Template Config
- Pyproject Metadata
- paginate
- _ErrorTranslatingReader
- CatalogCapabilities
- Schema
- Path
- Path
- Path
- _StubStorage
- Any
- Protocol
- Any
- .read
- Path
- Any
- Any
- DataFrame
- Any
- DataFrame
- PeekableReader
- data/access.py
- host_provider.py
- ._open_http_download
- BasicAuth
- HeadersAuth
- Resource
- ABC
- IterableBytesIO
- to_arrow_schema
- RateLimiter
- ._action
- .search
- UnsupportedQueryFieldError
- CatalogPort
- .get_dataset
- .search
- __getattr__
- Query
- SearchResult
- Any
- test_package.py
- Any

## God Nodes (most connected - your core abstractions)
1. `ContentCache` - 36 edges
2. `DataSluiceSession` - 36 edges
3. `BaseAuth` - 31 edges
4. `HttpClient` - 30 edges
5. `HttpxTransport` - 30 edges
6. `BearerAuth` - 26 edges
7. `RetryPolicy` - 26 edges
8. `MockResponse` - 25 edges
9. `start_test_server()` - 25 edges
10. `DownloadError` - 21 edges

## Surprising Connections (you probably didn't know these)
- `test_bearer_auth()` --calls--> `BearerAuth`  [INFERRED]
  tests/unit/auth/test_auth.py → src/datasluice/auth/bearer.py
- `test_bearer_auth_repr_redacts_token()` --calls--> `BearerAuth`  [INFERRED]
  tests/unit/auth/test_auth.py → src/datasluice/auth/bearer.py
- `test_no_auth()` --calls--> `NoAuth`  [INFERRED]
  tests/unit/auth/test_auth.py → src/datasluice/auth/none.py
- `test_socrata_adapter_satisfies_catalog_port()` --calls--> `SocrataAdapter`  [INFERRED]
  tests/unit/ports/test_capability_probing.py → src/datasluice/connectors/socrata/adapter.py
- `test_socrata_adapter_satisfies_organization_catalog()` --calls--> `SocrataAdapter`  [INFERRED]
  tests/unit/ports/test_capability_probing.py → src/datasluice/connectors/socrata/adapter.py

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

## Communities (125 total, 43 thin omitted)

### Community 0 - "HTTP Transport Layer"
Cohesion: 0.06
Nodes (53): BaseHTTPRequestHandler, Response, HttpxTransport, Return whether sensitive headers must be stripped on this redirect hop., Drive the manual redirect loop, stripping sensitive headers per hop (Pattern 1)., Perform an HTTP request and return the raw response body.          Raises:, GET *url* and return the response as parsed JSON (non-dicts wrapped under ``"dat, GET *url* and return the raw bytes (for file downloads). (+45 more)

### Community 1 - "Content Cache"
Cohesion: 0.06
Nodes (49): Connection, ContentCache, Any, Return the SHA-256 hexdigest of *key* (SEC-06 carry-forward, D-P3-09)., Return the absolute content-file path for a SHA-256 digest., Return cached bytes for *key*, or ``None`` on miss / expiry / writing., Store *data* under *key* (CachePort signature UNCHANGED, D-P3-12)., Store *data* under *key* with ETag/Last-Modified sidecar (D-P3-12).          Pha (+41 more)

### Community 2 - "CLI Search & Runtime"
Cohesion: 0.09
Nodes (20): Any, Tests for DataSluiceSession Phase 3 kwargs + injectables (Success Criterion 5)., An injected StoragePort is stored on the session (D-P3-20)., ConnectorContext fields stay exactly (base_url, transport, auth, page_size) (D-P, The session stays portal()/search()-only (D-P3-22)., User-defined transport stub satisfying the Transport Protocol structurally., Stub satisfying the StoragePort Protocol., Stub satisfying the CredentialProvider Protocol. (+12 more)

### Community 3 - "Authentication Strategies"
Cohesion: 0.15
Nodes (11): Dataset, Resource, Adapter for Socrata-powered open-data portals.      Uses the Socrata Discovery A, Call the Socrata Discovery API and return parsed JSON., Fetch a dataset (view) by its 4x4 identifier., Return resources for *dataset_id*., SocrataAdapter, Socrata portal adapter.  Socrata (Tyler Technologies) powers many US city, count (+3 more)

### Community 4 - "Bearer Auth & Credentials"
Cohesion: 0.08
Nodes (31): datetime, Lock, Refresher, BearerAuth, Any, Authenticate requests using a bearer token in the ``Authorization`` header., HostCredentialProvider, Drop the cached credential for *host* (off-port; D-P3-15).          Called by :c (+23 more)

### Community 5 - "Base Adapter & Connectors"
Cohesion: 0.13
Nodes (19): License, ResourceAccess, data.gouv.fr (udata) adapter implementation.  Communicates with the udata REST A, map_dataset(), map_license(), map_organization(), map_resource(), Dataset (+11 more)

### Community 6 - "Project Docs & Contributing"
Cohesion: 0.10
Nodes (28): Conventional Commits, DataSluice (Public API class), datasluice_source (dlt source factory), DataSluiceOperator (Airflow operator), Discovery Layer (datasluice.discovery), Airflow Integration Example, CKAN Example, data.gouv.fr Example (+20 more)

### Community 7 - "API Key Auth"
Cohesion: 0.08
Nodes (20): APIKeyAuth, Any, Authenticate requests using an API key.      Args:         api_key: The API key, BasicAuth, Any, Authenticate requests using HTTP Basic credentials.      Args:         username:, HeadersAuth, Any (+12 more)

### Community 9 - "Access & Detection Models"
Cohesion: 0.16
Nodes (18): HttpDownload, LocalFile, ObjectStorage, QueryAccess, ResourceAccess sum-type describing how a resource is reached., Base descriptor for how a resource is accessed.      Subclasses discriminate on, Resource fetched over HTTP(S).      Attributes:         url: Absolute URL to dow, Resource stored in object storage (S3, GCS, Azure Blob).      Attributes: (+10 more)

### Community 10 - "Artifact & Catalog Models"
Cohesion: 0.08
Nodes (24): Artifact, Artifact model — the materialized output reference (INTG-10)., A materialized artifact produced by the data plane.      Attributes:         uri, DetectionEvidence, DetectionResult, Detection models for evidence-based portal identification., A single piece of evidence produced by a detection check.      Attributes:, Outcome of portal auto-detection with confidence and evidence.      Attributes: (+16 more)

### Community 11 - "DataGouv Adapter"
Cohesion: 0.18
Nodes (10): IO, IterableBytesIO — adapt an ``Iterable[bytes]`` into a non-seekable BinaryIO (D-P, ParquetReader, Any, BaseFormatReader, Streaming Parquet reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-10).  Mig, Stream a Parquet ``BinaryIO`` source into Arrow ``RecordBatch`` objects.      On, Yield ``RecordBatch`` objects by streaming Parquet row groups.          Args: (+2 more)

### Community 12 - "Credentials & Redirect"
Cohesion: 0.13
Nodes (22): HTTPMessage, CredentialScope, Credential scoping model for host-bound credential policy., Host-scoped policy controlling where credentials may be sent.      Attributes:, CredentialAwareRedirectHandler, Request, Redirect handling that strips credentials on cross-origin or scheme-downgrade re, Strip sensitive headers when a redirect crosses origins or downgrades to plain H (+14 more)

### Community 13 - "Transport Retry & Pagination"
Cohesion: 0.09
Nodes (25): API-key authentication strategy.  Supports passing the key via a header (default, BaseAuth, ABC, Abstract base for authentication strategies., Protocol for pluggable authentication.      Each strategy knows how to decorate, HTTP Basic authentication strategy., Bearer-token (OAuth 2.0 / JWT) authentication strategy., Custom-headers authentication strategy.  Useful for portals that expect a non-st (+17 more)

### Community 14 - "Dataset & License Models"
Cohesion: 0.33
Nodes (5): CatalogCapabilities, CatalogCapabilities model — query-field-level capability contract (D-07)., Capabilities a catalog connector advertises to the runtime.      Phase 5's rejec, test_catalog_capabilities_defaults(), test_catalog_capabilities_is_frozen()

### Community 15 - "File Cache"
Cohesion: 0.15
Nodes (16): FileCache, Path, A time-based file cache.      Args:         cache_dir: Directory to store cached, Return cached bytes for *key*, or ``None`` if missing/expired., Store *data* under *key* and return the cache file path., Return ``True`` if *key* is cached and not expired., Remove all entries from the cache., Path (+8 more)

### Community 16 - "Cache Port Protocol"
Cohesion: 0.21
Nodes (12): _batch_from_rows(), _fmt_coord(), GeoJSONReader, Any, BaseFormatReader, Streaming GeoJSON reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-11).  Mig, Format a coordinate value: integers without trailing ``.0``; floats as-is., Stream a GeoJSON ``BinaryIO`` source into Arrow ``RecordBatch`` objects.      Ea (+4 more)

### Community 17 - "Rate Limit & Config"
Cohesion: 0.15
Nodes (4): PeekableReader, Read up to *n* bytes into the buffer WITHOUT consuming them.          The peeked, Wrap a ``BinaryIO`` byte source with one-chunk lookahead.      Buffers peeked by, WriteableBuffer

### Community 18 - "Architecture Docs"
Cohesion: 0.11
Nodes (17): Adapter isolation principle, Adapter 4-module pattern (adapter/mapper/pagination/errors), Adapters Layer (datasluice.adapters), Auth Layer (datasluice.auth), BaseAdapter protocol, CKAN adapter, CKAN portal platform, Composable transport (decorators) principle (+9 more)

### Community 19 - "CKAN Mapper"
Cohesion: 0.21
Nodes (11): _parse_retry_after(), Parse a ``Retry-After`` header into a delay in seconds.      Supports both delta, _full_jitter_delay(), Return a full-jitter sleep in ``[0, min(cap, base * 2**attempt)]``., Unit tests for retry classification, full-jitter backoff, and Retry-After parsin, test_full_jitter_delay_caps_at_max_delay(), test_full_jitter_delay_within_range(), test_parse_retry_after_delta_seconds() (+3 more)

### Community 20 - "Filesystem Layer"
Cohesion: 0.06
Nodes (38): AbstractFileSystem, open_filesystem(), Any, Centralised filesystem factory (INFRA-05).  All fsspec backend instantiation flo, Instantiate the correct fsspec backend for *uri* (INFRA-05, D-P3-20).      Deleg, __getattr__(), Lazily export ContentCache, FsspecStorage, and open_filesystem (D-P3-01 lazy dis, Unit tests for the ``open_filesystem`` factory (INFRA-05, D-P3-19/D-P3-20).  The (+30 more)

### Community 21 - "Socrata Adapter"
Cohesion: 0.15
Nodes (11): BaseAdapter, DataGouvAdapter, Dataset, Organization, Resource, Fetch organization metadata via ``/organizations/{slug}/``., Adapter for data.gouv.fr and other udata-powered portals.      Uses the udata RE, Call a udata API endpoint and return the parsed JSON. (+3 more)

### Community 22 - "Checksums & Errors"
Cohesion: 0.05
Nodes (32): ABC, BaseAuth, BaseAdapter, Dataset, Query, Resource, SearchResult, Abstract base class for all portal adapters. (+24 more)

### Community 23 - "Logging & Redaction"
Cohesion: 0.16
Nodes (19): configure_logging(), Any, LogRecord, Redact known sensitive keys from log records (INFRA-06, D-P3-18).      Walks ``r, Configure the package-level logger.      Args:         level: Logging level (e.g, RedactingFilter, _make_record(), LogRecord (+11 more)

### Community 24 - "CI/CD Workflows"
Cohesion: 0.16
Nodes (19): Conventional Commits Release Pipeline (release-please -> publish), uv package manager (used across all workflows), Dependabot Config (github-actions + pip weekly), CI Workflow, CI All checks pass gate (alls-green), CI Build & validate package job (uv build + twine), CI Coverage job (combine + report), CI Lint & format job (ruff) (+11 more)

### Community 26 - "Storage & Downloaders"
Cohesion: 0.27
Nodes (7): BaseOperator, DataSluiceOperator, _import_operator(), Any, Apache Airflow integration: DataSluice operators for DAGs.  Requires ``apache-ai, Import the Airflow BaseOperator lazily., Factory that returns an Airflow ``BaseOperator`` subclass.      Usage::

### Community 27 - "DuckDB Integration"
Cohesion: 0.15
Nodes (12): BatchStream, DuckDB integration (read paths removed per D-P4-18; SEC-03 utility preserved)., Validate that *table_name* is a safe SQL identifier.      Args:         table_na, Register *stream* as a named DuckDB relation (INTG-04, D-P6-14).      No SQL str, to_duckdb(), _validate_table_name(), Path, SQL injection regression tests for the DuckDB integration (SEC-03/QUAL-07). (+4 more)

### Community 28 - "CLI App Core"
Cohesion: 0.18
Nodes (10): HttpClient, Any, Perform an HTTP request and return the raw response body.          Raises:, GET *url* and return the response as text., GET *url* and return the response as parsed JSON., GET *url* and return the raw bytes (for file downloads)., Thin HTTP client wrapping :mod:`urllib` with auth, retry, and rate-limiting., Unit tests for Transport Protocol structural conformance with HttpClient. (+2 more)

### Community 29 - "CKAN Adapter & Factory"
Cohesion: 0.13
Nodes (19): map_dataset(), map_license(), map_organization(), map_resource(), Dataset, Organization, Resource, Mapping functions to convert CKAN-native JSON into domain models. (+11 more)

### Community 30 - "Portal Error Mapping"
Cohesion: 0.21
Nodes (15): map_ckan_error(), CKAN-specific error mapping., Map a CKAN HTTP error response to a DataSluice exception.      Args:         sta, map_datagouv_error(), data.gouv.fr (udata) specific error mapping., Map a udata HTTP error response to a DataSluice exception., map_socrata_error(), Socrata-specific error mapping. (+7 more)

### Community 31 - "Fsspec Storage"
Cohesion: 0.19
Nodes (8): FsspecStorage, Any, Adapter wrapping an fsspec ``AbstractFileSystem`` to satisfy ``StoragePort`` (CO, Persist *data* under *path* and return the resulting URI string.          Args:, Read and return the bytes stored under *path*., Return ``True`` if *path* exists in this filesystem., Return the backend-native path for *path*.          Absolute URIs pass through u, Reconstruct a URI string for *path* (CORR-05).

### Community 32 - "Discovery & Airflow"
Cohesion: 0.20
Nodes (6): IterableBytesIO, _content_encoding_from_headers(), HttpDownload: stream via StreamingTransport or buffer via urllib fallback (D-P4-, Extract a lowercased Content-Encoding value from response headers., IterableBytesIO that releases the StreamResponse + transport context on close., _StreamClosingBytesIO

### Community 33 - "Host Credential Provider"
Cohesion: 0.17
Nodes (14): Any, BaseException, apply_compression(), _detect_format(), _ErrorTranslatingReader, Transparent decompression decorator pipeline (DATA-06, D-P4-12).  Sits BETWEEN a, Return the compression format key from magic bytes or content-encoding hint., Spool ZIP body to BytesIO, extract the largest member (RESEARCH Pitfall 2 + OQ5) (+6 more)

### Community 34 - "DataGouv Mapper"
Cohesion: 0.13
Nodes (12): Protocol, BatchStream, Any, BatchStream — context-managed Arrow RecordBatch stream (DATA-01, DATA-02, D-P4-1, Context-managed Arrow RecordBatch stream.      Wraps a ``pa.RecordBatchReader``, The pa.Schema for batches yielded by this stream., Yield Arrow ``RecordBatch`` objects from the wrapped source.          Raises:, Release the underlying reader; idempotent (safe to call multiple times). (+4 more)

### Community 35 - "Plugin Manager"
Cohesion: 0.11
Nodes (17): PluginFailure, PluginManager, Record of a failed plugin discovery or load.      Attributes:         name: Entr, Registry-free connector manager backed by ``importlib.metadata``.      Built-in, Register *factory* programmatically (used by tests, D-06)., Return the factory callable for *name*.          Raises:             AdapterNotF, Return a sorted list of all registered connector names., Return a copy of the recorded plugin load failures. (+9 more)

### Community 37 - "Transport Port"
Cohesion: 0.11
Nodes (13): AbstractContextManager, Port Protocol interfaces for DataSluice — unstable boundary contracts., Protocol, Storage port Protocol returning URI references (CORR-05)., Boundary protocol for byte storage addressed by path/URI strings., StoragePort, Any, Protocol (+5 more)

### Community 38 - "Fsspec Storage Tests"
Cohesion: 0.17
Nodes (13): inspect(), ``datasluice inspect`` command., Inspect a single dataset in detail., Composition root and plugin machinery for DataSluice., DataSluiceSession, Public facade and composition root for DataSluice.      Wires the :class:`Plugin, Unit tests for the DataSluiceSession composition root (ARCH-03).  Covers zero-co, test_auth_param() (+5 more)

### Community 39 - "Local Storage"
Cohesion: 0.16
Nodes (20): ChecksumMismatchError, Raised when a downloaded file's checksum does not match., compute_hash(), compute_md5(), compute_sha256(), Path, Checksum (hash) computation and verification., Compute the hex digest of *file_path* using *algorithm*. (+12 more)

### Community 40 - "Portal Detection Fingerprints"
Cohesion: 0.15
Nodes (9): detect_portal(), Alias for :func:`detect_portal_type`., Portal type fingerprints for auto-detection.  Each entry maps a fingerprint (URL, Portal type discovery and auto-detection., PortalMetadata, Portal metadata describing known portal instances., Metadata about a detected or known portal.      Attributes:         portal_type:, Unit tests for discovery fingerprints. (+1 more)

### Community 41 - "Adapter Exceptions"
Cohesion: 0.20
Nodes (5): CachePort, Protocol, Cache port Protocol for content-addressed byte caching., Boundary protocol for a simple key/byte cache., Lazily construct the default ContentCache (plan 03-03) if importable.          R

### Community 42 - "Retry & Backoff"
Cohesion: 0.11
Nodes (24): Exception, AdapterError, AdapterNotFoundError, AuthenticationError, ConfigError, DataSluiceError, DecompressionError, FormatError (+16 more)

### Community 43 - "Sync State Model"
Cohesion: 0.16
Nodes (9): SyncState model for incremental sync cursors and watermarks (SYNC-03)., Incremental synchronization state for a resource or connector.      Attributes:, SyncState, Protocol, State store port Protocol for incremental sync state (SYNC-01)., Boundary protocol for persisting incremental sync state., StateStore, test_sync_state_defaults() (+1 more)

### Community 44 - "HTTP Retry Errors"
Cohesion: 0.12
Nodes (13): Logger, Host-scoped credential resolver with single-flight refresh (INFRA-04).  ``HostCr, Concrete ``ResourceReader`` implementation with access-kind dispatch (DATA-04, D, _normalize_base_url(), Auto-detection of portal type from a URL.  The detector probes well-known API en, Ensure *url* has a scheme and no trailing slash., dlt (data load tool) integration: use DataSluice as a dlt source.  Requires ``dl, Content-addressed cache backed by a SQLite WAL index + content files (INFRA-03). (+5 more)

### Community 45 - "test_fsspec_storage.py"
Cohesion: 0.23
Nodes (10): LocalStorage, Path, Local-filesystem storage backend.      Args:         base_dir: Root directory fo, Path, Unit tests for LocalStorage path-traversal containment., test_storage_rejects_dotdot_segment(), test_storage_rejects_path_traversal(), test_storage_round_trip() (+2 more)

### Community 47 - "test_retry.py"
Cohesion: 0.12
Nodes (16): HTTP client with retry, rate-limiting, and authentication support., Render *body* as text, truncating to *limit* characters., _truncate_body(), _host_credential_provider_type(), Any, httpx-backed HTTP transport satisfying the Transport + StreamingTransport Protoc, Lazily resolve ``HostCredentialProvider`` (plan 03-04), returning ``None`` if it, RateLimiter (+8 more)

### Community 48 - "Download CLI Tests"
Cohesion: 0.35
Nodes (8): _make_dataset(), _patch_client(), MonkeyPatch, Path, Unit tests for the ``datasluice download`` command., _RecordingDownloader, test_download_format_filtering(), test_download_no_matching_resources_exits_with_error()

### Community 49 - "Brand Logo Assets"
Cohesion: 0.29
Nodes (10): DataSluice Logo (Brand Mark), Datasluice Brand Logo (no background), Circuit Pathways Connectivity Element, Code Brackets (</>) Programming Icon, Funnel Processing Element, Filtered Output Basin, Padlock Security Icon, Table/Grid Tech Icon (+2 more)

### Community 50 - "Detect CLI Command"
Cohesion: 0.24
Nodes (8): _batch_from_rows(), Any, BaseFormatReader, Streaming XLSX reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-10).  Migrat, Stream an XLSX ``BinaryIO`` source into Arrow ``RecordBatch`` objects., Yield ``RecordBatch`` objects by chunking openpyxl ``iter_rows``.          Args:, Build a single ``RecordBatch`` from a chunk of row dicts., XLSXReader

### Community 51 - "HTTPX Transport Client"
Cohesion: 0.22
Nodes (7): Raised on HTTP 5xx responses that should be retried., RetryableHTTPError, Yield a :class:`StreamResponse` for streaming the response body (D-P3-06/D-P3-07, Backend-agnostic streaming response wrapper (D-P3-07).      Iterable for byte ch, Release the underlying httpx response., StreamResponse, test_retryable_http_error_carries_status_code()

### Community 52 - "Transport Protocol Tests"
Cohesion: 0.20
Nodes (9): Protocol-level tests for the Transport and StreamingTransport ports.  These comp, Transport must keep its runtime-checkable flag (carry-forward)., StreamingTransport must be a @runtime_checkable Protocol (INFRA-07)., StreamingTransport Protocol surface must declare ``stream``., HttpxTransport must satisfy Transport AND StreamingTransport (INFRA-01).      Th, test_httpx_transport_satisfies_both_protocols(), test_streaming_transport_protocol_declares_stream(), test_streaming_transport_protocol_is_runtime_checkable() (+1 more)

### Community 54 - "Abstract Storage Backend"
Cohesion: 0.10
Nodes (26): DownloadError, Raised when a resource download fails., Simple file-based cache for downloaded resources., Downloader, Path, Resource downloader with caching and checksum verification., Download multiple *resources* into *dest*., Downloads resources to local or pluggable storage.      Args:         transport: (+18 more)

### Community 56 - "Socrata Factory & Context"
Cohesion: 0.13
Nodes (15): create_ckan_connector(), Factory for the CKAN connector (entry-point target).  ``create_ckan_connector``, Construct a :class:`CKANAdapter` wired to the context's transport/auth., create_datagouv_connector(), Factory for the data.gouv.fr connector (entry-point target).  ``create_datagouv_, Construct a :class:`DataGouvAdapter` wired to the context's transport/auth., create_socrata_connector(), Factory for the Socrata connector (entry-point target).  ``create_socrata_connec (+7 more)

### Community 59 - "Custom Adapter Skeleton"
Cohesion: 0.10
Nodes (24): Dataset, Dataset model representing a collection of related open-data resources., A dataset is a logical grouping of one or more resources.      Attributes:, Portal-agnostic domain models for DataSluice., License, License model representing the license under which data is published., A license under which an open-data resource or dataset is published.      Attrib, Organization (+16 more)

### Community 60 - "Pre-commit Tooling"
Cohesion: 0.50
Nodes (5): pre-commit-hooks (basic hooks), pytest testing framework, Ruff linter and formatter, ty type checker (Astral), Pre-commit Configuration

### Community 61 - "BasicAuth"
Cohesion: 0.12
Nodes (15): Resource, Return resources for *dataset_id*., _chain(), DataPlaneResourceReader, Dispatch to the format reader and wrap output in BatchStream., ObjectStorage: open via open_filesystem().open() (D-P4-07)., LocalFile: open(path, 'rb')., Strip the fsspec storage scheme from *uri* to produce the path component. (+7 more)

### Community 62 - "Test Fixtures Config"
Cohesion: 0.40
Nodes (4): fixtures_dir(), Path, Shared pytest fixtures and configuration for DataSluice tests., Return the path to the test fixtures directory.

### Community 65 - "Security Tooling"
Cohesion: 0.67
Nodes (3): CodeQL (security scanning), Dependabot (dependency updates), Zizmor (workflow audit)

### Community 66 - "PR Automation Workflows"
Cohesion: 0.83
Nodes (4): AI-assisted PR automation pattern, Renovate bot exclusion pattern (skip AI review for bots), OpenCodeReview PR Review Workflow (alibaba open-code-review), PR Agent Workflow (/describe auto-generate title & description)

### Community 67 - "Layer Architecture Concepts"
Cohesion: 0.50
Nodes (4): Formats Layer (datasluice.formats), Integrations Layer (datasluice.integrations), IO Layer (datasluice.io), Lazy imports of optional deps principle

### Community 68 - "Release Please Config"
Cohesion: 0.50
Nodes (3): bootstrap-sha, packages, $schema

### Community 71 - "Integration Import Tests"
Cohesion: 0.50
Nodes (3): Unit tests for integration module imports (no heavy deps required)., Integration modules should be importable without their optional deps., test_integration_modules_importable()

### Community 73 - "CI Security Scanning"
Cohesion: 1.00
Nodes (3): Defense-in-depth CI security scanning pattern, CodeQL Workflow (actions + python security analysis), Zizmor Workflow Security Analysis

### Community 74 - "Documentation Logo"
Cohesion: 1.00
Nodes (3): Datasluice Brand Identity, Datasluice Logo / Brand Mark, Datasluice Documentation Logo Asset

### Community 75 - "Issue & PR Templates"
Cohesion: 0.67
Nodes (3): Bug Report Issue Template, Feature Request Issue Template, Pull Request Template (affected areas + AI provenance)

### Community 76 - "Auth Apply Method"
Cohesion: 0.25
Nodes (5): Pagination helpers for the Socrata SODA2 API.  Socrata uses offset / limit pagin, Parameters for a single Socrata results page., Return the parameters for the following page., Convert to query-string parameters for the SODA2 API., SocrataPage

### Community 88 - "paginate"
Cohesion: 0.25
Nodes (5): CKANPage, Pagination helpers for the CKAN Action API., Parameters for a single CKAN search results page.      CKAN uses ``start`` (offs, Return the parameters for the following page., Convert to query-string parameters for the CKAN API.

### Community 90 - "_ErrorTranslatingReader"
Cohesion: 0.21
Nodes (8): CKANAdapter, CKAN adapter implementation.  Communicates with the CKAN Action API (``/api/3/ac, Adapter for CKAN-powered open-data portals.      Uses the CKAN Action API at ``{, CKAN portal adapter.  CKAN is the world's most widely deployed open-data platfor, Unit tests for capability probing — isinstance discrimination across catalog por, test_ckan_adapter_satisfies_catalog_port(), test_ckan_adapter_satisfies_organization_catalog(), test_ckan_adapter_satisfies_searchable_catalog()

### Community 96 - "_StubStorage"
Cohesion: 0.40
Nodes (4): get_reader(), BaseFormatReader, Streaming format readers yielding Arrow ``RecordBatch`` (DATA-03, D-P4-10).  Reg, Return a format reader instance for *format_name*.      Args:         format_nam

### Community 109 - "host_provider.py"
Cohesion: 0.10
Nodes (17): Argument, help, Option, main(), Main Typer application for the DataSluice CLI., DataSluice — unified open-data toolkit., detect(), ``datasluice detect`` command. (+9 more)

### Community 110 - "._open_http_download"
Cohesion: 0.17
Nodes (14): Socrata adapter implementation.  Communicates with the Socrata Discovery API and, map_dataset(), map_organization(), map_resource(), Dataset, Resource, Mapping functions to convert Socrata-native JSON into domain models., Resolve the resource access descriptor per D-P5-02 for Socrata views.      Socra (+6 more)

### Community 111 - "BasicAuth"
Cohesion: 0.25
Nodes (4): An injected CachePort is stored as the session cache (D-P3-02)., Stub satisfying the CachePort Protocol., _StubCache, test_cache_injectable()

### Community 112 - "HeadersAuth"
Cohesion: 0.25
Nodes (5): DataGouvPage, Pagination helpers for the data.gouv.fr (udata) API.  udata uses page-number / p, Parameters for a single udata results page (1-based)., Return the parameters for the following page., Convert to query-string parameters for the udata API.

### Community 115 - "IterableBytesIO"
Cohesion: 0.14
Nodes (6): RawIOBase, IterableBytesIO, Mark the stream closed and exhaust the iterator., Wrap an ``Iterable[bytes]`` into a non-seekable ``BinaryIO``.      The canonical, Read up to ``len(b)`` bytes into the provided buffer.          Args:, Read up to ``n`` bytes; ``n=-1`` reads all remaining bytes.          Args:

### Community 116 - "to_arrow_schema"
Cohesion: 0.29
Nodes (6): Schema, Domain Schema → Arrow Schema mapper and batch unification helper.  The :func:`to, Derive a ``pa.Schema`` from a domain :class:`Schema` for display.      Maps know, Concatenate ``RecordBatch`` objects under a unified ``pa.Schema`` (DATA-08, D-P4, to_arrow_schema(), unify_batches()

### Community 117 - "RateLimiter"
Cohesion: 0.12
Nodes (23): __getattr__(), Transport layer: HTTP client, retry, rate-limiting, and pagination.  ``HttpxTran, Lazily resolve httpx-backed symbols on first attribute access (PEP 562)., paginate(), PaginationConfig, T, Generic pagination helpers shared across adapters., Common pagination parameters.      Attributes:         page_size: Number of item (+15 more)

### Community 118 - "._action"
Cohesion: 0.25
Nodes (5): Dataset, Organization, Fetch organization metadata via ``organization_show``., Call a CKAN Action API endpoint and return the ``result`` dict., Fetch a dataset via ``package_show``.

### Community 121 - "UnsupportedQueryFieldError"
Cohesion: 0.15
Nodes (9): DetectionResult, _is_set(), Query, Pre-flight reject policy for unsupported ``Query`` filter fields (D-P5-06).  The, Raise ``UnsupportedQueryFieldError`` if *query* uses a field not in *supported*., Return ``True`` when *value* counts as a set filter field.      ``None``, empty, _reject_unsupported_fields(), Raised when a caller sets a ``Query`` filter field the connector rejects (D-P5-0 (+1 more)

### Community 122 - "CatalogPort"
Cohesion: 0.11
Nodes (17): Query, Query model for searching datasets across portals., Portal-agnostic search parameters.      Attributes:         text: Free-text sear, datasluice_source(), Any, Return a dlt source that yields datasets from *portal*.      Args:         porta, CatalogPort, OrganizationCatalog (+9 more)

### Community 125 - ".search"
Cohesion: 0.32
Nodes (5): Query, SearchResult, Search datasets via ``package_search``.          Translates every set supported, Search datasets via ``/datasets/``.          Translates every set supported ``Qu, Search datasets via the Discovery API.

## Knowledge Gaps
- **25 isolated node(s):** `datasluice`, `$schema`, `bootstrap-sha`, `packages`, `Funding Config (Buy Me a Coffee: nitishraj)` (+20 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **43 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `DataSluiceSession` connect `Fsspec Storage Tests` to `CLI Search & Runtime`, `Plugin Manager`, `CatalogPort`, `Access & Detection Models`, `Adapter Exceptions`, `HTTP Retry Errors`, `host_provider.py`, `Transport Retry & Pagination`, `BasicAuth`, `Socrata Factory & Context`, `Storage & Downloaders`?**
  _High betweenness centrality (0.066) - this node is a cross-community bridge._
- **Why does `DownloadError` connect `Abstract Storage Backend` to `Content Cache`, `Local Storage`, `Access & Detection Models`, `Retry & Backoff`, `HTTP Retry Errors`, `test_fsspec_storage.py`, `Fsspec Storage`?**
  _High betweenness centrality (0.047) - this node is a cross-community bridge._
- **Why does `ContentCache` connect `Content Cache` to `HTTP Retry Errors`, `Abstract Storage Backend`?**
  _High betweenness centrality (0.042) - this node is a cross-community bridge._
- **Are the 6 inferred relationships involving `DataSluiceSession` (e.g. with `ConnectorContext` and `PluginManager`) actually correct?**
  _`DataSluiceSession` has 6 INFERRED edges - model-reasoned connections that need verification._
- **Are the 6 inferred relationships involving `BaseAuth` (e.g. with `APIKeyAuth` and `BasicAuth`) actually correct?**
  _`BaseAuth` has 6 INFERRED edges - model-reasoned connections that need verification._
- **Are the 16 inferred relationships involving `HttpClient` (e.g. with `PortalError` and `RateLimitError`) actually correct?**
  _`HttpClient` has 16 INFERRED edges - model-reasoned connections that need verification._
- **What connects `datasluice`, `$schema`, `bootstrap-sha` to the rest of the system?**
  _25 weakly-connected nodes found - possible documentation gaps or missing edges._
