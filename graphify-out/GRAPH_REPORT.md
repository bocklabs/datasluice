# Graph Report - .  (2026-07-25)

## Corpus Check
- 185 files · ~123,973 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1335 nodes · 2556 edges · 87 communities (75 shown, 12 thin omitted)
- Extraction: 91% EXTRACTED · 9% INFERRED · 0% AMBIGUOUS · INFERRED: 235 edges (avg confidence: 0.73)
- Token cost: 1,800 input · 3,200 output

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
- Pandas & Format Readers
- GeoJSON & XLSX Readers
- JSON Format Reader
- Download CLI Tests
- Brand Logo Assets
- Detect CLI Command
- HTTPX Transport Client
- Transport Protocol Tests
- CSV Format Reader
- Abstract Storage Backend
- Airflow Operator
- Socrata Factory & Context
- Downloader Class
- Storage Port Protocol
- Custom Adapter Skeleton
- Pre-commit Tooling
- Parquet Reader
- Test Fixtures Config
- Dead Settings Tests
- Package API Tests
- Security Tooling
- PR Automation Workflows
- Layer Architecture Concepts
- Release Please Config
- Format Reader Base
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

## God Nodes (most connected - your core abstractions)
1. `DataSluiceSession` - 38 edges
2. `Resource` - 37 edges
3. `ContentCache` - 36 edges
4. `BaseAuth` - 33 edges
5. `HttpClient` - 32 edges
6. `Dataset` - 30 edges
7. `HttpxTransport` - 30 edges
8. `BearerAuth` - 26 edges
9. `RetryPolicy` - 26 edges
10. `Organization` - 25 edges

## Surprising Connections (you probably didn't know these)
- `test_bearer_auth()` --calls--> `BearerAuth`  [INFERRED]
  tests/unit/auth/test_auth.py → src/datasluice/auth/bearer.py
- `test_bearer_auth_repr_redacts_token()` --calls--> `BearerAuth`  [INFERRED]
  tests/unit/auth/test_auth.py → src/datasluice/auth/bearer.py
- `test_no_auth()` --calls--> `NoAuth`  [INFERRED]
  tests/unit/auth/test_auth.py → src/datasluice/auth/none.py
- `test_ckan_adapter_satisfies_catalog_port()` --calls--> `CKANAdapter`  [INFERRED]
  tests/unit/ports/test_capability_probing.py → src/datasluice/connectors/ckan/adapter.py
- `test_ckan_adapter_satisfies_organization_catalog()` --calls--> `CKANAdapter`  [INFERRED]
  tests/unit/ports/test_capability_probing.py → src/datasluice/connectors/ckan/adapter.py

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

## Communities (87 total, 12 thin omitted)

### Community 0 - "HTTP Transport Layer"
Cohesion: 0.05
Nodes (61): BaseHTTPRequestHandler, Response, HttpClient, Any, Perform an HTTP request and return the raw response body.          Raises:, GET *url* and return the response as text., GET *url* and return the response as parsed JSON., GET *url* and return the raw bytes (for file downloads). (+53 more)

### Community 1 - "Content Cache"
Cohesion: 0.06
Nodes (49): Connection, ContentCache, Any, Return the SHA-256 hexdigest of *key* (SEC-06 carry-forward, D-P3-09)., Return the absolute content-file path for a SHA-256 digest., Return cached bytes for *key*, or ``None`` on miss / expiry / writing., Store *data* under *key* (CachePort signature UNCHANGED, D-P3-12)., Store *data* under *key* with ETag/Last-Modified sidecar (D-P3-12).          Pha (+41 more)

### Community 2 - "CLI Search & Runtime"
Cohesion: 0.05
Nodes (39): ``datasluice search`` command., Search for datasets on an open-data portal., search(), datasluice_source(), Any, Return a dlt source that yields datasets from *portal*.      Args:         porta, DataSluiceSession, Public facade and composition root for DataSluice.      Wires the :class:`Plugin (+31 more)

### Community 3 - "Authentication Strategies"
Cohesion: 0.09
Nodes (25): API-key authentication strategy.  Supports passing the key via a header (default, BaseAuth, ABC, Abstract base for authentication strategies., Protocol for pluggable authentication.      Each strategy knows how to decorate, HTTP Basic authentication strategy., Bearer-token (OAuth 2.0 / JWT) authentication strategy., Custom-headers authentication strategy.  Useful for portals that expect a non-st (+17 more)

### Community 4 - "Bearer Auth & Credentials"
Cohesion: 0.08
Nodes (30): Lock, Refresher, BearerAuth, Any, Authenticate requests using a bearer token in the ``Authorization`` header., HostCredentialProvider, Drop the cached credential for *host* (off-port; D-P3-15).          Called by :c, Resolve a :class:`BaseAuth` per host with cached expiry and single-flight refres (+22 more)

### Community 5 - "Base Adapter & Connectors"
Cohesion: 0.08
Nodes (21): BaseAdapter, ABC, Abstract base class for all portal adapters., Protocol that every portal adapter must implement.      Subclasses translate por, Search for datasets matching *query*., Search datasets via ``package_search``., Template adapter for unsupported portals.  Copy this module, rename the class, a, Socrata adapter implementation.  Communicates with the Socrata Discovery API and (+13 more)

### Community 6 - "Project Docs & Contributing"
Cohesion: 0.10
Nodes (28): Conventional Commits, DataSluice (Public API class), datasluice_source (dlt source factory), DataSluiceOperator (Airflow operator), Discovery Layer (datasluice.discovery), Airflow Integration Example, CKAN Example, data.gouv.fr Example (+20 more)

### Community 7 - "API Key Auth"
Cohesion: 0.08
Nodes (20): APIKeyAuth, Any, Authenticate requests using an API key.      Args:         api_key: The API key, BasicAuth, Any, Authenticate requests using HTTP Basic credentials.      Args:         username:, HeadersAuth, Any (+12 more)

### Community 8 - "Resource Mapping"
Cohesion: 0.09
Nodes (23): Return all downloadable resources for *dataset_id*., map_dataset(), map_organization(), map_resource(), Any, Mapping functions to convert Socrata-native JSON into domain models., Convert a Socrata view/resource dict into a :class:`Resource`., Convert a Socrata customer/owner dict into an :class:`Organization`. (+15 more)

### Community 9 - "Access & Detection Models"
Cohesion: 0.13
Nodes (24): HttpDownload, LocalFile, ObjectStorage, QueryAccess, ResourceAccess sum-type describing how a resource is reached., Base descriptor for how a resource is accessed.      Subclasses discriminate on, Resource fetched over HTTP(S).      Attributes:         url: Absolute URL to dow, Resource stored in object storage (S3, GCS, Azure Blob).      Attributes: (+16 more)

### Community 10 - "Artifact & Catalog Models"
Cohesion: 0.09
Nodes (23): Artifact, Artifact model — the materialized output reference (INTG-10)., A materialized artifact produced by the data plane.      Attributes:         uri, CatalogCapabilities, CatalogCapabilities model — query-field-level capability contract (D-07)., Capabilities a catalog connector advertises to the runtime.      Phase 5's rejec, DetectionEvidence, DetectionResult (+15 more)

### Community 11 - "DataGouv Adapter"
Cohesion: 0.10
Nodes (17): DataGouvAdapter, data.gouv.fr (udata) adapter implementation.  Communicates with the udata REST A, Adapter for data.gouv.fr and other udata-powered portals.      Uses the udata RE, Call a udata API endpoint and return the parsed JSON., Search datasets via ``/datasets/``., Fetch a dataset via ``/datasets/{id}/``., Return resources for *dataset_id*., Fetch organization metadata via ``/organizations/{slug}/``. (+9 more)

### Community 12 - "Credentials & Redirect"
Cohesion: 0.13
Nodes (21): HTTPMessage, CredentialScope, Credential scoping model for host-bound credential policy., Host-scoped policy controlling where credentials may be sent.      Attributes:, CredentialAwareRedirectHandler, Request, Strip sensitive headers when a redirect crosses origins or downgrades to plain H, Return the follow-up request, stripping sensitive headers when required. (+13 more)

### Community 13 - "Transport Retry & Pagination"
Cohesion: 0.12
Nodes (22): __getattr__(), Transport layer: HTTP client, retry, rate-limiting, and pagination.  ``HttpxTran, Lazily resolve httpx-backed symbols on first attribute access (PEP 562)., paginate(), PaginationConfig, T, Generic pagination helpers shared across adapters., Common pagination parameters.      Attributes:         page_size: Number of item (+14 more)

### Community 14 - "Dataset & License Models"
Cohesion: 0.10
Nodes (17): Fetch a single dataset by its portal-native *dataset_id*., Dataset, Dataset model representing a collection of related open-data resources., A dataset is a logical grouping of one or more resources.      Attributes:, License, License model representing the license under which data is published., A license under which an open-data resource or dataset is published.      Attrib, Organization model representing a dataset publisher. (+9 more)

### Community 15 - "File Cache"
Cohesion: 0.14
Nodes (17): FileCache, Path, Simple file-based cache for downloaded resources., A time-based file cache.      Args:         cache_dir: Directory to store cached, Return cached bytes for *key*, or ``None`` if missing/expired., Store *data* under *key* and return the cache file path., Return ``True`` if *key* is cached and not expired., Remove all entries from the cache. (+9 more)

### Community 16 - "Cache Port Protocol"
Cohesion: 0.11
Nodes (16): CachePort, Protocol, Cache port Protocol for content-addressed byte caching., Boundary protocol for a simple key/byte cache., CatalogPort, OrganizationCatalog, Protocol, Catalog port Protocols for portal catalog connectors. (+8 more)

### Community 17 - "Rate Limit & Config"
Cohesion: 0.12
Nodes (15): Default configuration constants., Configuration for DataSluice., HTTP client with retry, rate-limiting, and authentication support., Render *body* as text, truncating to *limit* characters., _truncate_body(), httpx-backed HTTP transport satisfying the Transport + StreamingTransport Protoc, RateLimiter, Rate-limiting to stay within portal request quotas. (+7 more)

### Community 18 - "Architecture Docs"
Cohesion: 0.11
Nodes (17): Adapter isolation principle, Adapter 4-module pattern (adapter/mapper/pagination/errors), Adapters Layer (datasluice.adapters), Auth Layer (datasluice.auth), BaseAdapter protocol, CKAN adapter, CKAN portal platform, Composable transport (decorators) principle (+9 more)

### Community 19 - "CKAN Mapper"
Cohesion: 0.12
Nodes (17): Call a CKAN Action API endpoint and return the ``result`` dict., Fetch a dataset via ``package_show``., Return resources for *dataset_id*., Fetch organization metadata via ``organization_show``., map_dataset(), map_license(), map_organization(), map_resource() (+9 more)

### Community 20 - "Filesystem Layer"
Cohesion: 0.11
Nodes (20): AbstractFileSystem, open_filesystem(), Any, Centralised filesystem factory (INFRA-05).  All fsspec backend instantiation flo, Instantiate the correct fsspec backend for *uri* (INFRA-05, D-P3-20).      Deleg, __getattr__(), Lazily export ContentCache, FsspecStorage, and open_filesystem (D-P3-01 lazy dis, Unit tests for the ``open_filesystem`` factory (INFRA-05, D-P3-19/D-P3-20).  The (+12 more)

### Community 21 - "Socrata Adapter"
Cohesion: 0.11
Nodes (14): Adapter for Socrata-powered open-data portals.      Uses the Socrata Discovery A, Call the Socrata Discovery API and return parsed JSON., Fetch a dataset (view) by its 4x4 identifier., Return resources for *dataset_id*., Socrata does not expose a dedicated organizations endpoint.          Returns a m, SocrataAdapter, Socrata portal adapter.  Socrata (Tyler Technologies) powers many US city, count, Unit tests for capability probing — isinstance discrimination across catalog por (+6 more)

### Community 22 - "Checksums & Errors"
Cohesion: 0.16
Nodes (20): ChecksumMismatchError, Raised when a downloaded file's checksum does not match., compute_hash(), compute_md5(), compute_sha256(), Path, Checksum (hash) computation and verification., Compute the hex digest of *file_path* using *algorithm*. (+12 more)

### Community 23 - "Logging & Redaction"
Cohesion: 0.16
Nodes (19): configure_logging(), Any, LogRecord, Redact known sensitive keys from log records (INFRA-06, D-P3-18).      Walks ``r, Configure the package-level logger.      Args:         level: Logging level (e.g, RedactingFilter, _make_record(), LogRecord (+11 more)

### Community 24 - "CI/CD Workflows"
Cohesion: 0.16
Nodes (19): Conventional Commits Release Pipeline (release-please -> publish), uv package manager (used across all workflows), Dependabot Config (github-actions + pip weekly), CI Workflow, CI All checks pass gate (alls-green), CI Build & validate package job (uv build + twine), CI Coverage job (combine + report), CI Lint & format job (ruff) (+11 more)

### Community 25 - "Format Readers Base"
Cohesion: 0.20
Nodes (12): IO, BaseFormatReader, ABC, Abstract base for format readers., Protocol for reading a specific file format into Python objects.      Each reade, GeoJSON format reader., Format readers for normalising tabular and geospatial data., JSON format reader.  Supports both line-delimited JSON (JSONL/NDJSON) and JSON a (+4 more)

### Community 26 - "Storage & Downloaders"
Cohesion: 0.20
Nodes (14): DownloadError, Raised when a resource download fails., Resource downloader with caching and checksum verification., IO layer: downloading, caching, checksums, and storage., ensure_dir(), Path, Local filesystem helpers for saving downloaded resources., Create *path* (and parents) if it does not exist; return as :class:`Path`. (+6 more)

### Community 27 - "DuckDB Integration"
Cohesion: 0.17
Nodes (17): Any, query_resource(), DuckDB integration: query resources directly with DuckDB.  Requires ``duckdb``:, Validate that *table_name* is a safe SQL identifier.      Args:         table_na, Register a remote resource as a DuckDB relation.      The URL flows through Duck, Run arbitrary *sql* against a resource and return the result.      Warning:, resource_to_relation(), _validate_table_name() (+9 more)

### Community 28 - "CLI App Core"
Cohesion: 0.12
Nodes (14): Argument, help, Option, main(), Main Typer application for the DataSluice CLI., DataSluice — unified open-data toolkit., download(), Path (+6 more)

### Community 29 - "CKAN Adapter & Factory"
Cohesion: 0.14
Nodes (12): CKANAdapter, CKAN adapter implementation.  Communicates with the CKAN Action API (``/api/3/ac, Adapter for CKAN-powered open-data portals.      Uses the CKAN Action API at ``{, create_ckan_connector(), Factory for the CKAN connector (entry-point target).  ``create_ckan_connector``, Construct a :class:`CKANAdapter` wired to the context's transport/auth., CKAN portal adapter.  CKAN is the world's most widely deployed open-data platfor, CKANPage (+4 more)

### Community 30 - "Portal Error Mapping"
Cohesion: 0.21
Nodes (15): map_ckan_error(), CKAN-specific error mapping., Map a CKAN HTTP error response to a DataSluice exception.      Args:         sta, map_datagouv_error(), data.gouv.fr (udata) specific error mapping., Map a udata HTTP error response to a DataSluice exception., map_socrata_error(), Socrata-specific error mapping. (+7 more)

### Community 31 - "Fsspec Storage"
Cohesion: 0.14
Nodes (12): FsspecStorage, Any, Adapter wrapping an fsspec ``AbstractFileSystem`` to satisfy ``StoragePort`` (CO, Persist *data* under *path* and return the resulting URI string.          Args:, Read and return the bytes stored under *path*., Return ``True`` if *path* exists in this filesystem., Return the backend-native path for *path*.          Absolute URIs pass through u, Reconstruct a URI string for *path* (CORR-05). (+4 more)

### Community 32 - "Discovery & Airflow"
Cohesion: 0.16
Nodes (10): Logger, Auto-detection of portal type from a URL.  The detector probes well-known API en, Apache Airflow integration: DataSluice operators for DAGs.  Requires ``apache-ai, dlt (data load tool) integration: use DataSluice as a dlt source.  Requires ``dl, Content-addressed cache backed by a SQLite WAL index + content files (INFRA-03)., fsspec-backed storage adapter satisfying :class:`StoragePort` (INFRA-02, CORR-05, get_logger(), Structured logging utilities for DataSluice.  Owns the canonical ``SENSITIVE_HEA (+2 more)

### Community 33 - "Host Credential Provider"
Cohesion: 0.17
Nodes (14): datetime, Exception, Host-scoped credential resolver with single-flight refresh (INFRA-04).  ``HostCr, AdapterError, AuthenticationError, ConfigError, DataSluiceError, PortalDetectionError (+6 more)

### Community 34 - "DataGouv Mapper"
Cohesion: 0.18
Nodes (13): Fetch publisher metadata for *organization_id*., map_dataset(), map_license(), map_organization(), map_resource(), Any, Mapping functions to convert data.gouv.fr (udata) JSON into domain models., Convert a udata license dict into a :class:`License`. (+5 more)

### Community 35 - "Plugin Manager"
Cohesion: 0.17
Nodes (11): PluginManager, Registry-free connector manager backed by ``importlib.metadata``.      Built-in, Register *factory* programmatically (used by tests, D-06)., Return a sorted list of all registered connector names., MonkeyPatch, Unit tests for the plugin manager and plugin failure record.  Covers ARCH-04 (en, test_duplicate_entry_point_recorded_as_failure(), test_entry_point_discovery() (+3 more)

### Community 37 - "Transport Port"
Cohesion: 0.17
Nodes (9): AbstractContextManager, Lazily initialised HTTP transport., Any, Protocol, Transport port Protocols for HTTP-like request execution.  Two narrow runtime-ch, Transport boundary Protocol satisfied structurally by HTTP clients., Streaming transport boundary Protocol (D-P3-06/D-P3-07).      `stream(url)` retu, StreamingTransport (+1 more)

### Community 38 - "Fsspec Storage Tests"
Cohesion: 0.18
Nodes (14): _memory_storage(), Unit tests for :class:`FsspecStorage` (INFRA-02, CORR-05).  Closes CORR-05 by as, write signature matches the StoragePort contract: (self, data: bytes, path: str), FsspecStorage is structurally a StoragePort (INFRA-02)., write() return value starts with the backend protocol prefix (CORR-05)., write() returns a bare str, never a pathlib.Path (CORR-05 regression)., write -> read round-trips the bytes; exists flips False -> True (INFRA-02)., Reading a key that was never written raises FileNotFoundError or DownloadError. (+6 more)

### Community 39 - "Local Storage"
Cohesion: 0.23
Nodes (10): LocalStorage, Path, Local-filesystem storage backend.      Args:         base_dir: Root directory fo, Path, Unit tests for LocalStorage path-traversal containment., test_storage_rejects_dotdot_segment(), test_storage_rejects_path_traversal(), test_storage_round_trip() (+2 more)

### Community 40 - "Portal Detection Fingerprints"
Cohesion: 0.18
Nodes (7): Portal type fingerprints for auto-detection.  Each entry maps a fingerprint (URL, Portal type discovery and auto-detection., PortalMetadata, Portal metadata describing known portal instances., Metadata about a detected or known portal.      Attributes:         portal_type:, Unit tests for discovery fingerprints., test_portal_metadata()

### Community 41 - "Adapter Exceptions"
Cohesion: 0.17
Nodes (9): AdapterNotFoundError, Raised when no adapter is registered for a portal type., PluginFailure, Plugin manager for entry-point-based connector discovery.  The :class:`PluginMan, Record of a failed plugin discovery or load.      Attributes:         name: Entr, Return the factory callable for *name*.          Raises:             AdapterNotF, Return a copy of the recorded plugin load failures., test_list_failures_returns_copy() (+1 more)

### Community 42 - "Retry & Backoff"
Cohesion: 0.19
Nodes (12): _parse_retry_after(), Parse a ``Retry-After`` header into a delay in seconds.      Supports both delta, _full_jitter_delay(), Return a full-jitter sleep in ``[0, min(cap, base * 2**attempt)]``., Unit tests for retry classification, full-jitter backoff, and Retry-After parsin, test_full_jitter_delay_caps_at_max_delay(), test_full_jitter_delay_within_range(), test_parse_retry_after_delta_seconds() (+4 more)

### Community 43 - "Sync State Model"
Cohesion: 0.18
Nodes (8): SyncState model for incremental sync cursors and watermarks (SYNC-03)., Incremental synchronization state for a resource or connector.      Attributes:, SyncState, Protocol, Boundary protocol for persisting incremental sync state., StateStore, test_sync_state_defaults(), test_sync_state_is_frozen()

### Community 44 - "HTTP Retry Errors"
Cohesion: 0.18
Nodes (6): Raised on HTTP 5xx responses that should be retried., RetryableHTTPError, Yield a :class:`StreamResponse` for streaming the response body (D-P3-06/D-P3-07, Backend-agnostic streaming response wrapper (D-P3-07).      Iterable for byte ch, Release the underlying httpx response., StreamResponse

### Community 45 - "Pandas & Format Readers"
Cohesion: 0.23
Nodes (11): get_reader(), Return a format reader instance for *format_name*.      Raises:         KeyError, dataset_to_dataframes(), Any, DataFrame, Pandas integration: load resources into DataFrames.  Requires ``pandas``: instal, Read a :class:`Resource` into a pandas :class:`~pandas.DataFrame`.      Args:, Return a ``{resource_name: DataFrame}`` mapping for a dataset. (+3 more)

### Community 46 - "GeoJSON & XLSX Readers"
Cohesion: 0.24
Nodes (8): FormatError, Raised when a resource cannot be parsed in the expected format., GeoJSONReader, Any, Path, Read GeoJSON FeatureCollections into a list of feature dictionaries., Any, Path

### Community 47 - "JSON Format Reader"
Cohesion: 0.25
Nodes (8): JSONReader, Any, Path, Read JSON or JSONL files into a list of dictionaries., Unit tests for format readers., test_json_reader_array(), test_json_reader_empty(), test_json_reader_jsonl()

### Community 48 - "Download CLI Tests"
Cohesion: 0.35
Nodes (8): _make_dataset(), _patch_client(), MonkeyPatch, Path, Unit tests for the ``datasluice download`` command., _RecordingDownloader, test_download_format_filtering(), test_download_no_matching_resources_exits_with_error()

### Community 49 - "Brand Logo Assets"
Cohesion: 0.29
Nodes (10): DataSluice Logo (Brand Mark), Datasluice Brand Logo (no background), Circuit Pathways Connectivity Element, Code Brackets (</>) Programming Icon, Funnel Processing Element, Filtered Output Basin, Padlock Security Icon, Table/Grid Tech Icon (+2 more)

### Community 50 - "Detect CLI Command"
Cohesion: 0.22
Nodes (9): detect(), ``datasluice detect`` command., Auto-detect the platform type of an open-data portal., detect_portal(), detect_portal_type(), _normalize_base_url(), Ensure *url* has a scheme and no trailing slash., Auto-detect the platform type for *base_url*.      Probes common API endpoints a (+1 more)

### Community 51 - "HTTPX Transport Client"
Cohesion: 0.24
Nodes (6): _host_credential_provider_type(), Any, Perform an HTTP request and return the raw response body.          Raises:, GET *url* and return the response as parsed JSON (non-dicts wrapped under ``"dat, GET *url* and return the raw bytes (for file downloads)., Lazily resolve ``HostCredentialProvider`` (plan 03-04), returning ``None`` if it

### Community 52 - "Transport Protocol Tests"
Cohesion: 0.20
Nodes (9): Protocol-level tests for the Transport and StreamingTransport ports.  These comp, Transport must keep its runtime-checkable flag (carry-forward)., StreamingTransport must be a @runtime_checkable Protocol (INFRA-07)., StreamingTransport Protocol surface must declare ``stream``., HttpxTransport must satisfy Transport AND StreamingTransport (INFRA-01).      Th, test_httpx_transport_satisfies_both_protocols(), test_streaming_transport_protocol_declares_stream(), test_streaming_transport_protocol_is_runtime_checkable() (+1 more)

### Community 53 - "CSV Format Reader"
Cohesion: 0.28
Nodes (6): CSVReader, Any, Path, Read CSV files into a list of dictionaries.      Args:         encoding: File en, test_csv_reader(), test_csv_reader_bytes()

### Community 54 - "Abstract Storage Backend"
Cohesion: 0.22
Nodes (6): ABC, Abstract storage backend., Persist *data* under *key* and return the storage URI/path., Read and return the bytes stored under *key*., Return ``True`` if *key* exists in storage., Storage

### Community 55 - "Airflow Operator"
Cohesion: 0.32
Nodes (6): BaseOperator, DataSluiceOperator, _import_operator(), Any, Import the Airflow BaseOperator lazily., Factory that returns an Airflow ``BaseOperator`` subclass.      Usage::

### Community 56 - "Socrata Factory & Context"
Cohesion: 0.29
Nodes (6): create_socrata_connector(), Factory for the Socrata connector (entry-point target).  ``create_socrata_connec, Construct a :class:`SocrataAdapter` wired to the context's transport/auth., ConnectorContext, Context passed to ``create_*_connector(ctx)`` factory functions.      Carries th, Resolve and construct a connector for *url*.          Auto-detects the portal ty

### Community 57 - "Downloader Class"
Cohesion: 0.32
Nodes (5): Downloader, Path, Download multiple *resources* into *dest*., Downloads resources to local or pluggable storage.      Args:         transport:, Download a single *resource* and return the local file path.          Args:

### Community 58 - "Storage Port Protocol"
Cohesion: 0.25
Nodes (4): Protocol, Storage port Protocol returning URI references (CORR-05)., Boundary protocol for byte storage addressed by path/URI strings., StoragePort

### Community 59 - "Custom Adapter Skeleton"
Cohesion: 0.29
Nodes (3): CustomAdapter, Skeleton adapter — override each method for your portal., Custom connector subpackage.  Provides a template for implementing connectors fo

### Community 60 - "Pre-commit Tooling"
Cohesion: 0.50
Nodes (5): pre-commit-hooks (basic hooks), pytest testing framework, Ruff linter and formatter, ty type checker (Astral), Pre-commit Configuration

### Community 61 - "Parquet Reader"
Cohesion: 0.40
Nodes (4): ParquetReader, Any, Path, Read Parquet files into a list of dictionaries.      Requires ``pyarrow``: insta

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

### Community 69 - "Format Reader Base"
Cohesion: 0.50
Nodes (3): Any, Path, Read *source* and return a list of record dictionaries.          Args:

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

## Knowledge Gaps
- **25 isolated node(s):** `datasluice`, `$schema`, `bootstrap-sha`, `packages`, `Funding Config (Buy Me a Coffee: nitishraj)` (+20 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **12 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Resource` connect `Resource Mapping` to `DataGouv Mapper`, `Base Adapter & Connectors`, `Access & Detection Models`, `DataGouv Adapter`, `Pandas & Format Readers`, `Dataset & License Models`, `Download CLI Tests`, `CKAN Mapper`, `Socrata Adapter`, `Downloader Class`, `Storage & Downloaders`, `Custom Adapter Skeleton`, `CKAN Adapter & Factory`?**
  _High betweenness centrality (0.085) - this node is a cross-community bridge._
- **Why does `DownloadError` connect `Storage & Downloaders` to `Discovery & Airflow`, `Host Credential Provider`, `Content Cache`, `Local Storage`, `Access & Detection Models`, `Checksums & Errors`, `Abstract Storage Backend`, `Downloader Class`, `Fsspec Storage`?**
  _High betweenness centrality (0.054) - this node is a cross-community bridge._
- **Why does `DataSluiceSession` connect `CLI Search & Runtime` to `Discovery & Airflow`, `Authentication Strategies`, `Plugin Manager`, `Base Adapter & Connectors`, `Access & Detection Models`, `Pandas & Format Readers`, `Cache Port Protocol`, `Socrata Factory & Context`, `CLI App Core`?**
  _High betweenness centrality (0.046) - this node is a cross-community bridge._
- **Are the 6 inferred relationships involving `DataSluiceSession` (e.g. with `ConnectorContext` and `PluginManager`) actually correct?**
  _`DataSluiceSession` has 6 INFERRED edges - model-reasoned connections that need verification._
- **Are the 4 inferred relationships involving `Resource` (e.g. with `Dataset` and `License`) actually correct?**
  _`Resource` has 4 INFERRED edges - model-reasoned connections that need verification._
- **Are the 6 inferred relationships involving `BaseAuth` (e.g. with `APIKeyAuth` and `BasicAuth`) actually correct?**
  _`BaseAuth` has 6 INFERRED edges - model-reasoned connections that need verification._
- **Are the 16 inferred relationships involving `HttpClient` (e.g. with `PortalError` and `RateLimitError`) actually correct?**
  _`HttpClient` has 16 INFERRED edges - model-reasoned connections that need verification._
