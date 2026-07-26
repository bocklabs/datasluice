# Graph Report - datasluice  (2026-07-26)

## Corpus Check
- 109 files · ~17,793 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1388 nodes · 2620 edges · 85 communities (71 shown, 14 thin omitted)
- Extraction: 91% EXTRACTED · 9% INFERRED · 0% AMBIGUOUS · INFERRED: 236 edges (avg confidence: 0.73)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `f5d8900a`
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
- Download CLI Tests
- Brand Logo Assets
- Detect CLI Command
- HTTPX Transport Client
- Transport Protocol Tests
- Abstract Storage Backend
- Socrata Factory & Context
- Custom Adapter Skeleton
- Pre-commit Tooling
- Test Fixtures Config
- Dead Settings Tests
- Package API Tests
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
- ._send_with_redirects
- Artifact
- CatalogCapabilities
- Schema
- _ScriptableHandler
- Any
- Protocol

## God Nodes (most connected - your core abstractions)
1. `DataSluiceSession` - 38 edges
2. `ContentCache` - 36 edges
3. `Resource` - 35 edges
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
- `test_http_download_defaults()` --calls--> `HttpDownload`  [INFERRED]
  tests/unit/domain/test_new_models.py → src/datasluice/domain/access.py
- `test_resource_access_is_frozen()` --calls--> `HttpDownload`  [INFERRED]
  tests/unit/domain/test_new_models.py → src/datasluice/domain/access.py

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

## Communities (85 total, 14 thin omitted)

### Community 0 - "HTTP Transport Layer"
Cohesion: 0.12
Nodes (37): HttpxTransport, HTTP transport backed by httpx, satisfying Transport + StreamingTransport., _CapturingServer, MockResponse, Reusable local HTTP test server for transport and integration tests.  Spawns a :, Configurable HTTP response.      Attributes:         status: HTTP status code., ThreadingHTTPServer that records received requests and exposes the script., Start a scriptable test HTTP server on an ephemeral port.      Args:         res (+29 more)

### Community 1 - "Content Cache"
Cohesion: 0.06
Nodes (50): Connection, ContentCache, Any, Content-addressed cache backed by a SQLite WAL index + content files (INFRA-03)., Return the SHA-256 hexdigest of *key* (SEC-06 carry-forward, D-P3-09)., Return the absolute content-file path for a SHA-256 digest., Return cached bytes for *key*, or ``None`` on miss / expiry / writing., Store *data* under *key* (CachePort signature UNCHANGED, D-P3-12). (+42 more)

### Community 2 - "CLI Search & Runtime"
Cohesion: 0.09
Nodes (20): Any, Tests for DataSluiceSession Phase 3 kwargs + injectables (Success Criterion 5)., An injected StoragePort is stored on the session (D-P3-20)., ConnectorContext fields stay exactly (base_url, transport, auth, page_size) (D-P, The session stays portal()/search()-only (D-P3-22)., User-defined transport stub satisfying the Transport Protocol structurally., Stub satisfying the StoragePort Protocol., Stub satisfying the CredentialProvider Protocol. (+12 more)

### Community 3 - "Authentication Strategies"
Cohesion: 0.25
Nodes (8): CatalogPort, OrganizationCatalog, Protocol, Catalog port Protocols for portal catalog connectors., Marker base protocol all catalog connectors share.      Attributes:         port, Capability protocol for dataset search., Capability protocol for organization lookup., SearchableCatalog

### Community 4 - "Bearer Auth & Credentials"
Cohesion: 0.08
Nodes (32): Lock, Refresher, BearerAuth, Any, Authenticate requests using a bearer token in the ``Authorization`` header., HostCredentialProvider, Drop the cached credential for *host* (off-port; D-P3-15).          Called by :c, Resolve a :class:`BaseAuth` per host with cached expiry and single-flight refres (+24 more)

### Community 5 - "Base Adapter & Connectors"
Cohesion: 0.11
Nodes (19): Socrata adapter implementation.  Communicates with the Socrata Discovery API and, map_dataset(), map_organization(), map_resource(), Any, Mapping functions to convert Socrata-native JSON into domain models., Convert a Socrata view/resource dict into a :class:`Resource`., Convert a Socrata customer/owner dict into an :class:`Organization`. (+11 more)

### Community 6 - "Project Docs & Contributing"
Cohesion: 0.10
Nodes (28): Conventional Commits, DataSluice (Public API class), datasluice_source (dlt source factory), DataSluiceOperator (Airflow operator), Discovery Layer (datasluice.discovery), Airflow Integration Example, CKAN Example, data.gouv.fr Example (+20 more)

### Community 7 - "API Key Auth"
Cohesion: 0.08
Nodes (20): APIKeyAuth, Any, Authenticate requests using an API key.      Args:         api_key: The API key, BasicAuth, Any, Authenticate requests using HTTP Basic credentials.      Args:         username:, HeadersAuth, Any (+12 more)

### Community 8 - "Resource Mapping"
Cohesion: 0.15
Nodes (13): Resource model representing a single downloadable file within a dataset., dataset_to_dataframes(), Any, DataFrame, Pandas integration: load resources into DataFrames.  Requires ``pandas``: instal, Read a :class:`Resource` into a pandas :class:`~pandas.DataFrame`.      Args:, Return a ``{resource_name: DataFrame}`` mapping for a dataset., resource_to_dataframe() (+5 more)

### Community 9 - "Access & Detection Models"
Cohesion: 0.23
Nodes (16): HttpDownload, LocalFile, ObjectStorage, QueryAccess, ResourceAccess sum-type describing how a resource is reached., Base descriptor for how a resource is accessed.      Subclasses discriminate on, Resource fetched over HTTP(S).      Attributes:         url: Absolute URL to dow, Resource stored in object storage (S3, GCS, Azure Blob).      Attributes: (+8 more)

### Community 10 - "Artifact & Catalog Models"
Cohesion: 0.22
Nodes (8): DetectionEvidence, Detection models for evidence-based portal identification., A single piece of evidence produced by a detection check.      Attributes:, Unit tests for the six new frozen domain models added in Phase 2., test_detection_evidence_is_frozen(), test_detection_result_accepts_none_portal_type(), test_http_download_defaults(), test_resource_access_is_frozen()

### Community 11 - "DataGouv Adapter"
Cohesion: 0.09
Nodes (24): DataGouvAdapter, data.gouv.fr (udata) adapter implementation.  Communicates with the udata REST A, Adapter for data.gouv.fr and other udata-powered portals.      Uses the udata RE, Call a udata API endpoint and return the parsed JSON., Search datasets via ``/datasets/``., Fetch a dataset via ``/datasets/{id}/``., Return resources for *dataset_id*., Fetch organization metadata via ``/organizations/{slug}/``. (+16 more)

### Community 12 - "Credentials & Redirect"
Cohesion: 0.13
Nodes (22): HTTPMessage, CredentialScope, Credential scoping model for host-bound credential policy., Host-scoped policy controlling where credentials may be sent.      Attributes:, CredentialAwareRedirectHandler, Request, Redirect handling that strips credentials on cross-origin or scheme-downgrade re, Strip sensitive headers when a redirect crosses origins or downgrades to plain H (+14 more)

### Community 13 - "Transport Retry & Pagination"
Cohesion: 0.07
Nodes (38): Raised on HTTP 5xx responses that should be retried., RetryableHTTPError, _parse_retry_after(), Parse a ``Retry-After`` header into a delay in seconds.      Supports both delta, __getattr__(), Transport layer: HTTP client, retry, rate-limiting, and pagination.  ``HttpxTran, Lazily resolve httpx-backed symbols on first attribute access (PEP 562)., paginate() (+30 more)

### Community 14 - "Dataset & License Models"
Cohesion: 0.13
Nodes (14): Dataset, Dataset model representing a collection of related open-data resources., A dataset is a logical grouping of one or more resources.      Attributes:, License, License model representing the license under which data is published., A license under which an open-data resource or dataset is published.      Attrib, Organization model representing a dataset publisher., Result container for paginated search responses. (+6 more)

### Community 15 - "File Cache"
Cohesion: 0.14
Nodes (17): FileCache, Path, Simple file-based cache for downloaded resources., A time-based file cache.      Args:         cache_dir: Directory to store cached, Return cached bytes for *key*, or ``None`` if missing/expired., Store *data* under *key* and return the cache file path., Return ``True`` if *key* is cached and not expired., Remove all entries from the cache. (+9 more)

### Community 16 - "Cache Port Protocol"
Cohesion: 0.14
Nodes (12): HTTP client with retry, rate-limiting, and authentication support., Render *body* as text, truncating to *limit* characters., _truncate_body(), httpx-backed HTTP transport satisfying the Transport + StreamingTransport Protoc, RateLimiter, Rate-limiting to stay within portal request quotas., Thread-safe token-bucket rate limiter.      Args:         requests_per_second: M, Block until the next request is permitted. (+4 more)

### Community 17 - "Rate Limit & Config"
Cohesion: 0.10
Nodes (18): API-key authentication strategy.  Supports passing the key via a header (default, BaseAuth, ABC, Any, Abstract base for authentication strategies., Protocol for pluggable authentication.      Each strategy knows how to decorate, Return a copy of *headers* (and optionally *params*) with credentials applied., HTTP Basic authentication strategy. (+10 more)

### Community 18 - "Architecture Docs"
Cohesion: 0.11
Nodes (17): Adapter isolation principle, Adapter 4-module pattern (adapter/mapper/pagination/errors), Adapters Layer (datasluice.adapters), Auth Layer (datasluice.auth), BaseAdapter protocol, CKAN adapter, CKAN portal platform, Composable transport (decorators) principle (+9 more)

### Community 19 - "CKAN Mapper"
Cohesion: 0.12
Nodes (19): CKAN adapter implementation.  Communicates with the CKAN Action API (``/api/3/ac, map_dataset(), map_license(), map_organization(), map_resource(), Any, Mapping functions to convert CKAN-native JSON into domain models., Convert a CKAN license dict into a :class:`License`. (+11 more)

### Community 20 - "Filesystem Layer"
Cohesion: 0.06
Nodes (41): AbstractFileSystem, inspect(), ``datasluice inspect`` command., Inspect a single dataset in detail., open_filesystem(), Any, Centralised filesystem factory (INFRA-05).  All fsspec backend instantiation flo, Instantiate the correct fsspec backend for *uri* (INFRA-05, D-P3-20).      Deleg (+33 more)

### Community 21 - "Socrata Adapter"
Cohesion: 0.08
Nodes (22): CKANAdapter, Adapter for CKAN-powered open-data portals.      Uses the CKAN Action API at ``{, Call a CKAN Action API endpoint and return the ``result`` dict., Search datasets via ``package_search``., Fetch a dataset via ``package_show``., Return resources for *dataset_id*., Fetch organization metadata via ``organization_show``., CKAN portal adapter.  CKAN is the world's most widely deployed open-data platfor (+14 more)

### Community 22 - "Checksums & Errors"
Cohesion: 0.28
Nodes (11): compute_hash(), compute_md5(), compute_sha256(), Path, Checksum (hash) computation and verification., Compute the hex digest of *file_path* using *algorithm*., Return the SHA-256 hex digest of *file_path*., Return the MD5 hex digest of *file_path*. (+3 more)

### Community 23 - "Logging & Redaction"
Cohesion: 0.16
Nodes (19): configure_logging(), Any, LogRecord, Redact known sensitive keys from log records (INFRA-06, D-P3-18).      Walks ``r, Configure the package-level logger.      Args:         level: Logging level (e.g, RedactingFilter, _make_record(), LogRecord (+11 more)

### Community 24 - "CI/CD Workflows"
Cohesion: 0.16
Nodes (19): Conventional Commits Release Pipeline (release-please -> publish), uv package manager (used across all workflows), Dependabot Config (github-actions + pip weekly), CI Workflow, CI All checks pass gate (alls-green), CI Build & validate package job (uv build + twine), CI Coverage job (combine + report), CI Lint & format job (ruff) (+11 more)

### Community 25 - "Format Readers Base"
Cohesion: 0.05
Nodes (47): IO, DecompressionError, FormatError, Raised when a resource cannot be parsed in the expected format., Raised when a compressed resource cannot be decompressed (D-P4-21).      Compres, BaseFormatReader, ABC, Any (+39 more)

### Community 26 - "Storage & Downloaders"
Cohesion: 0.32
Nodes (6): BaseOperator, DataSluiceOperator, _import_operator(), Any, Import the Airflow BaseOperator lazily., Factory that returns an Airflow ``BaseOperator`` subclass.      Usage::

### Community 27 - "DuckDB Integration"
Cohesion: 0.17
Nodes (17): Any, query_resource(), DuckDB integration: query resources directly with DuckDB.  Requires ``duckdb``:, Validate that *table_name* is a safe SQL identifier.      Args:         table_na, Register a remote resource as a DuckDB relation.      The URL flows through Duck, Run arbitrary *sql* against a resource and return the result.      Warning:, resource_to_relation(), _validate_table_name() (+9 more)

### Community 28 - "CLI App Core"
Cohesion: 0.25
Nodes (7): Argument, help, Option, download(), Path, ``datasluice download`` command., Download all resources from a dataset.

### Community 29 - "CKAN Adapter & Factory"
Cohesion: 0.11
Nodes (19): Lazily initialised HTTP transport., HttpClient, Any, Perform an HTTP request and return the raw response body.          Raises:, GET *url* and return the response as text., GET *url* and return the response as parsed JSON., GET *url* and return the raw bytes (for file downloads)., Thin HTTP client wrapping :mod:`urllib` with auth, retry, and rate-limiting. (+11 more)

### Community 30 - "Portal Error Mapping"
Cohesion: 0.08
Nodes (31): Exception, map_ckan_error(), CKAN-specific error mapping., Map a CKAN HTTP error response to a DataSluice exception.      Args:         sta, map_datagouv_error(), data.gouv.fr (udata) specific error mapping., Map a udata HTTP error response to a DataSluice exception., map_socrata_error() (+23 more)

### Community 31 - "Fsspec Storage"
Cohesion: 0.16
Nodes (9): FsspecStorage, Any, fsspec-backed storage adapter satisfying :class:`StoragePort` (INFRA-02, CORR-05, Adapter wrapping an fsspec ``AbstractFileSystem`` to satisfy ``StoragePort`` (CO, Persist *data* under *path* and return the resulting URI string.          Args:, Read and return the bytes stored under *path*., Return ``True`` if *path* exists in this filesystem., Return the backend-native path for *path*.          Absolute URIs pass through u (+1 more)

### Community 32 - "Discovery & Airflow"
Cohesion: 0.12
Nodes (19): NoAuth, Any, Authentication strategy that adds no credentials.      Suitable for portals that, Default configuration constants., Configuration for DataSluice., create_default_transport(), Default transport factory for the DataSluiceSession composition root.  Replaces, Construct a default transport from ``DEFAULT_*`` constants.      Picks :class:`H (+11 more)

### Community 34 - "DataGouv Mapper"
Cohesion: 0.05
Nodes (28): Protocol, RawIOBase, Schema, BatchStream, Any, BatchStream — context-managed Arrow RecordBatch stream (DATA-01, DATA-02, D-P4-1, Context-managed Arrow RecordBatch stream.      Wraps a ``pa.RecordBatchReader``, The pa.Schema for batches yielded by this stream. (+20 more)

### Community 35 - "Plugin Manager"
Cohesion: 0.10
Nodes (20): AdapterNotFoundError, Raised when no adapter is registered for a portal type., PluginFailure, PluginManager, Plugin manager for entry-point-based connector discovery.  The :class:`PluginMan, Record of a failed plugin discovery or load.      Attributes:         name: Entr, Registry-free connector manager backed by ``importlib.metadata``.      Built-in, Register *factory* programmatically (used by tests, D-06). (+12 more)

### Community 37 - "Transport Port"
Cohesion: 0.11
Nodes (13): AbstractContextManager, Port Protocol interfaces for DataSluice — unstable boundary contracts., Protocol, Storage port Protocol returning URI references (CORR-05)., Boundary protocol for byte storage addressed by path/URI strings., StoragePort, Any, Protocol (+5 more)

### Community 38 - "Fsspec Storage Tests"
Cohesion: 0.15
Nodes (11): main(), Main Typer application for the DataSluice CLI., DataSluice — unified open-data toolkit., Command-line interface for DataSluice., ``datasluice search`` command., Search for datasets on an open-data portal., search(), Query (+3 more)

### Community 39 - "Local Storage"
Cohesion: 0.23
Nodes (10): LocalStorage, Path, Local-filesystem storage backend.      Args:         base_dir: Root directory fo, Path, Unit tests for LocalStorage path-traversal containment., test_storage_rejects_dotdot_segment(), test_storage_rejects_path_traversal(), test_storage_round_trip() (+2 more)

### Community 40 - "Portal Detection Fingerprints"
Cohesion: 0.10
Nodes (19): detect(), ``datasluice detect`` command., Auto-detect the platform type of an open-data portal., detect_portal(), detect_portal_type(), _normalize_base_url(), Auto-detection of portal type from a URL.  The detector probes well-known API en, Ensure *url* has a scheme and no trailing slash. (+11 more)

### Community 41 - "Adapter Exceptions"
Cohesion: 0.20
Nodes (5): CachePort, Protocol, Cache port Protocol for content-addressed byte caching., Boundary protocol for a simple key/byte cache., Lazily construct the default ContentCache (plan 03-03) if importable.          R

### Community 42 - "Retry & Backoff"
Cohesion: 0.24
Nodes (11): Sanitise *name* for use as a filename., safe_filename(), Path, Unit tests for IO utilities (checksums, cache, local, storage)., test_compute_sha256(), test_ensure_dir(), test_file_cache(), test_local_storage() (+3 more)

### Community 43 - "Sync State Model"
Cohesion: 0.16
Nodes (9): SyncState model for incremental sync cursors and watermarks (SYNC-03)., Incremental synchronization state for a resource or connector.      Attributes:, SyncState, Protocol, State store port Protocol for incremental sync state (SYNC-01)., Boundary protocol for persisting incremental sync state., StateStore, test_sync_state_defaults() (+1 more)

### Community 44 - "HTTP Retry Errors"
Cohesion: 0.16
Nodes (11): datetime, Logger, Host-scoped credential resolver with single-flight refresh (INFRA-04).  ``HostCr, Apache Airflow integration: DataSluice operators for DAGs.  Requires ``apache-ai, datasluice_source(), Any, dlt (data load tool) integration: use DataSluice as a dlt source.  Requires ``dl, Return a dlt source that yields datasets from *portal*.      Args:         porta (+3 more)

### Community 48 - "Download CLI Tests"
Cohesion: 0.35
Nodes (8): _make_dataset(), _patch_client(), MonkeyPatch, Path, Unit tests for the ``datasluice download`` command., _RecordingDownloader, test_download_format_filtering(), test_download_no_matching_resources_exits_with_error()

### Community 49 - "Brand Logo Assets"
Cohesion: 0.29
Nodes (10): DataSluice Logo (Brand Mark), Datasluice Brand Logo (no background), Circuit Pathways Connectivity Element, Code Brackets (</>) Programming Icon, Funnel Processing Element, Filtered Output Basin, Padlock Security Icon, Table/Grid Tech Icon (+2 more)

### Community 50 - "Detect CLI Command"
Cohesion: 0.22
Nodes (8): DetectionResult, Outcome of portal auto-detection with confidence and evidence.      Attributes:, PortalDetector, Protocol, Portal detector port Protocol., Detection seam protocol returning evidence-based portal identification., test_detection_result_defaults(), test_detection_result_is_frozen()

### Community 51 - "HTTPX Transport Client"
Cohesion: 0.28
Nodes (6): _host_credential_provider_type(), Any, Perform an HTTP request and return the raw response body.          Raises:, GET *url* and return the response as parsed JSON (non-dicts wrapped under ``"dat, GET *url* and return the raw bytes (for file downloads)., Lazily resolve ``HostCredentialProvider`` (plan 03-04), returning ``None`` if it

### Community 52 - "Transport Protocol Tests"
Cohesion: 0.20
Nodes (9): Protocol-level tests for the Transport and StreamingTransport ports.  These comp, Transport must keep its runtime-checkable flag (carry-forward)., StreamingTransport must be a @runtime_checkable Protocol (INFRA-07)., StreamingTransport Protocol surface must declare ``stream``., HttpxTransport must satisfy Transport AND StreamingTransport (INFRA-01).      Th, test_httpx_transport_satisfies_both_protocols(), test_streaming_transport_protocol_declares_stream(), test_streaming_transport_protocol_is_runtime_checkable() (+1 more)

### Community 54 - "Abstract Storage Backend"
Cohesion: 0.10
Nodes (25): ChecksumMismatchError, DownloadError, Raised when a resource download fails., Raised when a downloaded file's checksum does not match., Raised when a resource's access kind has no reader implementation (D-P4-09)., UnsupportedAccessError, Downloader, Path (+17 more)

### Community 56 - "Socrata Factory & Context"
Cohesion: 0.12
Nodes (15): create_ckan_connector(), Factory for the CKAN connector (entry-point target).  ``create_ckan_connector``, Construct a :class:`CKANAdapter` wired to the context's transport/auth., create_datagouv_connector(), Factory for the data.gouv.fr connector (entry-point target).  ``create_datagouv_, Construct a :class:`DataGouvAdapter` wired to the context's transport/auth., create_socrata_connector(), Factory for the Socrata connector (entry-point target).  ``create_socrata_connec (+7 more)

### Community 59 - "Custom Adapter Skeleton"
Cohesion: 0.09
Nodes (16): BaseAdapter, ABC, Abstract base class for all portal adapters., Protocol that every portal adapter must implement.      Subclasses translate por, Search for datasets matching *query*., Fetch a single dataset by its portal-native *dataset_id*., Return all downloadable resources for *dataset_id*., Fetch publisher metadata for *organization_id*. (+8 more)

### Community 60 - "Pre-commit Tooling"
Cohesion: 0.50
Nodes (5): pre-commit-hooks (basic hooks), pytest testing framework, Ruff linter and formatter, ty type checker (Astral), Pre-commit Configuration

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
Nodes (4): An injected CachePort is stored as the session cache (D-P3-02)., Stub satisfying the CachePort Protocol., _StubCache, test_cache_injectable()

### Community 89 - "._send_with_redirects"
Cohesion: 0.33
Nodes (3): Response, Return whether sensitive headers must be stripped on this redirect hop., Drive the manual redirect loop, stripping sensitive headers per hop (Pattern 1).

### Community 90 - "Artifact"
Cohesion: 0.33
Nodes (5): Artifact, Artifact model — the materialized output reference (INTG-10)., A materialized artifact produced by the data plane.      Attributes:         uri, test_artifact_defaults(), test_artifact_is_frozen()

### Community 91 - "CatalogCapabilities"
Cohesion: 0.33
Nodes (5): CatalogCapabilities, CatalogCapabilities model — query-field-level capability contract (D-07)., Capabilities a catalog connector advertises to the runtime.      Phase 5's rejec, test_catalog_capabilities_defaults(), test_catalog_capabilities_is_frozen()

### Community 92 - "Schema"
Cohesion: 0.33
Nodes (5): Schema model describing the column-level shape of a resource., Schema describing the columns of a tabular resource.      Attributes:         na, Schema, test_schema_defaults(), test_schema_is_frozen()

### Community 93 - "_ScriptableHandler"
Cohesion: 0.40
Nodes (3): BaseHTTPRequestHandler, Any, _ScriptableHandler

## Knowledge Gaps
- **25 isolated node(s):** `datasluice`, `$schema`, `bootstrap-sha`, `packages`, `Funding Config (Buy Me a Coffee: nitishraj)` (+20 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **14 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Resource` connect `Base Adapter & Connectors` to `DataGouv Mapper`, `Resource Mapping`, `Access & Detection Models`, `DataGouv Adapter`, `Dataset & License Models`, `Download CLI Tests`, `CKAN Mapper`, `Socrata Adapter`, `Abstract Storage Backend`, `Custom Adapter Skeleton`?**
  _High betweenness centrality (0.069) - this node is a cross-community bridge._
- **Why does `DataSluiceSession` connect `Discovery & Airflow` to `CLI Search & Runtime`, `Plugin Manager`, `Fsspec Storage Tests`, `Resource Mapping`, `Access & Detection Models`, `Adapter Exceptions`, `HTTP Retry Errors`, `Auth Apply Method`, `Rate Limit & Config`, `Filesystem Layer`, `Socrata Factory & Context`, `CLI App Core`?**
  _High betweenness centrality (0.064) - this node is a cross-community bridge._
- **Why does `HttpClient` connect `CKAN Adapter & Factory` to `Discovery & Airflow`, `HTTP Transport Layer`, `Portal Detection Fingerprints`, `Credentials & Redirect`, `Transport Retry & Pagination`, `Cache Port Protocol`, `Abstract Storage Backend`, `Custom Adapter Skeleton`, `Portal Error Mapping`?**
  _High betweenness centrality (0.046) - this node is a cross-community bridge._
- **Are the 6 inferred relationships involving `DataSluiceSession` (e.g. with `ConnectorContext` and `PluginManager`) actually correct?**
  _`DataSluiceSession` has 6 INFERRED edges - model-reasoned connections that need verification._
- **Are the 3 inferred relationships involving `Resource` (e.g. with `Dataset` and `Downloader`) actually correct?**
  _`Resource` has 3 INFERRED edges - model-reasoned connections that need verification._
- **Are the 6 inferred relationships involving `BaseAuth` (e.g. with `APIKeyAuth` and `BasicAuth`) actually correct?**
  _`BaseAuth` has 6 INFERRED edges - model-reasoned connections that need verification._
- **Are the 16 inferred relationships involving `HttpClient` (e.g. with `PortalError` and `RateLimitError`) actually correct?**
  _`HttpClient` has 16 INFERRED edges - model-reasoned connections that need verification._
