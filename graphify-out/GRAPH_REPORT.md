# Graph Report - datasluice  (2026-08-02)

## Corpus Check
- 130 files · ~42,466 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1565 nodes · 3043 edges · 106 communities (83 shown, 23 thin omitted)
- Extraction: 95% EXTRACTED · 5% INFERRED · 0% AMBIGUOUS · INFERRED: 145 edges (avg confidence: 0.64)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `435c96f4`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- sync/state_store.py
- ContentCache
- TransformContext
- sync.py
- HostCredentialProvider
- data/access.py
- DataSluice (Public API class)
- Any
- SyncState
- LegacyArtifactRecord
- discovery/detector.py
- to_arrow
- _identity.py
- runtime/defaults.py
- artifact.py
- SocrataAdapter
- Resource
- compression.py
- _ApplicationServices
- detect
- application.py
- datagouv/mapper.py
- checks.py
- BaseAuth
- CI Workflow
- load_fixture
- .search
- BatchStream
- .request
- DataSluiceError
- HttpxTransport
- FsspecStorage
- PluginManager
- auth/__init__.py
- DataSluice
- DataSluiceSession
- RedactingFilter
- downloader.py
- CachePort
- ChecksumMismatchError
- BaseOperator
- BasicAuth
- Transport
- search
- transport.py
- io/storage.py
- context.py
- http_client.py
- inspect
- Datasluice Brand Logo (no background)
- _hashing.py
- data/__init__.py
- .__init__
- HeadersAuth
- FileCache
- DataSluiceError
- PortalError
- geojson.py
- DataPlaneResourceReader
- domain/__init__.py
- Pre-commit Configuration
- exceptions.py
- ParquetReader
- download
- FormatError
- SECURITY.md
- OpenCodeReview PR Review Workflow (alibaba open-code-review)
- Formats Layer (datasluice.formats)
- duckdb.py
- Path
- ports/detector.py
- StoragePort
- .read_batches
- Defense-in-depth CI security scanning pattern
- Datasluice Brand Identity
- Pull Request Template (affected areas + AI provenance)
- .read_batches
- connectors/__init__.py
- integrations/__init__.py
- .download
- Funding Config (Buy Me a Coffee: nitishraj)
- Issue Template Config (blank issues enabled)
- _byte_source.py
- DataSluiceSession
- config/defaults.py
- Any
- SyncState
- BearerAuth
- CKANAdapter
- materialize
- .read_batches
- DataGouvAdapter
- DetectionResult
- Query
- SearchResult
- APIKeyAuth
- BaseFormatReader
- Any
- SyncState
- Resource
- Any
- .apply
- Any
- IterableBytesIO
- transport/pagination.py
- ports/__init__.py

## God Nodes (most connected - your core abstractions)
1. `BatchStream` - 40 edges
2. `Resource` - 39 edges
3. `DataSluiceError` - 31 edges
4. `BaseAuth` - 27 edges
5. `FormatError` - 25 edges
6. `BaseAdapter` - 25 edges
7. `OpenedResource` - 24 edges
8. `Dataset` - 23 edges
9. `DataPlaneResourceReader` - 20 edges
10. `TransformContext` - 20 edges

## Surprising Connections (you probably didn't know these)
- `DataSluice Logo (Brand Mark)` --semantically_similar_to--> `Datasluice Brand Logo (no background)`  [INFERRED] [semantically similar]
  docs/assets/datasluice-logo.png → docs/assets/datasluice-logo-nbg.png
- `detect()` --calls--> `DataSluice`  [INFERRED]
  src/datasluice/cli/detect.py → src/datasluice/application.py
- `download()` --calls--> `DataSluice`  [INFERRED]
  src/datasluice/cli/download.py → src/datasluice/application.py
- `DataPlaneResourceReader` --uses--> `DataSluiceError`  [INFERRED]
  src/datasluice/data/access.py → src/datasluice/exceptions.py
- `_StreamClosingBytesIO` --uses--> `DataSluiceError`  [INFERRED]
  src/datasluice/data/access.py → src/datasluice/exceptions.py

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

## Communities (106 total, 23 thin omitted)

### Community 0 - "sync/state_store.py"
Cohesion: 0.06
Nodes (48): Any, RLock, datasluice_source(), dlt (data load tool) integration: use DataSluice as a dlt source. Requires…, Return a deterministic destination-safe name for a resource ID., Return a dlt source yielding one Arrow-backed table per resource. Args: portal:…, _sanitize(), _contains_secret_material() (+40 more)

### Community 1 - "ContentCache"
Cohesion: 0.13
Nodes (15): Connection, ContentCache, Any, Return the SHA-256 hexdigest of *key* (SEC-06 carry-forward, D-P3-09)., Return the absolute content-file path for a SHA-256 digest., Return cached bytes for *key*, or ``None`` on miss / expiry / writing., Store *data* under *key* (CachePort signature UNCHANGED, D-P3-12)., Store *data* under *key* with ETag/Last-Modified sidecar (D-P3-12). Phase 4's… (+7 more)

### Community 2 - "TransformContext"
Cohesion: 0.06
Nodes (43): Raised when a transform step cannot be applied (D-P6-15). Transform failures…, TransformError, __getattr__(), Composable transform pipeline package (TRANS-01..09). Re-exports are resolved…, Lazily export transform symbols (mirrors datasluice.data.__getattr__).…, _build_batch_stream(), _chain(), compose() (+35 more)

### Community 3 - "sync.py"
Cohesion: 0.06
Nodes (72): AbstractFileSystem, Artifact, DataSluiceError, datetime, LegacyArtifactRecord, Content-addressed cache backed by a SQLite WAL index + content files…, open_filesystem(), Any (+64 more)

### Community 4 - "HostCredentialProvider"
Cohesion: 0.17
Nodes (9): Lock, Refresher, HostCredentialProvider, Drop the cached credential for *host* (off-port; D-P3-15). Called by…, Resolve a :class:`BaseAuth` per host with cached expiry and single-flight…, Return the per-host lock, creating it if necessary. The dict-level lock is held…, Return whether *expires_at* has passed. ``None`` means the credential never…, Return the cached :class:`BaseAuth` for *host*, refreshing if expired. Args:… (+1 more)

### Community 5 - "data/access.py"
Cohesion: 0.18
Nodes (13): _chain(), _close_source(), _content_encoding_from_headers(), Any, Concrete ``ResourceReader`` implementation with access-kind dispatch (DATA-04,…, Open an already-fetched streaming response through the data plane., Dispatch to the format reader and wrap output in BatchStream., Extract a lowercased Content-Encoding value from response headers. (+5 more)

### Community 6 - "DataSluice (Public API class)"
Cohesion: 0.05
Nodes (45): Adapter isolation principle, Adapter 4-module pattern (adapter/mapper/pagination/errors), Adapters Layer (datasluice.adapters), Auth Layer (datasluice.auth), BaseAdapter protocol, CKAN adapter, CKAN portal platform, Composable transport (decorators) principle (+37 more)

### Community 7 - "Any"
Cohesion: 0.12
Nodes (16): OpenedResource, Any, Open one resource through the injected data-plane reader., Apply a reusable transform pipeline to an existing stream., Lazy, single-use application wrapper over a Resource reader., Whether the underlying data stream is currently open., Attach one transform pipeline without opening the resource., Iterate batches once, closing every stream when iteration finishes. (+8 more)

### Community 8 - "SyncState"
Cohesion: 0.13
Nodes (12): SyncState model for incremental sync cursors and watermarks (SYNC-03)., Incremental synchronization state for a resource or connector. Attributes:…, SyncState, AtomicStateStore, Protocol, State store port Protocols for incremental sync state (SYNC-01). The base…, Boundary protocol for persisting incremental sync state., Additive capability Protocol for compare-and-swap (CAS) state writes… (+4 more)

### Community 10 - "discovery/detector.py"
Cohesion: 0.16
Nodes (15): detect(), _normalize_base_url(), Evidence-based portal type detection (D-P5-15/16/17/18). The detector probes…, Ensure *url* has a scheme and no trailing slash., Probe *url* for every registered portal fingerprint (D-P5-15/16/17/18).…, Portal type fingerprints for auto-detection. Each entry maps a fingerprint (URL…, Portal type discovery and auto-detection., PortalMetadata (+7 more)

### Community 11 - "to_arrow"
Cohesion: 0.15
Nodes (12): Any, to_arrow terminal: materialize a BatchStream into a pa.Table (INTG-01,…, Materialize *stream* into a ``pa.Table`` (INTG-01, D-P6-02). The shared…, to_arrow(), Any, to_pandas terminal: convert a BatchStream to a pandas DataFrame (INTG-02,…, Convert *stream* to a pandas ``DataFrame`` (INTG-02). Delegates through…, to_pandas() (+4 more)

### Community 12 - "_identity.py"
Cohesion: 0.24
Nodes (9): canonical_destination_identity(), canonical_identity(), Canonical resource identity (CR-01 blocker fix, SYNC-05/07). Portal-controlled…, Extract the origin scope for canonical identity hashing. HTTP(S) resources…, Return the SHA-256 canonical identity for *resource*. The identity is…, Return a secret-free SHA-256 identity for a destination URI., Reject duplicate canonical identities before any artifact or state write.…, _url_origin() (+1 more)

### Community 13 - "runtime/defaults.py"
Cohesion: 0.24
Nodes (7): CredentialProvider, Protocol, Boundary protocol resolving authentication strategies per host., create_default_transport(), Default transport factory for the DataSluiceSession composition root. Replaces…, Construct a default transport from ``DEFAULT_*`` constants. Picks…, Composition root and plugin machinery for DataSluice.

### Community 14 - "artifact.py"
Cohesion: 0.10
Nodes (23): Artifact, ArtifactProvenance, _contract_error(), Digest, _freeze_extensions(), _freeze_json(), _is_sha256(), _object_dict() (+15 more)

### Community 15 - "SocrataAdapter"
Cohesion: 0.10
Nodes (14): Fetch a dataset (view) by its 4x4 identifier., Return resources for *dataset_id*., Translate a ``Query.sort`` spec into a Socrata catalog ``order`` token. The…, Adapter for Socrata-powered open-data portals. Uses the Socrata Discovery API…, Call the Socrata Discovery API and return parsed JSON., Search datasets via the Discovery API., SocrataAdapter, _translate_sort() (+6 more)

### Community 16 - "Resource"
Cohesion: 0.17
Nodes (14): Resource, ResourceLocator, materialize(), open_resource(), Wrap one resolved resource for lazy, single-use consumption., Materialize one resource into its canonical Artifact record., Resolve one public locator to the canonical Resource model., Build one lazy opened-resource wrapper. (+6 more)

### Community 17 - "compression.py"
Cohesion: 0.10
Nodes (20): BaseException, apply_compression(), _detect_format(), _ErrorTranslatingReader, PeekableReader, Any, WriteableBuffer, Transparent decompression decorator pipeline (DATA-06, D-P4-12). Sits BETWEEN… (+12 more)

### Community 18 - "_ApplicationServices"
Cohesion: 0.16
Nodes (8): _ApplicationServices, Portal, Private coordinator for application operations over one composition substrate., Retrieve catalog metadata through the private session substrate., Raw bulk-copy multiple resources into a destination directory (D-15)., Stable application wrapper for a portal URL., Retrieve one dataset without exposing the underlying connector., Return a stable Portal wrapper for *url*.

### Community 19 - "detect"
Cohesion: 0.21
Nodes (11): detect(), _detection_json(), Any, Argument, help, Option, ``datasluice detect`` command — evidence-based portal detection (D-P5-21, D-07)., Serialize one public DetectionResult into a JSON-safe envelope. (+3 more)

### Community 20 - "application.py"
Cohesion: 0.16
Nodes (15): CatalogResourceLocator, _contract_error(), DirectResourceLocator, _locator_from_resource(), _object_dict(), Public application facade, locators, and opened-resource lifecycle., A validated, serializable catalog resource reference., Return a fresh, secret-free locator envelope. (+7 more)

### Community 21 - "datagouv/mapper.py"
Cohesion: 0.23
Nodes (14): map_dataset(), map_license(), map_organization(), map_resource(), Any, Mapping functions to convert data.gouv.fr (udata) JSON into domain models., Convert a udata license dict into a :class:`License`., Resolve the resource access descriptor per D-P5-02 for udata resources. udata… (+6 more)

### Community 22 - "checks.py"
Cohesion: 0.08
Nodes (34): BaseAdapter, ABC, Protocol that every portal adapter must implement. Subclasses translate portal-…, Search for datasets matching *query*., Fetch a single dataset by its portal-native *dataset_id*., Return all downloadable resources for *dataset_id*., _check_dataset_ids_stable(), _check_get_dataset_returns_dataset_with_resources() (+26 more)

### Community 23 - "BaseAuth"
Cohesion: 0.25
Nodes (7): BaseAuth, ABC, Abstract base for authentication strategies., Protocol for pluggable authentication. Each strategy knows how to decorate a…, Bearer-token (OAuth 2.0 / JWT) authentication strategy., Custom-headers authentication strategy. Useful for portals that expect a non-…, Credential provider port Protocol.

### Community 24 - "CI Workflow"
Cohesion: 0.16
Nodes (19): Conventional Commits Release Pipeline (release-please -> publish), uv package manager (used across all workflows), Dependabot Config (github-actions + pip weekly), CI Workflow, CI All checks pass gate (alls-green), CI Build & validate package job (uv build + twine), CI Coverage job (combine + report), CI Lint & format job (ruff) (+11 more)

### Community 25 - "load_fixture"
Cohesion: 0.36
Nodes (7): load_fixture(), load_fixture_set(), Any, Path, Fixture loading helpers for the conformance suite. Hand-authored portal-…, Load a single hand-authored portal-response fixture from *path*. Args: path:…, Load a keyed fixture set: ``{fixture_name: parsed fixture JSON}``.

### Community 26 - ".search"
Cohesion: 0.33
Nodes (7): Query, SearchResult, Search one portal through injected session dependencies., Search through the injected composition substrate., Search through the facade without exposing a connector., Search one portal through the session substrate., search_datasets()

### Community 27 - "BatchStream"
Cohesion: 0.15
Nodes (12): BatchStream, Release the underlying reader and any owned closeables; idempotent (WR-02).…, Context-managed Arrow RecordBatch stream. Wraps a ``pa.RecordBatchReader``…, The pa.Schema for batches yielded by this stream., CheckpointableResourceReader, Protocol, Resource reader port Protocol (defined-only, no implementation yet — D-16)., Boundary protocol for opening a resource for streaming reads. The ``open``… (+4 more)

### Community 28 - ".request"
Cohesion: 0.24
Nodes (7): Any, Perform an HTTP request and return the raw response body. Raises: PortalError:…, GET *url* and return the response as text., GET *url* and return the response as parsed JSON., GET *url* and return the raw bytes (for file downloads)., Render *body* as text, truncating to *limit* characters., _truncate_body()

### Community 30 - "HttpxTransport"
Cohesion: 0.08
Nodes (31): ConditionalFetchResult, CredentialScope, RateLimiter, Response, RetryPolicy, _default_port(), _effective_origin(), _host_credential_provider_type() (+23 more)

### Community 31 - "FsspecStorage"
Cohesion: 0.19
Nodes (8): FsspecStorage, Any, Adapter wrapping an fsspec ``AbstractFileSystem`` to satisfy ``StoragePort``…, Persist *data* under *path* and return the resulting URI string. Args: data:…, Read and return the bytes stored under *path*., Return ``True`` if *path* exists in this filesystem., Return the backend-native path for *path*. Absolute URIs pass through…, Reconstruct a URI string for *path* (CORR-05).

### Community 32 - "PluginManager"
Cohesion: 0.15
Nodes (9): PluginFailure, PluginManager, Plugin manager for entry-point-based connector discovery. The…, Record of a failed plugin discovery or load. Attributes: name: Entry-point name…, Registry-free connector manager backed by ``importlib.metadata``. Built-in…, Register *factory* programmatically (used by tests, D-06)., Return the factory callable for *name*. Raises: AdapterNotFoundError: If no…, Return a sorted list of all registered connector names. (+1 more)

### Community 33 - "auth/__init__.py"
Cohesion: 0.29
Nodes (5): Authentication strategies for DataSluice., NoAuth, Any, No-op authentication strategy for public portals., Authentication strategy that adds no credentials. Suitable for portals that are…

### Community 34 - "DataSluice"
Cohesion: 0.15
Nodes (10): DetectionResult, DataSluice, detect_portal(), Detect a portal through caller-supplied infrastructure., Run injected portal detection without facade-specific mapping., Canonical public facade for discovery, resource access, and materialization., Detect a portal through the session's injected infrastructure., Return a lazy, single-use OpenedResource wrapper. (+2 more)

### Community 35 - "DataSluiceSession"
Cohesion: 0.09
Nodes (20): BaseAdapter, BaseAuth, CachePort, CredentialProvider, PluginManager, DataSluiceSession, Any, Query (+12 more)

### Community 36 - "RedactingFilter"
Cohesion: 0.29
Nodes (6): LogRecord, configure_logging(), Any, Redact known sensitive keys from log records (INFRA-06, D-P3-18). Walks…, Configure the package-level logger. Args: level: Logging level (e.g.…, RedactingFilter

### Community 37 - "downloader.py"
Cohesion: 0.18
Nodes (13): DownloadError, Raised when a resource download fails., Simple file-based cache for downloaded resources., Resource downloader with caching and checksum verification., IO layer: downloading, caching, checksums, and storage., ensure_dir(), Path, Local filesystem helpers for saving downloaded resources. (+5 more)

### Community 38 - "CachePort"
Cohesion: 0.25
Nodes (4): CachePort, Protocol, Cache port Protocol for content-addressed byte caching., Boundary protocol for a simple key/byte cache.

### Community 39 - "ChecksumMismatchError"
Cohesion: 0.24
Nodes (12): ChecksumMismatchError, Raised when a downloaded file's checksum does not match., compute_hash(), compute_md5(), compute_sha256(), Path, Checksum (hash) computation and verification., Compute the hex digest of *file_path* using *algorithm*. (+4 more)

### Community 41 - "BasicAuth"
Cohesion: 0.25
Nodes (4): BasicAuth, Any, HTTP Basic authentication strategy., Authenticate requests using HTTP Basic credentials. Args: username: Basic-auth…

### Community 42 - "Transport"
Cohesion: 0.22
Nodes (5): AbstractContextManager, Lazily initialised HTTP transport., Any, Transport boundary Protocol satisfied structurally by HTTP clients., Transport

### Community 43 - "search"
Cohesion: 0.20
Nodes (13): _dataset_json(), Any, Argument, help, Option, ``datasluice search`` command., Serialize one catalog dataset into a JSON-safe summary., Build one machine-readable search result envelope. (+5 more)

### Community 44 - "transport.py"
Cohesion: 0.24
Nodes (8): ConditionalFetchResult, ConditionalTransport, Protocol, Transport port Protocols for HTTP-like request execution. Three narrow runtime-…, Streaming transport boundary Protocol (D-P3-06/D-P3-07). `stream(url)` returns…, Result of a conditional resource fetch (D-P7-15/D-P7-16). A 304 Not Modified…, Optional transport capability for ETag/Last-Modified conditional GETs…, StreamingTransport

### Community 45 - "io/storage.py"
Cohesion: 0.13
Nodes (10): LocalStorage, ABC, Path, Storage abstraction for reading and writing resource files. Currently supports…, Abstract storage backend., Persist *data* under *key* and return the storage URI/path., Read and return the bytes stored under *key*., Return ``True`` if *key* exists in storage. (+2 more)

### Community 46 - "context.py"
Cohesion: 0.17
Nodes (12): create_ckan_connector(), Factory for the CKAN connector (entry-point target). ``create_ckan_connector``…, Construct a :class:`CKANAdapter` wired to the context's transport/auth., create_datagouv_connector(), Factory for the data.gouv.fr connector (entry-point target).…, Construct a :class:`DataGouvAdapter` wired to the context's transport/auth., create_socrata_connector(), Factory for the Socrata connector (entry-point target).… (+4 more)

### Community 47 - "http_client.py"
Cohesion: 0.07
Nodes (38): Logger, CredentialScope, Credential scoping model for host-bound credential policy., Host-scoped policy controlling where credentials may be sent. Attributes:…, RateLimitError, Raised when the portal rate-limits requests., Raised on HTTP 5xx responses that should be retried., RetryableHTTPError (+30 more)

### Community 48 - "inspect"
Cohesion: 0.21
Nodes (11): _dataset_json(), inspect(), Any, Argument, help, Option, ``datasluice inspect`` command., Serialize one catalog dataset into a JSON-safe metadata envelope. (+3 more)

### Community 49 - "Datasluice Brand Logo (no background)"
Cohesion: 0.29
Nodes (10): DataSluice Logo (Brand Mark), Datasluice Brand Logo (no background), Circuit Pathways Connectivity Element, Code Brackets (</>) Programming Icon, Funnel Processing Element, Filtered Output Basin, Padlock Security Icon, Table/Grid Tech Icon (+2 more)

### Community 50 - "_hashing.py"
Cohesion: 0.48
Nodes (6): _encode(), logical_sha256(), Any, Serialization-stable logical hashing for Arrow tables., Return a SHA-256 digest over an Arrow table's schema and logical rows., _schema_fingerprint()

### Community 51 - "data/__init__.py"
Cohesion: 0.24
Nodes (9): __getattr__(), Streaming data-plane package: BatchStream, byte-source adapter, schema mapper.…, Lazily export BatchStream, IterableBytesIO, to_arrow_schema,…, Any, Domain Schema → Arrow Schema mapper and batch unification helper. The…, Derive a ``pa.Schema`` from a domain :class:`Schema` for display. Maps known…, Concatenate ``RecordBatch`` objects under a unified ``pa.Schema`` (DATA-08,…, to_arrow_schema() (+1 more)

### Community 53 - "HeadersAuth"
Cohesion: 0.33
Nodes (3): HeadersAuth, Any, Authenticate requests using arbitrary static headers. Args: headers: A mapping…

### Community 54 - "FileCache"
Cohesion: 0.17
Nodes (9): FileCache, Path, A time-based file cache. Args: cache_dir: Directory to store cached files. ttl:…, Return cached bytes for *key*, or ``None`` if missing/expired., Store *data* under *key* and return the cache file path., Return ``True`` if *key* is cached and not expired., Remove all entries from the cache., Downloader (+1 more)

### Community 56 - "PortalError"
Cohesion: 0.20
Nodes (13): map_ckan_error(), CKAN-specific error mapping., Map a CKAN HTTP error response to a DataSluice exception. Args: status_code:…, map_datagouv_error(), data.gouv.fr (udata) specific error mapping., Map a udata HTTP error response to a DataSluice exception., map_socrata_error(), Socrata-specific error mapping. (+5 more)

### Community 57 - "geojson.py"
Cohesion: 0.23
Nodes (11): _batch_from_rows(), _fmt_coord(), GeoJSONReader, Any, Streaming GeoJSON reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-11).…, Format a coordinate value: integers without trailing ``.0``; floats as-is., Stream a GeoJSON ``BinaryIO`` source into Arrow ``RecordBatch`` objects. Each…, Yield ``RecordBatch`` objects by flattening GeoJSON Features. Args: source: A… (+3 more)

### Community 58 - "DataPlaneResourceReader"
Cohesion: 0.22
Nodes (10): DataPlaneResourceReader, Open *resource* as a :class:`BatchStream` of Arrow ``RecordBatch``. Dispatches…, Open a seekable Parquet resource from an exact row-group cursor., HttpDownload: stream via StreamingTransport or buffer via urllib fallback…, ObjectStorage: open via open_filesystem().open() (D-P4-07)., LocalFile: open(path, 'rb')., Concrete ``ResourceReader`` implementing access-kind dispatch (DATA-04). Args:…, Return the resource's access descriptor, defaulting to HttpDownload (D-P4-06). (+2 more)

### Community 59 - "domain/__init__.py"
Cohesion: 0.11
Nodes (24): Abstract base class for all portal adapters., CKAN adapter implementation. Communicates with the CKAN Action API…, data.gouv.fr (udata) adapter implementation. Communicates with the udata REST…, _is_set(), Pre-flight reject policy for unsupported ``Query`` filter fields (D-P5-06). The…, Raise ``UnsupportedQueryFieldError`` if *query* uses a field not in…, Return ``True`` when *value* counts as a set filter field. ``None``, empty…, _reject_unsupported_fields() (+16 more)

### Community 60 - "Pre-commit Configuration"
Cohesion: 0.50
Nodes (5): pre-commit-hooks (basic hooks), pytest testing framework, Ruff linter and formatter, ty type checker (Astral), Pre-commit Configuration

### Community 61 - "exceptions.py"
Cohesion: 0.07
Nodes (39): Exception, Host-scoped credential resolver with single-flight refresh (INFRA-04).…, BatchCursor, ParquetRowGroupPosition, Any, BatchStream — context-managed Arrow RecordBatch stream (DATA-01, DATA-02,…, Yield batches with the closed cursor for the next unread row group. In…, Return a PyCapsule for zero-copy Arrow interop (Phase 6 terminals). Delegates… (+31 more)

### Community 62 - "ParquetReader"
Cohesion: 0.23
Nodes (9): ParquetReader, Any, Streaming Parquet reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-10).…, Read one complete Parquet row group as one RecordBatch., Return ``source.seekable()`` if available; ``False`` on any error., Stream a Parquet ``BinaryIO`` source into Arrow ``RecordBatch`` objects. On a…, Yield ``RecordBatch`` objects by streaming Parquet row groups. Args: source: A…, Yield ``(row_group_index, batch)`` tuples for each non-empty row group. Each… (+1 more)

### Community 63 - "download"
Cohesion: 0.22
Nodes (9): Path, download(), Argument, help, Option, ``datasluice download`` command — raw bulk copy (D-15)., Render raw download results to stdout., Download all resources from a dataset as raw bulk copies. (+1 more)

### Community 64 - "FormatError"
Cohesion: 0.30
Nodes (8): _first_non_whitespace_byte(), JSONReader, Any, Streaming JSON reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-10).…, Stream a JSON / JSONL ``BinaryIO`` source into Arrow ``RecordBatch`` objects., Yield ``RecordBatch`` objects from a JSON array or JSONL source. Args: source:…, FormatError, Raised when a resource cannot be parsed in the expected format.

### Community 65 - "SECURITY.md"
Cohesion: 0.67
Nodes (3): CodeQL (security scanning), Dependabot (dependency updates), Zizmor (workflow audit)

### Community 66 - "OpenCodeReview PR Review Workflow (alibaba open-code-review)"
Cohesion: 0.83
Nodes (4): AI-assisted PR automation pattern, Renovate bot exclusion pattern (skip AI review for bots), OpenCodeReview PR Review Workflow (alibaba open-code-review), PR Agent Workflow (/describe auto-generate title & description)

### Community 67 - "Formats Layer (datasluice.formats)"
Cohesion: 0.50
Nodes (4): Formats Layer (datasluice.formats), Integrations Layer (datasluice.integrations), IO Layer (datasluice.io), Lazy imports of optional deps principle

### Community 68 - "duckdb.py"
Cohesion: 0.33
Nodes (6): Any, DuckDB integration (read paths removed per D-P4-18; SEC-03 utility preserved).…, Validate that *table_name* is a safe SQL identifier. Args: table_name: The…, Register *stream* as a named DuckDB relation (INTG-04, D-P6-14). No SQL string…, to_duckdb(), _validate_table_name()

### Community 70 - "ports/detector.py"
Cohesion: 0.33
Nodes (4): PortalDetector, Protocol, Portal detector port Protocol., Detection seam protocol returning evidence-based portal identification.

### Community 71 - "StoragePort"
Cohesion: 0.25
Nodes (4): Protocol, Storage port Protocol returning URI references (CORR-05)., Boundary protocol for byte storage addressed by path/URI strings., StoragePort

### Community 72 - ".read_batches"
Cohesion: 0.50
Nodes (4): Any, Yield ``RecordBatch`` objects by delegating to ``pyarrow.csv.open_csv``. Args:…, Drain a ``pa.RecordBatchReader`` and re-chunk into ``batch_size``-row batches., _rechunk_reader()

### Community 73 - "Defense-in-depth CI security scanning pattern"
Cohesion: 1.00
Nodes (3): Defense-in-depth CI security scanning pattern, CodeQL Workflow (actions + python security analysis), Zizmor Workflow Security Analysis

### Community 74 - "Datasluice Brand Identity"
Cohesion: 1.00
Nodes (3): Datasluice Brand Identity, Datasluice Logo / Brand Mark, Datasluice Documentation Logo Asset

### Community 75 - "Pull Request Template (affected areas + AI provenance)"
Cohesion: 0.67
Nodes (3): Bug Report Issue Template, Feature Request Issue Template, Pull Request Template (affected areas + AI provenance)

### Community 76 - ".read_batches"
Cohesion: 0.50
Nodes (4): _batch_from_rows(), Any, Yield ``RecordBatch`` objects by chunking openpyxl ``iter_rows``. Args: source:…, Build a single ``RecordBatch`` from a chunk of row dicts.

### Community 79 - ".download"
Cohesion: 0.50
Nodes (3): Path, Download multiple *resources* into *dest*., Download a single *resource* and return the local file path. Args: resource:…

### Community 82 - "_byte_source.py"
Cohesion: 0.29
Nodes (5): HTTPMessage, IO, Request, IterableBytesIO — adapt an ``Iterable[bytes]`` into a non-seekable BinaryIO…, Return the follow-up request, stripping sensitive headers when required.

### Community 87 - "BearerAuth"
Cohesion: 0.33
Nodes (3): BearerAuth, Any, Authenticate requests using a bearer token in the ``Authorization`` header.…

### Community 88 - "CKANAdapter"
Cohesion: 0.09
Nodes (20): CKANAdapter, Fetch organization metadata via ``organization_show``., Adapter for CKAN-powered open-data portals. Uses the CKAN Action API at…, Call a CKAN Action API endpoint and return the ``result`` dict., Search datasets via ``package_search``. Translates every set supported…, Fetch a dataset via ``package_show``., Return resources for *dataset_id*., CKAN portal adapter. CKAN is the world's most widely deployed open-data… (+12 more)

### Community 89 - "materialize"
Cohesion: 0.08
Nodes (25): Argument, callback, help, is_eager, Option, main(), help, Option (+17 more)

### Community 91 - "DataGouvAdapter"
Cohesion: 0.11
Nodes (13): DataGouvAdapter, Fetch organization metadata via ``/organizations/{slug}/``., Adapter for data.gouv.fr and other udata-powered portals. Uses the udata REST…, Call a udata API endpoint and return the parsed JSON., Search datasets via ``/datasets/``. Translates every set supported ``Query``…, Fetch a dataset via ``/datasets/{id}/``., Return resources for *dataset_id*., data.gouv.fr portal adapter. The French national open-data platform, powered by… (+5 more)

### Community 95 - "APIKeyAuth"
Cohesion: 0.25
Nodes (4): APIKeyAuth, Any, API-key authentication strategy. Supports passing the key via a header (default…, Authenticate requests using an API key. Args: api_key: The API key value.…

### Community 96 - "BaseFormatReader"
Cohesion: 0.18
Nodes (13): BaseFormatReader, ABC, Abstract base class for streaming format readers (D-P4-10). Each reader…, Protocol for decoding a single file format into Arrow ``RecordBatch`` objects.…, CSVReader, Streaming CSV reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-10).…, Stream a CSV ``BinaryIO`` source into Arrow ``RecordBatch`` objects. Args:…, get_reader() (+5 more)

### Community 100 - "Resource"
Cohesion: 0.08
Nodes (42): map_resource(), Mapping functions to convert CKAN-native JSON into domain models., Resolve the resource access descriptor per D-P5-02. HttpDownload wins when…, Best-effort schema extraction per D-P5-03. Reads ``datastore_fields`` first…, Convert a CKAN resource dict into a :class:`Resource`. Populates advisory…, _resolve_access(), _resolve_schema(), map_dataset() (+34 more)

### Community 115 - "IterableBytesIO"
Cohesion: 0.13
Nodes (7): RawIOBase, IterableBytesIO, WriteableBuffer, Mark the stream closed and exhaust the iterator., Wrap an ``Iterable[bytes]`` into a non-seekable ``BinaryIO``. The canonical use…, Read up to ``len(b)`` bytes into the provided buffer. Args: b: A writable…, Read up to ``n`` bytes; ``n=-1`` reads all remaining bytes. Args: n: Maximum…

### Community 117 - "transport/pagination.py"
Cohesion: 0.29
Nodes (6): paginate(), PaginationConfig, T, Generic pagination helpers shared across adapters., Common pagination parameters. Attributes: page_size: Number of items per page.…, Lazily yield pages of results. Args: fetch_page: Callable taking…

### Community 122 - "ports/__init__.py"
Cohesion: 0.27
Nodes (9): CatalogPort, OrganizationCatalog, Protocol, Catalog port Protocols for portal catalog connectors., Marker base protocol all catalog connectors share. Attributes: portal_type:…, Capability protocol for dataset search., Capability protocol for organization lookup., SearchableCatalog (+1 more)

## Knowledge Gaps
- **21 isolated node(s):** `Funding Config (Buy Me a Coffee: nitishraj)`, `Bug Report Issue Template`, `Issue Template Config (blank issues enabled)`, `Feature Request Issue Template`, `CI Lint & format job (ruff)` (+16 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **23 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `OpenedResource` connect `Any` to `Resource`, `DataSluice`, `application.py`, `exceptions.py`?**
  _High betweenness centrality (0.055) - this node is a cross-community bridge._
- **Why does `BatchStream` connect `BatchStream` to `TransformContext`, `duckdb.py`, `data/access.py`, `to_arrow`, `data/__init__.py`, `DataPlaneResourceReader`, `exceptions.py`?**
  _High betweenness centrality (0.046) - this node is a cross-community bridge._
- **Why does `FileStateStore` connect `sync/state_store.py` to `sync.py`?**
  _High betweenness centrality (0.044) - this node is a cross-community bridge._
- **Are the 8 inferred relationships involving `BatchStream` (e.g. with `DataPlaneResourceReader` and `_StreamClosingBytesIO`) actually correct?**
  _`BatchStream` has 8 INFERRED edges - model-reasoned connections that need verification._
- **Are the 4 inferred relationships involving `Resource` (e.g. with `Dataset` and `License`) actually correct?**
  _`Resource` has 4 INFERRED edges - model-reasoned connections that need verification._
- **Are the 5 inferred relationships involving `DataSluiceError` (e.g. with `DataPlaneResourceReader` and `_StreamClosingBytesIO`) actually correct?**
  _`DataSluiceError` has 5 INFERRED edges - model-reasoned connections that need verification._
- **Are the 6 inferred relationships involving `BaseAuth` (e.g. with `APIKeyAuth` and `BasicAuth`) actually correct?**
  _`BaseAuth` has 6 INFERRED edges - model-reasoned connections that need verification._
