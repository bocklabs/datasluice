# Graph Report - datasluice  (2026-08-09)

## Corpus Check
- 130 files · ~43,775 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1589 nodes · 3602 edges · 95 communities (85 shown, 10 thin omitted)
- Extraction: 94% EXTRACTED · 6% INFERRED · 0% AMBIGUOUS · INFERRED: 216 edges (avg confidence: 0.58)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `9e774e69`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- FileStateStore
- sync.py
- sync/materialize.py
- ContentCache
- host_provider.py
- data/access.py
- DataSluice (Public API class)
- Resource
- AtomicStateStore
- FormatError
- resolve_one_resource
- sync/state_store.py
- _identity.py
- TransformContext
- artifact.py
- socrata/adapter.py
- DataSluiceError
- .open
- SyncState
- detect.py
- ParquetReader
- redirect.py
- Query
- resource_locator_from_dict
- CI Workflow
- app.py
- FileCache
- BatchStream
- HttpClient
- logging.py
- HttpxTransport
- FsspecStorage
- PluginManager
- .__init__
- .search
- ckan/adapter.py
- StoragePort
- content_cache.py
- render_json
- ChecksumMismatchError
- InMemoryStateStore
- load_fixture
- Transport
- search.py
- ports/__init__.py
- PortalError
- LocalStorage
- transport/__init__.py
- main
- Datasluice Brand Logo (no background)
- BasicAuth
- Schema
- _effective_origin
- ResourceAccess
- BearerAuth
- HeadersAuth
- logical_sha256
- ._decode_envelope
- .apply
- sync/__init__.py
- Pre-commit Configuration
- .iter_batches_with_cursors
- httpx_transport.py
- SearchResult
- DataSluice
- SECURITY.md
- OpenCodeReview PR Review Workflow (alibaba open-code-review)
- Formats Layer (datasluice.formats)
- inspect.py
- _SingleStreamReader
- ConnectorContext
- .portal
- Feature Request Issue Template
- Defense-in-depth CI security scanning pattern
- Datasluice Brand Identity
- Bug Report Issue Template
- connectors/__init__.py
- integrations/__init__.py
- Funding Config (Buy Me a Coffee: nitishraj)
- Issue Template Config (blank issues enabled)
- BaseAuth
- DataGouvPage
- domain/__init__.py
- io/__init__.py
- geojson.py
- CatalogCapabilities
- Downloader
- IterableBytesIO
- session.py
- .read_batches
- .to_dict
- StreamResponse
- auth/__init__.py
- discovery/detector.py
- RetryableHTTPError

## God Nodes (most connected - your core abstractions)
1. `DataSluiceError` - 71 edges
2. `Resource` - 60 edges
3. `BatchStream` - 40 edges
4. `DataSluice` - 39 edges
5. `DataPlaneResourceReader` - 34 edges
6. `BaseAuth` - 33 edges
7. `Query` - 32 edges
8. `OpenedResource` - 31 edges
9. `SyncState` - 28 edges
10. `sanitize_uri()` - 27 edges

## Surprising Connections (you probably didn't know these)
- `DataSluice Logo (Brand Mark)` --semantically_similar_to--> `Datasluice Brand Logo (no background)`  [INFERRED] [semantically similar]
  docs/assets/datasluice-logo.png → docs/assets/datasluice-logo-nbg.png
- `DirectResourceLocator` --uses--> `DataPlaneResourceReader`  [INFERRED]
  src/datasluice/application.py → src/datasluice/data/access.py
- `DirectResourceLocator` --uses--> `Downloader`  [INFERRED]
  src/datasluice/application.py → src/datasluice/io/downloader.py
- `DirectResourceLocator` --uses--> `DataSluiceSession`  [INFERRED]
  src/datasluice/application.py → src/datasluice/runtime/session.py
- `CatalogResourceLocator` --uses--> `DataPlaneResourceReader`  [INFERRED]
  src/datasluice/application.py → src/datasluice/data/access.py

## Import Cycles
- 3-file cycle: `src/datasluice/domain/__init__.py -> src/datasluice/domain/artifact.py -> src/datasluice/exceptions.py -> src/datasluice/domain/__init__.py`

## Hyperedges (group relationships)
- **Conventional Commits Release Flow** — github_workflows_release_please, github_workflows_publish_build_job, github_workflows_publish_testpypi_job, github_workflows_publish_pypi_job [INFERRED 0.95]
- **CI Quality Gate Pipeline** — github_workflows_ci_lint_job, github_workflows_ci_typecheck_job, github_workflows_ci_test_job, github_workflows_ci_coverage_job, github_workflows_ci_build_job, github_workflows_ci_smoke_test_job, github_workflows_ci_all_checks_pass_job [INFERRED 0.95]
- **AI PR Automation Stack (shared LLM secrets + renovate exclusion)** — github_workflows_ocr_review, github_workflows_pr_agent, concept_ai_pr_automation, concept_renovate_bot_exclusion [INFERRED 0.85]
- **QA pipeline (just qa / make qa)** — agents_md, ruff_linter, ty_typechecker, pytest_framework [EXTRACTED 1.00]
- **Automated release flow** — conventional_commits, release_please, changelog_md, contributing_md [EXTRACTED 1.00]
- **Adapter layer core components** — baseadapter_protocol, registry_concept, factory_concept, adapters_layer [EXTRACTED 1.00]
- **DataSluice unified open-data portal access pattern** — datasluice_class, portal_ckan, portal_datagouv, portal_socrata [INFERRED 0.95]
- **Pre-commit quality gate pipeline (format, lint, typecheck, test)** — precommit_config, lib_ruff, lib_ty, lib_pytest [EXTRACTED 1.00]

## Communities (95 total, 10 thin omitted)

### Community 0 - "FileStateStore"
Cohesion: 0.11
Nodes (14): RLock, FileStateStore, Whether this backend's ``mv`` is a true atomic rename (CR-11)., Return the process-global lock scope for *key* on this store (CR-02). Two…, Hold the per-key lock for *key* so callers serialize a multi-step transaction…, Acquire (lazily creating) the process-global per-key lock, tracking users…, Return the SHA-256-hexdigest (.json) path for *key* (T-07-03 mitigation)., Load the :class:`SyncState` for *key*, or ``None`` if absent. Raises:… (+6 more)

### Community 1 - "sync.py"
Cohesion: 0.14
Nodes (30): ParquetRowGroupPosition, Logical position at the next unread Parquet row group., canonical_destination_identity(), Return a secret-free SHA-256 identity for a destination URI., _Checkpoint, _completed_artifact_record(), _completed_sync_state(), _compute_source_version() (+22 more)

### Community 2 - "sync/materialize.py"
Cohesion: 0.15
Nodes (35): AbstractFileSystem, Artifact, A strict, immutable schema-v1 materialization envelope., open_filesystem(), Any, Instantiate the correct fsspec backend for *uri* (INFRA-05, D-P3-20). Delegates…, canonical_identity(), Return the SHA-256 canonical identity for *resource*. The identity is… (+27 more)

### Community 3 - "ContentCache"
Cohesion: 0.13
Nodes (15): Connection, ContentCache, Any, Return the SHA-256 hexdigest of *key* (SEC-06 carry-forward, D-P3-09)., Return the absolute content-file path for a SHA-256 digest., Return cached bytes for *key*, or ``None`` on miss / expiry / writing., Store *data* under *key* (CachePort signature UNCHANGED, D-P3-12)., Store *data* under *key* with ETag/Last-Modified sidecar (D-P3-12). Phase 4's… (+7 more)

### Community 4 - "host_provider.py"
Cohesion: 0.15
Nodes (11): Lock, Refresher, HostCredentialProvider, datetime, Host-scoped credential resolver with single-flight refresh (INFRA-04).…, Drop the cached credential for *host* (off-port; D-P3-15). Called by…, Resolve a :class:`BaseAuth` per host with cached expiry and single-flight…, Return the per-host lock, creating it if necessary. The dict-level lock is held… (+3 more)

### Community 5 - "data/access.py"
Cohesion: 0.12
Nodes (20): _chain(), _close_source(), _content_encoding_from_headers(), Any, Concrete ``ResourceReader`` implementation with access-kind dispatch (DATA-04,…, Open an already-fetched streaming response through the data plane., Open a seekable Parquet resource from an exact row-group cursor., Dispatch to the format reader and wrap output in BatchStream. (+12 more)

### Community 6 - "DataSluice (Public API class)"
Cohesion: 0.05
Nodes (45): Adapter isolation principle, Adapter 4-module pattern (adapter/mapper/pagination/errors), Adapters Layer (datasluice.adapters), Auth Layer (datasluice.auth), BaseAdapter protocol, CKAN adapter, CKAN portal platform, Composable transport (decorators) principle (+37 more)

### Community 7 - "Resource"
Cohesion: 0.06
Nodes (35): _ApplicationServices, _locator_from_resource(), materialize(), open_resource(), OpenedResource, Any, ResourceLocator, Open one resource through the injected data-plane reader. (+27 more)

### Community 8 - "AtomicStateStore"
Cohesion: 0.13
Nodes (9): AtomicStateStore, Protocol, State store port Protocols for incremental sync state (SYNC-01). The base…, Boundary protocol for persisting incremental sync state., Additive capability Protocol for compare-and-swap (CAS) state writes…, Return the raw envelope bytes for *key*, or ``None`` if absent. The returned…, Atomically load ``(state, version)`` from one backend read (CR-01). Returns…, Persist *state* under *key* only if the current version matches… (+1 more)

### Community 9 - "FormatError"
Cohesion: 0.11
Nodes (26): BaseFormatReader, ABC, Abstract base class for streaming format readers (D-P4-10). Each reader…, Protocol for decoding a single file format into Arrow ``RecordBatch`` objects.…, CSVReader, Any, Streaming CSV reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-10).…, Stream a CSV ``BinaryIO`` source into Arrow ``RecordBatch`` objects. Args:… (+18 more)

### Community 10 - "resolve_one_resource"
Cohesion: 0.15
Nodes (16): ParsedLocator, Any, ResourceLocator, Resolve exactly one Resource and reject ambiguous catalog datasets., resolve_one_resource(), Any, Argument, help (+8 more)

### Community 11 - "sync/state_store.py"
Cohesion: 0.33
Nodes (17): Raised when a state store cannot read or write durable sync state (D-P7-26). A…, StateStoreError, _contains_secret_material(), _is_completed_watermark(), _is_safe_destination_uri(), _is_sha256(), _is_source_version(), Any (+9 more)

### Community 12 - "_identity.py"
Cohesion: 0.33
Nodes (5): Canonical resource identity (CR-01 blocker fix, SYNC-05/07). Portal-controlled…, Extract the origin scope for canonical identity hashing. HTTP(S) resources…, Reject duplicate canonical identities before any artifact or state write.…, _url_origin(), validate_unique_identities()

### Community 13 - "TransformContext"
Cohesion: 0.06
Nodes (43): Raised when a transform step cannot be applied (D-P6-15). Transform failures…, TransformError, __getattr__(), Composable transform pipeline package (TRANS-01..09). Re-exports are resolved…, Lazily export transform symbols (mirrors datasluice.data.__getattr__).…, _build_batch_stream(), _chain(), compose() (+35 more)

### Community 14 - "artifact.py"
Cohesion: 0.16
Nodes (15): ArtifactProvenance, _contract_error(), Digest, _freeze_extensions(), _freeze_json(), _is_sha256(), _object_dict(), _public_uri() (+7 more)

### Community 15 - "socrata/adapter.py"
Cohesion: 0.09
Nodes (22): _is_set(), Pre-flight reject policy for unsupported ``Query`` filter fields (D-P5-06). The…, Raise ``UnsupportedQueryFieldError`` if *query* uses a field not in…, Return ``True`` when *value* counts as a set filter field. ``None``, empty…, _reject_unsupported_fields(), Socrata adapter implementation. Communicates with the Socrata Discovery API and…, Fetch a dataset (view) by its 4x4 identifier., Return resources for *dataset_id*. (+14 more)

### Community 16 - "DataSluiceError"
Cohesion: 0.09
Nodes (44): Exception, CatalogResourceLocator, DirectResourceLocator, Portal, Public application facade, locators, and opened-resource lifecycle., A validated, serializable catalog resource reference., Return a fresh, secret-free locator envelope., Stable application wrapper for a portal URL. (+36 more)

### Community 17 - ".open"
Cohesion: 0.09
Nodes (21): BaseException, Open *resource* as a :class:`BatchStream` of Arrow ``RecordBatch``. Dispatches…, apply_compression(), _detect_format(), _ErrorTranslatingReader, PeekableReader, Any, WriteableBuffer (+13 more)

### Community 18 - "SyncState"
Cohesion: 0.21
Nodes (10): SyncState model for incremental sync cursors and watermarks (SYNC-03)., Incremental synchronization state for a resource or connector. Attributes:…, SyncState, Raised when a state write loses an optimistic compare-and-swap race (D-P7-27).…, SyncStateConflictError, _encode_state(), Persist *state* under *key* via an atomic, optionally CAS-protected write.…, Persist *state* under *key* only if the current version matches… (+2 more)

### Community 19 - "detect.py"
Cohesion: 0.19
Nodes (13): detect(), _detection_json(), Any, Argument, help, Option, ``datasluice detect`` command — evidence-based portal detection (D-P5-21, D-07)., Serialize one public DetectionResult into a JSON-safe envelope. (+5 more)

### Community 20 - "ParquetReader"
Cohesion: 0.27
Nodes (8): ParquetReader, Any, Read one complete Parquet row group as one RecordBatch., Return ``source.seekable()`` if available; ``False`` on any error., Stream a Parquet ``BinaryIO`` source into Arrow ``RecordBatch`` objects. On a…, Yield ``RecordBatch`` objects by streaming Parquet row groups. Args: source: A…, Yield ``(row_group_index, batch)`` tuples for each non-empty row group. Each…, _safe_seekable()

### Community 21 - "redirect.py"
Cohesion: 0.14
Nodes (14): HTTPMessage, ParseResult, CredentialScope, Credential scoping model for host-bound credential policy., Host-scoped policy controlling where credentials may be sent. Attributes:…, CredentialAwareRedirectHandler, _effective_port(), Request (+6 more)

### Community 22 - "Query"
Cohesion: 0.08
Nodes (36): BaseAdapter, ABC, Abstract base class for all portal adapters., Protocol that every portal adapter must implement. Subclasses translate portal-…, Lazily initialised HTTP transport., Search for datasets matching *query*., Fetch a single dataset by its portal-native *dataset_id*., Return all downloadable resources for *dataset_id*. (+28 more)

### Community 23 - "resource_locator_from_dict"
Cohesion: 0.31
Nodes (7): _contract_error(), _object_dict(), Decode one strict catalog locator envelope., Decode one strict, tagged ResourceLocator envelope., Decode one strict direct locator envelope., resource_locator_from_dict(), _validate_uri()

### Community 24 - "CI Workflow"
Cohesion: 0.17
Nodes (18): Conventional Commits Release Pipeline (release-please -> publish), uv package manager (used across all workflows), CI Workflow, CI All checks pass gate (alls-green), CI Build & validate package job (uv build + twine), CI Coverage job (combine + report), CI Lint & format job (ruff), CI Smoke test job (install wheel, import datasluice) (+10 more)

### Community 25 - "app.py"
Cohesion: 0.16
Nodes (11): Main Typer application for the DataSluice CLI., Command-line interface for DataSluice., materialize(), Any, Argument, help, Option, ``datasluice materialize`` command for one canonical Artifact result. (+3 more)

### Community 26 - "FileCache"
Cohesion: 0.22
Nodes (7): FileCache, Path, A time-based file cache. Args: cache_dir: Directory to store cached files. ttl:…, Return cached bytes for *key*, or ``None`` if missing/expired., Store *data* under *key* and return the cache file path., Return ``True`` if *key* is cached and not expired., Remove all entries from the cache.

### Community 27 - "BatchStream"
Cohesion: 0.08
Nodes (31): BatchCursor, BatchStream, BatchStream — context-managed Arrow RecordBatch stream (DATA-01, DATA-02,…, Closed continuation cursor for the next unread batch. ``next_batch_index``…, Context-managed Arrow RecordBatch stream. Wraps a ``pa.RecordBatchReader``…, Any, to_arrow terminal: materialize a BatchStream into a pa.Table (INTG-01,…, Materialize *stream* into a ``pa.Table`` (INTG-01, D-P6-02). The shared… (+23 more)

### Community 28 - "HttpClient"
Cohesion: 0.29
Nodes (7): HttpClient, Any, Perform an HTTP request and return the raw response body. Raises: PortalError:…, GET *url* and return the response as text., GET *url* and return the response as parsed JSON., GET *url* and return the raw bytes (for file downloads)., Thin HTTP client wrapping :mod:`urllib` with auth, retry, and rate-limiting.…

### Community 29 - "logging.py"
Cohesion: 0.11
Nodes (18): Logger, LogRecord, datasluice_source(), mirror_dlt_state(), Any, dlt (data load tool) integration: use DataSluice as a dlt source. Requires…, Mirror dlt's load-committed per-resource watermarks into a DataSluice…, Return a deterministic destination-safe name for a resource ID. (+10 more)

### Community 30 - "HttpxTransport"
Cohesion: 0.10
Nodes (24): Response, _parse_retry_after(), Parse a ``Retry-After`` header into a delay in seconds. Supports both delta-…, Render *body* as text, truncating to *limit* characters., _truncate_body(), _host_credential_provider_type(), HttpxTransport, Any (+16 more)

### Community 31 - "FsspecStorage"
Cohesion: 0.14
Nodes (11): FsspecStorage, _has_parent_segments(), Any, fsspec-backed storage adapter satisfying :class:`StoragePort` (INFRA-02,…, Adapter wrapping an fsspec ``AbstractFileSystem`` to satisfy ``StoragePort``…, Persist *data* under *path* and return the resulting URI string. Args: data:…, Read and return the bytes stored under *path*., Return ``True`` if *path* exists in this filesystem. (+3 more)

### Community 32 - "PluginManager"
Cohesion: 0.14
Nodes (13): AdapterNotFoundError, Raised when no adapter is registered for a portal type., Composition root and plugin machinery for DataSluice., PluginFailure, PluginManager, Any, Plugin manager for entry-point-based connector discovery. The…, Record of a failed plugin discovery or load. Attributes: name: Entry-point name… (+5 more)

### Community 33 - ".__init__"
Cohesion: 0.15
Nodes (8): CachePort, Protocol, Cache port Protocol for content-addressed byte caching., Boundary protocol for a simple key/byte cache., CredentialProvider, Protocol, Boundary protocol resolving authentication strategies per host., Lazily construct the default ContentCache (plan 03-03) if importable. Resolved…

### Community 34 - ".search"
Cohesion: 0.25
Nodes (5): Search one portal through injected session dependencies., Search through the injected composition substrate., Search through the facade without exposing a connector., Search one portal through the session substrate., search_datasets()

### Community 35 - "ckan/adapter.py"
Cohesion: 0.08
Nodes (30): CKANAdapter, CKAN adapter implementation. Communicates with the CKAN Action API…, Fetch a dataset via ``package_show``., Return resources for *dataset_id*., Fetch organization metadata via ``organization_show``., Adapter for CKAN-powered open-data portals. Uses the CKAN Action API at…, Call a CKAN Action API endpoint and return the ``result`` dict. CKAN returns…, Search datasets via ``package_search``. Translates every set supported… (+22 more)

### Community 36 - "StoragePort"
Cohesion: 0.25
Nodes (4): Protocol, Storage port Protocol returning URI references (CORR-05)., Boundary protocol for byte storage addressed by path/URI strings., StoragePort

### Community 37 - "content_cache.py"
Cohesion: 0.29
Nodes (6): Content-addressed cache backed by a SQLite WAL index + content files…, Centralised filesystem factory (INFRA-05). All fsspec backend instantiation…, Best-effort removal of *path* on *fs*; ignore absence and secondary OSError.…, safe_remove(), __getattr__(), Lazily export ContentCache, FsspecStorage, and open_filesystem (D-P3-01 lazy…

### Community 38 - "render_json"
Cohesion: 0.15
Nodes (17): _iter_rows(), open(), Any, Argument, help, Option, ``datasluice open`` command for bounded previews and JSONL streams., Yield rows incrementally while honoring an optional row limit. (+9 more)

### Community 39 - "ChecksumMismatchError"
Cohesion: 0.24
Nodes (12): ChecksumMismatchError, Raised when a downloaded file's checksum does not match., compute_hash(), compute_md5(), compute_sha256(), Path, Checksum (hash) computation and verification., Compute the hex digest of *file_path* using *algorithm*. (+4 more)

### Community 40 - "InMemoryStateStore"
Cohesion: 0.29
Nodes (4): InMemoryStateStore, Ephemeral in-process :class:`StateStore` backed by a plain dict (D-P7-02).…, Store *state* under *key* (last-writer-wins; ephemeral)., Remove *key* if present; a missing key is tolerated.

### Community 41 - "load_fixture"
Cohesion: 0.36
Nodes (7): load_fixture(), load_fixture_set(), Any, Path, Fixture loading helpers for the conformance suite. Hand-authored portal-…, Load a single hand-authored portal-response fixture from *path*. Args: path:…, Load a keyed fixture set: ``{fixture_name: parsed fixture JSON}``.

### Community 42 - "Transport"
Cohesion: 0.24
Nodes (7): AbstractContextManager, Any, Protocol, Transport boundary Protocol satisfied structurally by HTTP clients., Streaming transport boundary Protocol (D-P3-06/D-P3-07). `stream(url)` returns…, StreamingTransport, Transport

### Community 43 - "search.py"
Cohesion: 0.20
Nodes (13): _dataset_json(), Any, Argument, help, Option, ``datasluice search`` command., Serialize one catalog dataset into a JSON-safe summary., Build one machine-readable search result envelope. (+5 more)

### Community 44 - "ports/__init__.py"
Cohesion: 0.16
Nodes (13): CatalogPort, OrganizationCatalog, Protocol, Catalog port Protocols for portal catalog connectors., Marker base protocol all catalog connectors share. Attributes: portal_type:…, Capability protocol for dataset search., Capability protocol for organization lookup., SearchableCatalog (+5 more)

### Community 45 - "PortalError"
Cohesion: 0.21
Nodes (15): map_ckan_error(), CKAN-specific error mapping., Map a CKAN HTTP error response to a DataSluice exception. Args: status_code:…, map_datagouv_error(), data.gouv.fr (udata) specific error mapping., Map a udata HTTP error response to a DataSluice exception., map_socrata_error(), Socrata-specific error mapping. (+7 more)

### Community 46 - "LocalStorage"
Cohesion: 0.33
Nodes (4): LocalStorage, Path, Local-filesystem storage backend. Args: base_dir: Root directory for stored…, Resolve *key* against the base directory, rejecting path traversal. A ``key``…

### Community 47 - "transport/__init__.py"
Cohesion: 0.14
Nodes (15): __getattr__(), Transport layer: HTTP client, retry, rate-limiting, and pagination.…, Lazily resolve httpx-backed symbols on first attribute access (PEP 562)., paginate(), PaginationConfig, T, Generic pagination helpers shared across adapters., Common pagination parameters. Attributes: page_size: Number of items per page.… (+7 more)

### Community 48 - "main"
Cohesion: 0.33
Nodes (6): callback, is_eager, main(), help, Option, DataSluice — unified open-data toolkit.

### Community 49 - "Datasluice Brand Logo (no background)"
Cohesion: 0.29
Nodes (10): DataSluice Logo (Brand Mark), Datasluice Brand Logo (no background), Circuit Pathways Connectivity Element, Code Brackets (</>) Programming Icon, Funnel Processing Element, Filtered Output Basin, Padlock Security Icon, Table/Grid Tech Icon (+2 more)

### Community 50 - "BasicAuth"
Cohesion: 0.25
Nodes (4): BasicAuth, Any, HTTP Basic authentication strategy., Authenticate requests using HTTP Basic credentials. Args: username: Basic-auth…

### Community 51 - "Schema"
Cohesion: 0.21
Nodes (9): Any, Domain Schema → Arrow Schema mapper and batch unification helper. The…, Derive a ``pa.Schema`` from a domain :class:`Schema` for display. Maps known…, Concatenate ``RecordBatch`` objects under a unified ``pa.Schema`` (DATA-08,…, to_arrow_schema(), unify_batches(), Schema model describing the column-level shape of a resource., Schema describing the columns of a tabular resource. Attributes: name: Logical… (+1 more)

### Community 52 - "_effective_origin"
Cohesion: 0.33
Nodes (5): _default_port(), _effective_origin(), Return whether sensitive headers must be stripped on this redirect hop. Mirrors…, Return the IANA default port for *scheme*, or ``None`` when unknown (CR-06)., Return ``(scheme, hostname, effective_port)`` for a parsed URL (CR-06).…

### Community 53 - "ResourceAccess"
Cohesion: 0.13
Nodes (22): map_dataset(), map_organization(), map_resource(), Any, Mapping functions to convert Socrata-native JSON into domain models., Resolve the resource access descriptor per D-P5-02 for Socrata views. Socrata…, Best-effort schema extraction per D-P5-03 for Socrata views. Socrata's catalog…, Convert a Socrata view/resource dict into a :class:`Resource`. Populates… (+14 more)

### Community 54 - "BearerAuth"
Cohesion: 0.25
Nodes (4): BearerAuth, Any, Bearer-token (OAuth 2.0 / JWT) authentication strategy., Authenticate requests using a bearer token in the ``Authorization`` header.…

### Community 55 - "HeadersAuth"
Cohesion: 0.25
Nodes (4): HeadersAuth, Any, Custom-headers authentication strategy. Useful for portals that expect a non-…, Authenticate requests using arbitrary static headers. Args: headers: A mapping…

### Community 56 - "logical_sha256"
Cohesion: 0.48
Nodes (6): _encode(), logical_sha256(), Any, Serialization-stable logical hashing for Arrow tables., Return a SHA-256 digest over an Arrow table's schema and logical rows., _schema_fingerprint()

### Community 57 - "._decode_envelope"
Cohesion: 0.60
Nodes (4): _decode_completed_cursor(), _decode_legacy_state(), _mapping_field(), Return the :class:`SyncState` for *key*, or ``None`` if absent.

### Community 59 - "sync/__init__.py"
Cohesion: 0.50
Nodes (3): __getattr__(), Incremental sync primitives: state stores, sync loop, idempotent materialize.…, Lazily export pyarrow-adjacent sync primitives. ``materialize`` remains lazy so…

### Community 60 - "Pre-commit Configuration"
Cohesion: 0.50
Nodes (5): pre-commit-hooks (basic hooks), pytest testing framework, Ruff linter and formatter, ty type checker (Astral), Pre-commit Configuration

### Community 61 - ".iter_batches_with_cursors"
Cohesion: 0.18
Nodes (6): Any, Yield batches with the closed cursor for the next unread row group. In…, Release the underlying reader and any owned closeables; idempotent (WR-02).…, Return a PyCapsule for zero-copy Arrow interop (Phase 6 terminals). Delegates…, The pa.Schema for batches yielded by this stream., Yield Arrow ``RecordBatch`` objects from the wrapped source. When ``indexed``…

### Community 62 - "httpx_transport.py"
Cohesion: 0.10
Nodes (20): NoAuth, Any, Authentication strategy that adds no credentials. Suitable for portals that are…, Default configuration constants., Configuration for DataSluice., create_default_transport(), Default transport factory for the DataSluiceSession composition root. Replaces…, Construct a default transport from ``DEFAULT_*`` constants. Picks… (+12 more)

### Community 63 - "SearchResult"
Cohesion: 0.14
Nodes (9): DataGouvAdapter, Adapter for data.gouv.fr and other udata-powered portals. Uses the udata REST…, Call a udata API endpoint and return the parsed JSON., Search datasets via ``/datasets/``. Translates every set supported ``Query``…, Fetch a dataset via ``/datasets/{id}/``., Return resources for *dataset_id*., data.gouv.fr portal adapter. The French national open-data platform, powered by…, A paginated page of search results. Attributes: datasets: Datasets returned in… (+1 more)

### Community 64 - "DataSluice"
Cohesion: 0.20
Nodes (11): DataSluice, Canonical public facade for discovery, resource access, and materialization., download(), Argument, help, Option, Path, ``datasluice download`` command — raw bulk copy (D-15). (+3 more)

### Community 65 - "SECURITY.md"
Cohesion: 0.67
Nodes (3): CodeQL (security scanning), Dependabot (dependency updates), Zizmor (workflow audit)

### Community 66 - "OpenCodeReview PR Review Workflow (alibaba open-code-review)"
Cohesion: 0.83
Nodes (4): AI-assisted PR automation pattern, Renovate bot exclusion pattern (skip AI review for bots), OpenCodeReview PR Review Workflow (alibaba open-code-review), PR Agent Workflow (/describe auto-generate title & description)

### Community 67 - "Formats Layer (datasluice.formats)"
Cohesion: 0.50
Nodes (4): Formats Layer (datasluice.formats), Integrations Layer (datasluice.integrations), IO Layer (datasluice.io), Lazy imports of optional deps principle

### Community 68 - "inspect.py"
Cohesion: 0.21
Nodes (11): _dataset_json(), inspect(), Any, Argument, help, Option, ``datasluice inspect`` command., Serialize one catalog dataset into a JSON-safe metadata envelope. (+3 more)

### Community 70 - "ConnectorContext"
Cohesion: 0.17
Nodes (12): create_ckan_connector(), Factory for the CKAN connector (entry-point target). ``create_ckan_connector``…, Construct a :class:`CKANAdapter` wired to the context's transport/auth., create_datagouv_connector(), Factory for the data.gouv.fr connector (entry-point target).…, Construct a :class:`DataGouvAdapter` wired to the context's transport/auth., create_socrata_connector(), Factory for the Socrata connector (entry-point target).… (+4 more)

### Community 71 - ".portal"
Cohesion: 0.33
Nodes (3): Retrieve catalog metadata through the private session substrate., Retrieve one dataset without exposing the underlying connector., Return a stable Portal wrapper for *url*.

### Community 73 - "Defense-in-depth CI security scanning pattern"
Cohesion: 1.00
Nodes (3): Defense-in-depth CI security scanning pattern, CodeQL Workflow (actions + python security analysis), Zizmor Workflow Security Analysis

### Community 74 - "Datasluice Brand Identity"
Cohesion: 1.00
Nodes (3): Datasluice Brand Identity, Datasluice Logo / Brand Mark, Datasluice Documentation Logo Asset

### Community 82 - "BaseAuth"
Cohesion: 0.19
Nodes (6): BaseAuth, ABC, Abstract base for authentication strategies., Protocol for pluggable authentication. Each strategy knows how to decorate a…, No-op authentication strategy for public portals., Credential provider port Protocol.

### Community 90 - "DataGouvPage"
Cohesion: 0.25
Nodes (5): DataGouvPage, Pagination helpers for the data.gouv.fr (udata) API. udata uses page-number /…, Parameters for a single udata results page (1-based)., Return the parameters for the following page., Convert to query-string parameters for the udata API.

### Community 91 - "domain/__init__.py"
Cohesion: 0.11
Nodes (28): data.gouv.fr (udata) adapter implementation. Communicates with the udata REST…, Fetch organization metadata via ``/organizations/{slug}/``., map_dataset(), map_license(), map_organization(), map_resource(), Any, Mapping functions to convert data.gouv.fr (udata) JSON into domain models. (+20 more)

### Community 95 - "io/__init__.py"
Cohesion: 0.20
Nodes (14): DownloadError, Raised when a resource download fails., Simple file-based cache for downloaded resources., Resource downloader with caching and checksum verification., IO layer: downloading, caching, checksums, and storage., ensure_dir(), Path, Local filesystem helpers for saving downloaded resources. (+6 more)

### Community 96 - "geojson.py"
Cohesion: 0.23
Nodes (11): _batch_from_rows(), _fmt_coord(), GeoJSONReader, Any, Streaming GeoJSON reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-11).…, Format a coordinate value: integers without trailing ``.0``; floats as-is., Stream a GeoJSON ``BinaryIO`` source into Arrow ``RecordBatch`` objects. Each…, Yield ``RecordBatch`` objects by flattening GeoJSON Features. Args: source: A… (+3 more)

### Community 98 - "CatalogCapabilities"
Cohesion: 0.40
Nodes (3): CatalogCapabilities, CatalogCapabilities model — query-field-level capability contract (D-07)., Capabilities a catalog connector advertises to the runtime. Phase 5's reject…

### Community 106 - "Downloader"
Cohesion: 0.14
Nodes (11): Downloader, Path, Download multiple *resources* into *dest*., Downloads resources to local or pluggable storage. Args: transport: HTTP client…, Download a single *resource* and return the local file path. Args: resource:…, ABC, Abstract storage backend., Persist *data* under *key* and return the storage URI/path. (+3 more)

### Community 115 - "IterableBytesIO"
Cohesion: 0.10
Nodes (12): IO, RawIOBase, IterableBytesIO, WriteableBuffer, IterableBytesIO — adapt an ``Iterable[bytes]`` into a non-seekable BinaryIO…, Mark the stream closed and exhaust the iterator., Wrap an ``Iterable[bytes]`` into a non-seekable ``BinaryIO``. The canonical use…, Read up to ``len(b)`` bytes into the provided buffer. Args: b: A writable… (+4 more)

### Community 117 - "session.py"
Cohesion: 0.15
Nodes (15): DataPlaneResourceReader, Concrete ``ResourceReader`` implementing access-kind dispatch (DATA-04). Args:…, PortalDetectionError, Raised when the portal type cannot be auto-detected. Attributes:…, DataSluiceSession, Any, DataSluiceSession — internal composition substrate (ARCH-03). The session wires…, Resolve and construct a connector for *url*. Auto-detects the portal type via… (+7 more)

### Community 119 - ".read_batches"
Cohesion: 0.25
Nodes (6): Any, Read *source* and yield Arrow ``RecordBatch`` objects. Args: source: A binary…, _batch_from_rows(), Any, Yield ``RecordBatch`` objects by chunking openpyxl ``iter_rows``. Args: source:…, Build a single ``RecordBatch`` from a chunk of row dicts.

### Community 125 - "StreamResponse"
Cohesion: 0.17
Nodes (8): ConditionalFetchResult, ConditionalTransport, Transport port Protocols for HTTP-like request execution. Three narrow runtime-…, Result of a conditional resource fetch (D-P7-15/D-P7-16). A 304 Not Modified…, Optional transport capability for ETag/Last-Modified conditional GETs…, Backend-agnostic streaming response wrapper (D-P3-07). Iterable for byte chunks…, Release the underlying httpx response., StreamResponse

### Community 126 - "auth/__init__.py"
Cohesion: 0.22
Nodes (5): APIKeyAuth, Any, API-key authentication strategy. Supports passing the key via a header (default…, Authenticate requests using an API key. Args: api_key: The API key value.…, Authentication strategies for DataSluice.

### Community 130 - "discovery/detector.py"
Cohesion: 0.11
Nodes (19): detect_portal(), Detect a portal through caller-supplied infrastructure., Run injected portal detection without facade-specific mapping., Detect a portal through the session's injected infrastructure., detect(), _normalize_base_url(), Evidence-based portal type detection (D-P5-15/16/17/18). The detector probes…, Ensure *url* has a scheme and no trailing slash. (+11 more)

## Knowledge Gaps
- **21 isolated node(s):** `Funding Config (Buy Me a Coffee: nitishraj)`, `Bug Report Issue Template`, `Issue Template Config (blank issues enabled)`, `Feature Request Issue Template`, `CI Lint & format job (ruff)` (+16 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **10 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `DataSluiceError` connect `DataSluiceError` to `sync.py`, `sync/materialize.py`, `data/access.py`, `Resource`, `FormatError`, `resolve_one_resource`, `sync/state_store.py`, `_identity.py`, `artifact.py`, `socrata/adapter.py`, `detect.py`, `resource_locator_from_dict`, `app.py`, `BatchStream`, `render_json`, `search.py`, `PortalError`, `.iter_batches_with_cursors`, `DataSluice`, `inspect.py`, `_SingleStreamReader`, `io/__init__.py`, `session.py`?**
  _High betweenness centrality (0.101) - this node is a cross-community bridge._
- **Why does `Resource` connect `Resource` to `sync/materialize.py`, `ckan/adapter.py`, `data/access.py`, `BatchStream`, `resolve_one_resource`, `Downloader`, `_identity.py`, `socrata/adapter.py`, `DataSluiceError`, `.open`, `Schema`, `ResourceAccess`, `Query`, `domain/__init__.py`, `io/__init__.py`, `SearchResult`?**
  _High betweenness centrality (0.055) - this node is a cross-community bridge._
- **Why does `BaseAuth` connect `BaseAuth` to `.__init__`, `host_provider.py`, `ConnectorContext`, `BasicAuth`, `session.py`, `BearerAuth`, `HeadersAuth`, `httpx_transport.py`, `Query`, `.apply`, `redirect.py`, `auth/__init__.py`?**
  _High betweenness centrality (0.039) - this node is a cross-community bridge._
- **Are the 18 inferred relationships involving `DataSluiceError` (e.g. with `_ApplicationServices` and `CatalogResourceLocator`) actually correct?**
  _`DataSluiceError` has 18 INFERRED edges - model-reasoned connections that need verification._
- **Are the 4 inferred relationships involving `Resource` (e.g. with `Dataset` and `License`) actually correct?**
  _`Resource` has 4 INFERRED edges - model-reasoned connections that need verification._
- **Are the 8 inferred relationships involving `BatchStream` (e.g. with `DataPlaneResourceReader` and `_StreamClosingBytesIO`) actually correct?**
  _`BatchStream` has 8 INFERRED edges - model-reasoned connections that need verification._
- **Are the 7 inferred relationships involving `DataSluice` (e.g. with `DataPlaneResourceReader` and `DataSluiceError`) actually correct?**
  _`DataSluice` has 7 INFERRED edges - model-reasoned connections that need verification._
