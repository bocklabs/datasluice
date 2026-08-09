# Graph Report - datasluice  (2026-08-09)

## Corpus Check
- 130 files · ~43,775 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1590 nodes · 3604 edges · 94 communities (85 shown, 9 thin omitted)
- Extraction: 94% EXTRACTED · 6% INFERRED · 0% AMBIGUOUS · INFERRED: 218 edges (avg confidence: 0.58)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `e07374d1`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- FileStateStore
- DataSluiceError
- sync/materialize.py
- ContentCache
- host_provider.py
- DataPlaneResourceReader
- DataSluice (Public API class)
- Any
- AtomicStateStore
- BaseFormatReader
- discovery/__init__.py
- sync/state_store.py
- _identity.py
- TransformContext
- artifact.py
- socrata/adapter.py
- Resource
- compression.py
- SyncState
- detect.py
- ParquetReader
- .redirect_request
- BaseAdapter
- resource_locator_from_dict
- CI Workflow
- FormatError
- FileCache
- BatchStream
- HttpClient
- dlt.py
- HttpxTransport
- DownloadError
- PluginManager
- CachePort
- Query
- ckan/adapter.py
- StoragePort
- open_filesystem
- DataSluice
- ChecksumMismatchError
- InMemoryStateStore
- load_fixture
- ports/__init__.py
- search.py
- catalog.py
- PortalError
- LocalStorage
- transport/pagination.py
- main
- Datasluice Brand Logo (no background)
- BasicAuth
- data/schema.py
- Any
- domain/__init__.py
- BearerAuth
- HeadersAuth
- logical_sha256
- ._decode_envelope
- .apply
- __getattr__
- Pre-commit Configuration
- exceptions.py
- httpx_transport.py
- Dataset
- SECURITY.md
- OpenCodeReview PR Review Workflow (alibaba open-code-review)
- Formats Layer (datasluice.formats)
- ConnectorContext
- Defense-in-depth CI security scanning pattern
- Datasluice Brand Identity
- Pull Request Template (affected areas + AI provenance)
- connectors/__init__.py
- integrations/__init__.py
- Funding Config (Buy Me a Coffee: nitishraj)
- Issue Template Config (blank issues enabled)
- session.py
- DataGouvPage
- datagouv/adapter.py
- io/__init__.py
- geojson.py
- CatalogCapabilities
- config/defaults.py
- Downloader
- IterableBytesIO
- CSVReader
- DataSluiceSession
- .read_batches
- configure_logging
- .to_dict
- StreamResponse
- APIKeyAuth
- DetectionResult
- .read_batches

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
- `DirectResourceLocator` --uses--> `DataSluiceError`  [INFERRED]
  src/datasluice/application.py → src/datasluice/exceptions.py
- `DirectResourceLocator` --uses--> `OpenedResourceConsumedError`  [INFERRED]
  src/datasluice/application.py → src/datasluice/exceptions.py
- `DirectResourceLocator` --uses--> `Downloader`  [INFERRED]
  src/datasluice/application.py → src/datasluice/io/downloader.py

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

## Communities (94 total, 9 thin omitted)

### Community 0 - "FileStateStore"
Cohesion: 0.11
Nodes (14): RLock, FileStateStore, Whether this backend's ``mv`` is a true atomic rename (CR-11)., Return the process-global lock scope for *key* on this store (CR-02). Two…, Hold the per-key lock for *key* so callers serialize a multi-step transaction…, Acquire (lazily creating) the process-global per-key lock, tracking users…, Return the SHA-256-hexdigest (.json) path for *key* (T-07-03 mitigation)., Load the :class:`SyncState` for *key*, or ``None`` if absent. Raises:… (+6 more)

### Community 1 - "DataSluiceError"
Cohesion: 0.09
Nodes (39): Exception, BatchCursor, ParquetRowGroupPosition, Yield batches with the closed cursor for the next unread row group. In…, Logical position at the next unread Parquet row group., Closed continuation cursor for the next unread batch. ``next_batch_index``…, ConfigError, DataSluiceError (+31 more)

### Community 2 - "sync/materialize.py"
Cohesion: 0.15
Nodes (33): Artifact, A strict, immutable schema-v1 materialization envelope., canonical_identity(), Return the SHA-256 canonical identity for *resource*. The identity is…, _artifact(), _atomic_pipe(), _batch_shard_uri(), _blob_digest_from_fs() (+25 more)

### Community 3 - "ContentCache"
Cohesion: 0.13
Nodes (15): Connection, ContentCache, Any, Return the SHA-256 hexdigest of *key* (SEC-06 carry-forward, D-P3-09)., Return the absolute content-file path for a SHA-256 digest., Return cached bytes for *key*, or ``None`` on miss / expiry / writing., Store *data* under *key* (CachePort signature UNCHANGED, D-P3-12)., Store *data* under *key* with ETag/Last-Modified sidecar (D-P3-12). Phase 4's… (+7 more)

### Community 4 - "host_provider.py"
Cohesion: 0.14
Nodes (13): Lock, Refresher, HostCredentialProvider, datetime, Host-scoped credential resolver with single-flight refresh (INFRA-04).…, Drop the cached credential for *host* (off-port; D-P3-15). Called by…, Resolve a :class:`BaseAuth` per host with cached expiry and single-flight…, Return the per-host lock, creating it if necessary. The dict-level lock is held… (+5 more)

### Community 5 - "DataPlaneResourceReader"
Cohesion: 0.13
Nodes (23): _chain(), _close_source(), _content_encoding_from_headers(), DataPlaneResourceReader, Any, Concrete ``ResourceReader`` implementation with access-kind dispatch (DATA-04,…, Open *resource* as a :class:`BatchStream` of Arrow ``RecordBatch``. Dispatches…, Open an already-fetched streaming response through the data plane. (+15 more)

### Community 6 - "DataSluice (Public API class)"
Cohesion: 0.05
Nodes (45): Adapter isolation principle, Adapter 4-module pattern (adapter/mapper/pagination/errors), Adapters Layer (datasluice.adapters), Auth Layer (datasluice.auth), BaseAdapter protocol, CKAN adapter, CKAN portal platform, Composable transport (decorators) principle (+37 more)

### Community 7 - "Any"
Cohesion: 0.09
Nodes (20): OpenedResource, Any, Open one resource through the injected data-plane reader., Apply a reusable transform pipeline to an existing stream., Materialize one resource through the application operation., Materialize one Resource or ResourceLocator into an Artifact., Lazy, single-use application wrapper over a Resource reader., Whether the underlying data stream is currently open. (+12 more)

### Community 8 - "AtomicStateStore"
Cohesion: 0.13
Nodes (9): AtomicStateStore, Protocol, State store port Protocols for incremental sync state (SYNC-01). The base…, Boundary protocol for persisting incremental sync state., Additive capability Protocol for compare-and-swap (CAS) state writes…, Return the raw envelope bytes for *key*, or ``None`` if absent. The returned…, Atomically load ``(state, version)`` from one backend read (CR-01). Returns…, Persist *state* under *key* only if the current version matches… (+1 more)

### Community 9 - "BaseFormatReader"
Cohesion: 0.18
Nodes (14): BaseFormatReader, ABC, Abstract base class for streaming format readers (D-P4-10). Each reader…, Protocol for decoding a single file format into Arrow ``RecordBatch`` objects.…, Streaming CSV reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-10).…, GeoJSONReader, Stream a GeoJSON ``BinaryIO`` source into Arrow ``RecordBatch`` objects. Each…, get_reader() (+6 more)

### Community 10 - "discovery/__init__.py"
Cohesion: 0.22
Nodes (7): Portal type fingerprints for auto-detection. Each entry maps a fingerprint (URL…, Portal type discovery and auto-detection., PortalMetadata, Portal metadata describing known portal instances., Metadata about a detected or known portal. Attributes: portal_type: Canonical…, DetectionEvidence, A single piece of evidence produced by a detection check. Attributes: check:…

### Community 11 - "sync/state_store.py"
Cohesion: 0.33
Nodes (17): Raised when a state store cannot read or write durable sync state (D-P7-26). A…, StateStoreError, _contains_secret_material(), _is_completed_watermark(), _is_safe_destination_uri(), _is_sha256(), _is_source_version(), Any (+9 more)

### Community 12 - "_identity.py"
Cohesion: 0.20
Nodes (9): canonical_destination_identity(), Canonical resource identity (CR-01 blocker fix, SYNC-05/07). Portal-controlled…, Extract the origin scope for canonical identity hashing. HTTP(S) resources…, Return a secret-free SHA-256 identity for a destination URI., Reject duplicate canonical identities before any artifact or state write.…, _url_origin(), validate_unique_identities(), _completed_artifact_record() (+1 more)

### Community 13 - "TransformContext"
Cohesion: 0.06
Nodes (43): Raised when a transform step cannot be applied (D-P6-15). Transform failures…, TransformError, __getattr__(), Composable transform pipeline package (TRANS-01..09). Re-exports are resolved…, Lazily export transform symbols (mirrors datasluice.data.__getattr__).…, _build_batch_stream(), _chain(), compose() (+35 more)

### Community 14 - "artifact.py"
Cohesion: 0.16
Nodes (15): ArtifactProvenance, _contract_error(), Digest, _freeze_extensions(), _freeze_json(), _is_sha256(), _object_dict(), _public_uri() (+7 more)

### Community 15 - "socrata/adapter.py"
Cohesion: 0.08
Nodes (26): _is_set(), Pre-flight reject policy for unsupported ``Query`` filter fields (D-P5-06). The…, Raise ``UnsupportedQueryFieldError`` if *query* uses a field not in…, Return ``True`` when *value* counts as a set filter field. ``None``, empty…, _reject_unsupported_fields(), Socrata adapter implementation. Communicates with the Socrata Discovery API and…, Fetch a dataset (view) by its 4x4 identifier., Return resources for *dataset_id*. (+18 more)

### Community 16 - "Resource"
Cohesion: 0.07
Nodes (41): _ApplicationServices, CatalogResourceLocator, DirectResourceLocator, _locator_from_resource(), materialize(), open_resource(), Portal, ResourceLocator (+33 more)

### Community 17 - "compression.py"
Cohesion: 0.10
Nodes (20): BaseException, apply_compression(), _detect_format(), _ErrorTranslatingReader, PeekableReader, Any, WriteableBuffer, Transparent decompression decorator pipeline (DATA-06, D-P4-12). Sits BETWEEN… (+12 more)

### Community 18 - "SyncState"
Cohesion: 0.21
Nodes (10): SyncState model for incremental sync cursors and watermarks (SYNC-03)., Incremental synchronization state for a resource or connector. Attributes:…, SyncState, Raised when a state write loses an optimistic compare-and-swap race (D-P7-27).…, SyncStateConflictError, _encode_state(), Persist *state* under *key* via an atomic, optionally CAS-protected write.…, Persist *state* under *key* only if the current version matches… (+2 more)

### Community 19 - "detect.py"
Cohesion: 0.19
Nodes (13): detect(), _detection_json(), Any, Argument, help, Option, ``datasluice detect`` command — evidence-based portal detection (D-P5-21, D-07)., Serialize one public DetectionResult into a JSON-safe envelope. (+5 more)

### Community 20 - "ParquetReader"
Cohesion: 0.27
Nodes (8): ParquetReader, Any, Read one complete Parquet row group as one RecordBatch., Return ``source.seekable()`` if available; ``False`` on any error., Stream a Parquet ``BinaryIO`` source into Arrow ``RecordBatch`` objects. On a…, Yield ``RecordBatch`` objects by streaming Parquet row groups. Args: source: A…, Yield ``(row_group_index, batch)`` tuples for each non-empty row group. Each…, _safe_seekable()

### Community 21 - ".redirect_request"
Cohesion: 0.22
Nodes (8): HTTPMessage, ParseResult, _effective_port(), Request, Normalize an explicit port against the scheme default (None when default)., Return True when both URLs share hostname (case-insensitive) and effective port., Return the follow-up request, stripping sensitive headers when required., _same_origin()

### Community 22 - "BaseAdapter"
Cohesion: 0.11
Nodes (28): BaseAdapter, Protocol that every portal adapter must implement. Subclasses translate portal-…, Return all downloadable resources for *dataset_id*., _check_dataset_ids_stable(), _check_get_dataset_returns_dataset_with_resources(), _check_isinstance_searchable_catalog(), _check_pagination_no_duplicates(), _check_publishes_catalog_capabilities() (+20 more)

### Community 23 - "resource_locator_from_dict"
Cohesion: 0.31
Nodes (7): _contract_error(), _object_dict(), Decode one strict catalog locator envelope., Decode one strict, tagged ResourceLocator envelope., Decode one strict direct locator envelope., resource_locator_from_dict(), _validate_uri()

### Community 24 - "CI Workflow"
Cohesion: 0.17
Nodes (18): Conventional Commits Release Pipeline (release-please -> publish), uv package manager (used across all workflows), CI Workflow, CI All checks pass gate (alls-green), CI Build & validate package job (uv build + twine), CI Coverage job (combine + report), CI Lint & format job (ruff), CI Smoke test job (install wheel, import datasluice) (+10 more)

### Community 25 - "FormatError"
Cohesion: 0.30
Nodes (8): _first_non_whitespace_byte(), JSONReader, Any, Streaming JSON reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-10).…, Stream a JSON / JSONL ``BinaryIO`` source into Arrow ``RecordBatch`` objects., Yield ``RecordBatch`` objects from a JSON array or JSONL source. Args: source:…, FormatError, Raised when a resource cannot be parsed in the expected format.

### Community 26 - "FileCache"
Cohesion: 0.22
Nodes (7): FileCache, Path, A time-based file cache. Args: cache_dir: Directory to store cached files. ttl:…, Return cached bytes for *key*, or ``None`` if missing/expired., Store *data* under *key* and return the cache file path., Return ``True`` if *key* is cached and not expired., Remove all entries from the cache.

### Community 27 - "BatchStream"
Cohesion: 0.06
Nodes (34): BatchStream, Any, BatchStream — context-managed Arrow RecordBatch stream (DATA-01, DATA-02,…, Release the underlying reader and any owned closeables; idempotent (WR-02).…, Return a PyCapsule for zero-copy Arrow interop (Phase 6 terminals). Delegates…, Context-managed Arrow RecordBatch stream. Wraps a ``pa.RecordBatchReader``…, The pa.Schema for batches yielded by this stream., Yield Arrow ``RecordBatch`` objects from the wrapped source. When ``indexed``… (+26 more)

### Community 28 - "HttpClient"
Cohesion: 0.23
Nodes (9): HttpClient, Any, Perform an HTTP request and return the raw response body. Raises: PortalError:…, GET *url* and return the response as text., GET *url* and return the response as parsed JSON., GET *url* and return the raw bytes (for file downloads)., Render *body* as text, truncating to *limit* characters., Thin HTTP client wrapping :mod:`urllib` with auth, retry, and rate-limiting.… (+1 more)

### Community 29 - "dlt.py"
Cohesion: 0.25
Nodes (8): datasluice_source(), mirror_dlt_state(), Any, dlt (data load tool) integration: use DataSluice as a dlt source. Requires…, Mirror dlt's load-committed per-resource watermarks into a DataSluice…, Return a deterministic destination-safe name for a resource ID., Return a dlt source yielding one Arrow-backed table per resource. Args: portal:…, _sanitize()

### Community 30 - "HttpxTransport"
Cohesion: 0.13
Nodes (19): Response, Raised on HTTP 5xx responses, or transport-level failures, that should be…, RetryableHTTPError, _parse_retry_after(), Parse a ``Retry-After`` header into a delay in seconds. Supports both delta-…, HttpxTransport, Request, Yield a :class:`StreamResponse` and deterministically close its response. (+11 more)

### Community 31 - "DownloadError"
Cohesion: 0.14
Nodes (13): DownloadError, Raised when a resource download fails., FsspecStorage, _has_parent_segments(), Any, fsspec-backed storage adapter satisfying :class:`StoragePort` (INFRA-02,…, Adapter wrapping an fsspec ``AbstractFileSystem`` to satisfy ``StoragePort``…, Persist *data* under *path* and return the resulting URI string. Args: data:… (+5 more)

### Community 32 - "PluginManager"
Cohesion: 0.15
Nodes (9): PluginFailure, PluginManager, Any, Record of a failed plugin discovery or load. Attributes: name: Entry-point name…, Registry-free connector manager backed by ``importlib.metadata``. Built-in…, Register *factory* programmatically (used by tests, D-06)., Return the factory callable for *name*. Raises: AdapterNotFoundError: If no…, Return a sorted list of all registered connector names. (+1 more)

### Community 33 - "CachePort"
Cohesion: 0.25
Nodes (4): CachePort, Protocol, Cache port Protocol for content-addressed byte caching., Boundary protocol for a simple key/byte cache.

### Community 34 - "Query"
Cohesion: 0.15
Nodes (12): Search one portal through injected session dependencies., Search through the injected composition substrate., Search through the facade without exposing a connector., Search one portal through the session substrate., search_datasets(), Search for datasets matching *query*., Query, Query model for searching datasets across portals. (+4 more)

### Community 35 - "ckan/adapter.py"
Cohesion: 0.08
Nodes (23): ABC, Abstract base class for all portal adapters., CKANAdapter, CKAN adapter implementation. Communicates with the CKAN Action API…, Fetch a dataset via ``package_show``., Return resources for *dataset_id*., Fetch organization metadata via ``organization_show``., Adapter for CKAN-powered open-data portals. Uses the CKAN Action API at… (+15 more)

### Community 36 - "StoragePort"
Cohesion: 0.25
Nodes (4): Protocol, Storage port Protocol returning URI references (CORR-05)., Boundary protocol for byte storage addressed by path/URI strings., StoragePort

### Community 37 - "open_filesystem"
Cohesion: 0.22
Nodes (10): AbstractFileSystem, open_filesystem(), Any, Centralised filesystem factory (INFRA-05). All fsspec backend instantiation…, Instantiate the correct fsspec backend for *uri* (INFRA-05, D-P3-20). Delegates…, Best-effort removal of *path* on *fs*; ignore absence and secondary OSError.…, safe_remove(), __getattr__() (+2 more)

### Community 38 - "DataSluice"
Cohesion: 0.04
Nodes (67): ParsedLocator, DataSluice, Canonical public facade for discovery, resource access, and materialization., Close this facade and any resource wrappers it owns., Main Typer application for the DataSluice CLI., download(), Argument, help (+59 more)

### Community 39 - "ChecksumMismatchError"
Cohesion: 0.24
Nodes (12): ChecksumMismatchError, Raised when a downloaded file's checksum does not match., compute_hash(), compute_md5(), compute_sha256(), Path, Checksum (hash) computation and verification., Compute the hex digest of *file_path* using *algorithm*. (+4 more)

### Community 40 - "InMemoryStateStore"
Cohesion: 0.29
Nodes (4): InMemoryStateStore, Ephemeral in-process :class:`StateStore` backed by a plain dict (D-P7-02).…, Store *state* under *key* (last-writer-wins; ephemeral)., Remove *key* if present; a missing key is tolerated.

### Community 41 - "load_fixture"
Cohesion: 0.36
Nodes (7): load_fixture(), load_fixture_set(), Any, Path, Fixture loading helpers for the conformance suite. Hand-authored portal-…, Load a single hand-authored portal-response fixture from *path*. Args: path:…, Load a keyed fixture set: ``{fixture_name: parsed fixture JSON}``.

### Community 42 - "ports/__init__.py"
Cohesion: 0.13
Nodes (11): AbstractContextManager, Lazily initialised HTTP transport., Port Protocol interfaces for DataSluice — unstable boundary contracts., ConditionalTransport, Any, Protocol, Transport boundary Protocol satisfied structurally by HTTP clients., Streaming transport boundary Protocol (D-P3-06/D-P3-07). `stream(url)` returns… (+3 more)

### Community 43 - "search.py"
Cohesion: 0.20
Nodes (13): _dataset_json(), Any, Argument, help, Option, ``datasluice search`` command., Serialize one catalog dataset into a JSON-safe summary., Build one machine-readable search result envelope. (+5 more)

### Community 44 - "catalog.py"
Cohesion: 0.25
Nodes (8): CatalogPort, OrganizationCatalog, Protocol, Catalog port Protocols for portal catalog connectors., Marker base protocol all catalog connectors share. Attributes: portal_type:…, Capability protocol for dataset search., Capability protocol for organization lookup., SearchableCatalog

### Community 45 - "PortalError"
Cohesion: 0.21
Nodes (15): map_ckan_error(), CKAN-specific error mapping., Map a CKAN HTTP error response to a DataSluice exception. Args: status_code:…, map_datagouv_error(), data.gouv.fr (udata) specific error mapping., Map a udata HTTP error response to a DataSluice exception., map_socrata_error(), Socrata-specific error mapping. (+7 more)

### Community 46 - "LocalStorage"
Cohesion: 0.33
Nodes (4): LocalStorage, Path, Local-filesystem storage backend. Args: base_dir: Root directory for stored…, Resolve *key* against the base directory, rejecting path traversal. A ``key``…

### Community 47 - "transport/pagination.py"
Cohesion: 0.29
Nodes (6): paginate(), PaginationConfig, T, Generic pagination helpers shared across adapters., Common pagination parameters. Attributes: page_size: Number of items per page.…, Lazily yield pages of results. Args: fetch_page: Callable taking…

### Community 48 - "main"
Cohesion: 0.33
Nodes (6): callback, is_eager, main(), help, Option, DataSluice — unified open-data toolkit.

### Community 49 - "Datasluice Brand Logo (no background)"
Cohesion: 0.29
Nodes (10): DataSluice Logo (Brand Mark), Datasluice Brand Logo (no background), Circuit Pathways Connectivity Element, Code Brackets (</>) Programming Icon, Funnel Processing Element, Filtered Output Basin, Padlock Security Icon, Table/Grid Tech Icon (+2 more)

### Community 50 - "BasicAuth"
Cohesion: 0.33
Nodes (3): BasicAuth, Any, Authenticate requests using HTTP Basic credentials. Args: username: Basic-auth…

### Community 51 - "data/schema.py"
Cohesion: 0.28
Nodes (8): Any, Domain Schema → Arrow Schema mapper and batch unification helper. The…, Derive a ``pa.Schema`` from a domain :class:`Schema` for display. Maps known…, Concatenate ``RecordBatch`` objects under a unified ``pa.Schema`` (DATA-08,…, to_arrow_schema(), unify_batches(), Raised when batch schemas cannot be unified under pyarrow promotion (D-P4-21).…, SchemaUnificationError

### Community 52 - "Any"
Cohesion: 0.18
Nodes (10): _default_port(), _effective_origin(), _host_credential_provider_type(), Any, Perform an HTTP request and return the raw response body. Raises: PortalError:…, GET *url* and return the response as parsed JSON (non-dicts wrapped under…, GET *url* and return the raw bytes (for file downloads)., Return the IANA default port for *scheme*, or ``None`` when unknown (CR-06). (+2 more)

### Community 53 - "domain/__init__.py"
Cohesion: 0.07
Nodes (45): _coerce_int(), map_license(), map_resource(), Any, Mapping functions to convert CKAN-native JSON into domain models., Best-effort coerce *value* to ``int``; return ``None`` on failure., Convert a CKAN license dict into a :class:`License`., Resolve the resource access descriptor per D-P5-02. HttpDownload wins when… (+37 more)

### Community 54 - "BearerAuth"
Cohesion: 0.33
Nodes (3): BearerAuth, Any, Authenticate requests using a bearer token in the ``Authorization`` header.…

### Community 55 - "HeadersAuth"
Cohesion: 0.33
Nodes (3): HeadersAuth, Any, Authenticate requests using arbitrary static headers. Args: headers: A mapping…

### Community 56 - "logical_sha256"
Cohesion: 0.48
Nodes (6): _encode(), logical_sha256(), Any, Serialization-stable logical hashing for Arrow tables., Return a SHA-256 digest over an Arrow table's schema and logical rows., _schema_fingerprint()

### Community 57 - "._decode_envelope"
Cohesion: 0.60
Nodes (4): _decode_completed_cursor(), _decode_legacy_state(), _mapping_field(), Return the :class:`SyncState` for *key*, or ``None`` if absent.

### Community 60 - "Pre-commit Configuration"
Cohesion: 0.50
Nodes (5): pre-commit-hooks (basic hooks), pytest testing framework, Ruff linter and formatter, ty type checker (Astral), Pre-commit Configuration

### Community 61 - "exceptions.py"
Cohesion: 0.18
Nodes (12): Logger, Evidence-based portal type detection (D-P5-15/16/17/18). The detector probes…, AdapterError, AdapterNotFoundError, Exception hierarchy for DataSluice., Raised when an adapter cannot fulfil a request., Raised when no adapter is registered for a portal type., Content-addressed cache backed by a SQLite WAL index + content files… (+4 more)

### Community 62 - "httpx_transport.py"
Cohesion: 0.09
Nodes (27): CredentialScope, Credential scoping model for host-bound credential policy., Host-scoped policy controlling where credentials may be sent. Attributes:…, HTTP client with retry, rate-limiting, and authentication support., httpx-backed HTTP transport satisfying the Transport + StreamingTransport…, __getattr__(), Transport layer: HTTP client, retry, rate-limiting, and pagination.…, Lazily resolve httpx-backed symbols on first attribute access (PEP 562). (+19 more)

### Community 63 - "Dataset"
Cohesion: 0.29
Nodes (4): Fetch a single dataset by its portal-native *dataset_id*., Dataset, A dataset is a logical grouping of one or more resources. Attributes: id:…, Result container for paginated search responses.

### Community 65 - "SECURITY.md"
Cohesion: 0.67
Nodes (3): CodeQL (security scanning), Dependabot (dependency updates), Zizmor (workflow audit)

### Community 66 - "OpenCodeReview PR Review Workflow (alibaba open-code-review)"
Cohesion: 0.83
Nodes (4): AI-assisted PR automation pattern, Renovate bot exclusion pattern (skip AI review for bots), OpenCodeReview PR Review Workflow (alibaba open-code-review), PR Agent Workflow (/describe auto-generate title & description)

### Community 67 - "Formats Layer (datasluice.formats)"
Cohesion: 0.50
Nodes (4): Formats Layer (datasluice.formats), Integrations Layer (datasluice.integrations), IO Layer (datasluice.io), Lazy imports of optional deps principle

### Community 70 - "ConnectorContext"
Cohesion: 0.20
Nodes (10): create_datagouv_connector(), Factory for the data.gouv.fr connector (entry-point target).…, Construct a :class:`DataGouvAdapter` wired to the context's transport/auth., create_socrata_connector(), Factory for the Socrata connector (entry-point target).…, Construct a :class:`SocrataAdapter` wired to the context's transport/auth., ConnectorContext, Connector construction context carrying injected infra ports. (+2 more)

### Community 73 - "Defense-in-depth CI security scanning pattern"
Cohesion: 1.00
Nodes (3): Defense-in-depth CI security scanning pattern, CodeQL Workflow (actions + python security analysis), Zizmor Workflow Security Analysis

### Community 74 - "Datasluice Brand Identity"
Cohesion: 1.00
Nodes (3): Datasluice Brand Identity, Datasluice Logo / Brand Mark, Datasluice Documentation Logo Asset

### Community 75 - "Pull Request Template (affected areas + AI provenance)"
Cohesion: 0.67
Nodes (3): Bug Report Issue Template, Feature Request Issue Template, Pull Request Template (affected areas + AI provenance)

### Community 82 - "session.py"
Cohesion: 0.11
Nodes (21): API-key authentication strategy. Supports passing the key via a header (default…, BaseAuth, ABC, Abstract base for authentication strategies., Protocol for pluggable authentication. Each strategy knows how to decorate a…, HTTP Basic authentication strategy., Bearer-token (OAuth 2.0 / JWT) authentication strategy., Custom-headers authentication strategy. Useful for portals that expect a non-… (+13 more)

### Community 90 - "DataGouvPage"
Cohesion: 0.25
Nodes (5): DataGouvPage, Pagination helpers for the data.gouv.fr (udata) API. udata uses page-number /…, Parameters for a single udata results page (1-based)., Return the parameters for the following page., Convert to query-string parameters for the udata API.

### Community 91 - "datagouv/adapter.py"
Cohesion: 0.10
Nodes (25): DataGouvAdapter, data.gouv.fr (udata) adapter implementation. Communicates with the udata REST…, Fetch organization metadata via ``/organizations/{slug}/``., Adapter for data.gouv.fr and other udata-powered portals. Uses the udata REST…, Call a udata API endpoint and return the parsed JSON., Search datasets via ``/datasets/``. Translates every set supported ``Query``…, Fetch a dataset via ``/datasets/{id}/``., Return resources for *dataset_id*. (+17 more)

### Community 95 - "io/__init__.py"
Cohesion: 0.22
Nodes (12): Simple file-based cache for downloaded resources., Resource downloader with caching and checksum verification., IO layer: downloading, caching, checksums, and storage., ensure_dir(), Path, Local filesystem helpers for saving downloaded resources., Create *path* (and parents) if it does not exist; return as :class:`Path`., Write *data* to *dest* / *filename* and return the file path. Raises:… (+4 more)

### Community 96 - "geojson.py"
Cohesion: 0.27
Nodes (9): _batch_from_rows(), _fmt_coord(), Any, Streaming GeoJSON reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-11).…, Format a coordinate value: integers without trailing ``.0``; floats as-is., Yield ``RecordBatch`` objects by flattening GeoJSON Features. Args: source: A…, Build a single ``RecordBatch`` from a chunk of feature rows., Encode a GeoJSON ``geometry`` object as WKT (or fall back to raw JSON). Handles… (+1 more)

### Community 98 - "CatalogCapabilities"
Cohesion: 0.40
Nodes (3): CatalogCapabilities, CatalogCapabilities model — query-field-level capability contract (D-07)., Capabilities a catalog connector advertises to the runtime. Phase 5's reject…

### Community 106 - "Downloader"
Cohesion: 0.14
Nodes (11): Downloader, Path, Download multiple *resources* into *dest*., Downloads resources to local or pluggable storage. Args: transport: HTTP client…, Download a single *resource* and return the local file path. Args: resource:…, ABC, Abstract storage backend., Persist *data* under *key* and return the storage URI/path. (+3 more)

### Community 115 - "IterableBytesIO"
Cohesion: 0.10
Nodes (12): IO, RawIOBase, IterableBytesIO, WriteableBuffer, IterableBytesIO — adapt an ``Iterable[bytes]`` into a non-seekable BinaryIO…, Mark the stream closed and exhaust the iterator., Wrap an ``Iterable[bytes]`` into a non-seekable ``BinaryIO``. The canonical use…, Read up to ``len(b)`` bytes into the provided buffer. Args: b: A writable… (+4 more)

### Community 116 - "CSVReader"
Cohesion: 0.29
Nodes (6): CSVReader, Any, Stream a CSV ``BinaryIO`` source into Arrow ``RecordBatch`` objects. Args:…, Yield ``RecordBatch`` objects by delegating to ``pyarrow.csv.open_csv``. Args:…, Drain a ``pa.RecordBatchReader`` and re-chunk into ``batch_size``-row batches., _rechunk_reader()

### Community 117 - "DataSluiceSession"
Cohesion: 0.11
Nodes (14): detect(), _normalize_base_url(), Ensure *url* has a scheme and no trailing slash., Probe *url* for every registered portal fingerprint (D-P5-15/16/17/18).…, PortalDetectionError, Raised when the portal type cannot be auto-detected. Attributes:…, DataSluiceSession, Any (+6 more)

### Community 119 - ".read_batches"
Cohesion: 0.50
Nodes (4): _batch_from_rows(), Any, Yield ``RecordBatch`` objects by chunking openpyxl ``iter_rows``. Args: source:…, Build a single ``RecordBatch`` from a chunk of row dicts.

### Community 120 - "configure_logging"
Cohesion: 0.29
Nodes (6): LogRecord, configure_logging(), Any, Redact known sensitive keys from log records (INFRA-06, D-P3-18). Walks…, Configure the package-level logger. Args: level: Logging level (e.g.…, RedactingFilter

### Community 125 - "StreamResponse"
Cohesion: 0.20
Nodes (6): ConditionalFetchResult, Transport port Protocols for HTTP-like request execution. Three narrow runtime-…, Result of a conditional resource fetch (D-P7-15/D-P7-16). A 304 Not Modified…, Backend-agnostic streaming response wrapper (D-P3-07). Iterable for byte chunks…, Release the underlying httpx response., StreamResponse

### Community 126 - "APIKeyAuth"
Cohesion: 0.33
Nodes (3): APIKeyAuth, Any, Authenticate requests using an API key. Args: api_key: The API key value.…

### Community 130 - "DetectionResult"
Cohesion: 0.14
Nodes (11): detect_portal(), Detect a portal through caller-supplied infrastructure., Run injected portal detection without facade-specific mapping., Detect a portal through the session's injected infrastructure., DetectionResult, Detection models for evidence-based portal identification., Outcome of portal auto-detection with confidence and evidence. Attributes:…, PortalDetector (+3 more)

## Knowledge Gaps
- **21 isolated node(s):** `Funding Config (Buy Me a Coffee: nitishraj)`, `Bug Report Issue Template`, `Issue Template Config (blank issues enabled)`, `Feature Request Issue Template`, `CI Lint & format job (ruff)` (+16 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **9 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `DataSluiceError` connect `DataSluiceError` to `sync/materialize.py`, `host_provider.py`, `DataPlaneResourceReader`, `Any`, `sync/state_store.py`, `_identity.py`, `artifact.py`, `socrata/adapter.py`, `Resource`, `detect.py`, `resource_locator_from_dict`, `FormatError`, `BatchStream`, `DownloadError`, `DataSluice`, `search.py`, `PortalError`, `data/schema.py`, `domain/__init__.py`, `exceptions.py`, `DataSluiceSession`?**
  _High betweenness centrality (0.101) - this node is a cross-community bridge._
- **Why does `Resource` connect `Resource` to `sync/materialize.py`, `ckan/adapter.py`, `DataPlaneResourceReader`, `DataSluice`, `Any`, `BatchStream`, `Downloader`, `_identity.py`, `socrata/adapter.py`, `domain/__init__.py`, `BaseAdapter`, `datagouv/adapter.py`, `io/__init__.py`, `Dataset`?**
  _High betweenness centrality (0.055) - this node is a cross-community bridge._
- **Why does `BaseAuth` connect `session.py` to `ckan/adapter.py`, `host_provider.py`, `ConnectorContext`, `ports/__init__.py`, `BasicAuth`, `DataSluiceSession`, `BearerAuth`, `HeadersAuth`, `httpx_transport.py`, `.apply`, `APIKeyAuth`?**
  _High betweenness centrality (0.039) - this node is a cross-community bridge._
- **Are the 18 inferred relationships involving `DataSluiceError` (e.g. with `_ApplicationServices` and `CatalogResourceLocator`) actually correct?**
  _`DataSluiceError` has 18 INFERRED edges - model-reasoned connections that need verification._
- **Are the 4 inferred relationships involving `Resource` (e.g. with `Dataset` and `License`) actually correct?**
  _`Resource` has 4 INFERRED edges - model-reasoned connections that need verification._
- **Are the 8 inferred relationships involving `BatchStream` (e.g. with `DataPlaneResourceReader` and `_StreamClosingBytesIO`) actually correct?**
  _`BatchStream` has 8 INFERRED edges - model-reasoned connections that need verification._
- **Are the 7 inferred relationships involving `DataSluice` (e.g. with `DataPlaneResourceReader` and `DataSluiceError`) actually correct?**
  _`DataSluice` has 7 INFERRED edges - model-reasoned connections that need verification._
