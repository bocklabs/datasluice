# Graph Report - datasluice  (2026-08-05)

## Corpus Check
- 130 files · ~43,770 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1666 nodes · 2876 edges · 123 communities (81 shown, 42 thin omitted)
- Extraction: 96% EXTRACTED · 4% INFERRED · 0% AMBIGUOUS · INFERRED: 105 edges (avg confidence: 0.67)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `cbd6b558`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- sync/state_store.py
- sync.py
- ResourceLocator
- ContentCache
- host_provider.py
- data/access.py
- DataSluice (Public API class)
- Any
- SyncState
- readers/__init__.py
- discovery/detector.py
- pipeline.py
- _identity.py
- steps.py
- artifact.py
- SocrataAdapter
- _contract_error
- compression.py
- _ApplicationServices
- Argument
- BatchStream
- StoragePort
- checks.py
- application.py
- CI Workflow
- json.py
- FileCache
- to_arrow
- HttpClient
- DataSluiceError
- HttpxTransport
- FsspecStorage
- PluginManager
- batch_stream.py
- DataSluice
- .__init__
- BaseAuth
- open_filesystem
- detect
- checksums.py
- BaseOperator
- load_fixture
- Transport
- search
- ports/__init__.py
- PortalError
- download
- exceptions.py
- inspect
- Datasluice Brand Logo (no background)
- CachePort
- data/__init__.py
- _effective_origin
- domain/access.py
- Adapters Layer (datasluice.adapters)
- DataSluiceError
- _hashing.py
- .__init__
- Artifact
- LegacyArtifactRecord
- Pre-commit Configuration
- logging.py
- transport/__init__.py
- domain/__init__.py
- CredentialProvider
- SECURITY.md
- OpenCodeReview PR Review Workflow (alibaba open-code-review)
- Formats Layer (datasluice.formats)
- PluginManager
- Path
- _same_origin
- RateLimiter
- RetryPolicy
- Defense-in-depth CI security scanning pattern
- Datasluice Brand Identity
- Pull Request Template (affected areas + AI provenance)
- DetectionResult
- connectors/__init__.py
- integrations/__init__.py
- T
- Funding Config (Buy Me a Coffee: nitishraj)
- Issue Template Config (blank issues enabled)
- BaseAuth
- DataSluiceSession
- Query
- Any
- SearchResult
- CKANPage
- ckan/mapper.py
- Any
- DataGouvPage
- DataGouvAdapter
- DetectionResult
- Query
- SearchResult
- downloader.py
- geojson.py
- help
- CatalogCapabilities
- Transport
- Option
- httpx_transport.py
- SocrataPage
- Any
- config/defaults.py
- BaseAdapter
- Storage
- Dataset
- Organization
- Query
- ABC
- Path
- Resource
- SearchResult
- IterableBytesIO
- CSVReader
- xlsx.py
- _reject.py
- WriteableBuffer
- domain/credentials.py
- StreamResponse
- BaseFormatReader
- SyncState

## God Nodes (most connected - your core abstractions)
1. `BatchStream` - 36 edges
2. `OpenedResource` - 24 edges
3. `PortalError` - 23 edges
4. `BaseAuth` - 23 edges
5. `DataSluiceError` - 22 edges
6. `RateLimitError` - 20 edges
7. `HttpxTransport` - 20 edges
8. `sync_resources()` - 19 edges
9. `FileStateStore` - 19 edges
10. `BaseAdapter` - 19 edges

## Surprising Connections (you probably didn't know these)
- `DataSluice Logo (Brand Mark)` --semantically_similar_to--> `Datasluice Brand Logo (no background)`  [INFERRED] [semantically similar]
  docs/assets/datasluice-logo.png → docs/assets/datasluice-logo-nbg.png
- `Resource` --uses--> `ResourceAccess`  [INFERRED]
  src/datasluice/domain/resource.py → src/datasluice/domain/access.py
- `Digest` --uses--> `DataSluiceError`  [INFERRED]
  src/datasluice/domain/artifact.py → src/datasluice/exceptions.py
- `ArtifactProvenance` --uses--> `DataSluiceError`  [INFERRED]
  src/datasluice/domain/artifact.py → src/datasluice/exceptions.py
- `Artifact` --uses--> `DataSluiceError`  [INFERRED]
  src/datasluice/domain/artifact.py → src/datasluice/exceptions.py

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

## Communities (123 total, 42 thin omitted)

### Community 0 - "sync/state_store.py"
Cohesion: 0.07
Nodes (43): RLock, _contains_secret_material(), _decode_completed_cursor(), _decode_legacy_state(), _encode_state(), FileStateStore, InMemoryStateStore, _is_completed_watermark() (+35 more)

### Community 1 - "sync.py"
Cohesion: 0.07
Nodes (62): datetime, __getattr__(), Incremental sync primitives: state stores, sync loop, idempotent materialize.…, Lazily export pyarrow-adjacent sync primitives. ``materialize`` remains lazy so…, _artifact(), _atomic_pipe(), _batch_shard_uri(), _blob_digest_from_fs() (+54 more)

### Community 2 - "ResourceLocator"
Cohesion: 0.14
Nodes (14): ParsedLocator, ResourceLocator, open_resource(), Wrap one resolved resource for lazy, single-use consumption., Build one lazy opened-resource wrapper., Materialize one resource through the application operation., _CatalogReference, parse_locator() (+6 more)

### Community 3 - "ContentCache"
Cohesion: 0.12
Nodes (16): Connection, ContentCache, Any, Content-addressed cache backed by a SQLite WAL index + content files…, Return the SHA-256 hexdigest of *key* (SEC-06 carry-forward, D-P3-09)., Return the absolute content-file path for a SHA-256 digest., Return cached bytes for *key*, or ``None`` on miss / expiry / writing., Store *data* under *key* (CachePort signature UNCHANGED, D-P3-12). (+8 more)

### Community 4 - "host_provider.py"
Cohesion: 0.15
Nodes (10): Lock, Refresher, HostCredentialProvider, Host-scoped credential resolver with single-flight refresh (INFRA-04).…, Drop the cached credential for *host* (off-port; D-P3-15). Called by…, Resolve a :class:`BaseAuth` per host with cached expiry and single-flight…, Return the per-host lock, creating it if necessary. The dict-level lock is held…, Return whether *expires_at* has passed. ``None`` means the credential never… (+2 more)

### Community 5 - "data/access.py"
Cohesion: 0.13
Nodes (23): IterableBytesIO, Resource, _chain(), _close_source(), _content_encoding_from_headers(), DataPlaneResourceReader, Any, Concrete ``ResourceReader`` implementation with access-kind dispatch (DATA-04,… (+15 more)

### Community 6 - "DataSluice (Public API class)"
Cohesion: 0.10
Nodes (28): Conventional Commits, DataSluice (Public API class), datasluice_source (dlt source factory), DataSluiceOperator (Airflow operator), Discovery Layer (datasluice.discovery), Airflow Integration Example, CKAN Example, data.gouv.fr Example (+20 more)

### Community 7 - "Any"
Cohesion: 0.11
Nodes (17): Any, OpenedResource, Open one resource through the injected data-plane reader., Apply a reusable transform pipeline to an existing stream., Materialize one Resource or ResourceLocator into an Artifact., Lazy, single-use application wrapper over a Resource reader., Whether the underlying data stream is currently open., Attach one transform pipeline without opening the resource. (+9 more)

### Community 8 - "SyncState"
Cohesion: 0.13
Nodes (12): SyncState model for incremental sync cursors and watermarks (SYNC-03)., Incremental synchronization state for a resource or connector. Attributes:…, SyncState, AtomicStateStore, Protocol, State store port Protocols for incremental sync state (SYNC-01). The base…, Boundary protocol for persisting incremental sync state., Additive capability Protocol for compare-and-swap (CAS) state writes… (+4 more)

### Community 9 - "readers/__init__.py"
Cohesion: 0.19
Nodes (11): BaseFormatReader, ABC, Any, Abstract base class for streaming format readers (D-P4-10). Each reader…, Protocol for decoding a single file format into Arrow ``RecordBatch`` objects.…, Read *source* and yield Arrow ``RecordBatch`` objects. Args: source: A binary…, GeoJSONReader, Stream a GeoJSON ``BinaryIO`` source into Arrow ``RecordBatch`` objects. Each… (+3 more)

### Community 10 - "discovery/detector.py"
Cohesion: 0.15
Nodes (15): detect(), _normalize_base_url(), Evidence-based portal type detection (D-P5-15/16/17/18). The detector probes…, Ensure *url* has a scheme and no trailing slash., Probe *url* for every registered portal fingerprint (D-P5-15/16/17/18).…, Portal type fingerprints for auto-detection. Each entry maps a fingerprint (URL…, Portal type discovery and auto-detection., PortalMetadata (+7 more)

### Community 11 - "pipeline.py"
Cohesion: 0.12
Nodes (21): __getattr__(), Composable transform pipeline package (TRANS-01..09). Re-exports are resolved…, Lazily export transform symbols (mirrors datasluice.data.__getattr__).…, _build_batch_stream(), _chain(), compose(), Pipeline, Any (+13 more)

### Community 12 - "_identity.py"
Cohesion: 0.25
Nodes (10): canonical_destination_identity(), canonical_identity(), Resource, Canonical resource identity (CR-01 blocker fix, SYNC-05/07). Portal-controlled…, Extract the origin scope for canonical identity hashing. HTTP(S) resources…, Return the SHA-256 canonical identity for *resource*. The identity is…, Return a secret-free SHA-256 identity for a destination URI., Reject duplicate canonical identities before any artifact or state write.… (+2 more)

### Community 13 - "steps.py"
Cohesion: 0.09
Nodes (21): CastSchema, Filter, Flatten, NormalizeTimestamps, Any, Closed-set normalization transforms (TRANS-02..07, D-P6-09..13). Each transform…, Cast each batch to a target Arrow schema, strictly (TRANS-04, D-P6-10). Uses…, Yield batches cast to ``self.target_schema`` (safe=True). (+13 more)

### Community 14 - "artifact.py"
Cohesion: 0.12
Nodes (20): Artifact, ArtifactProvenance, _contract_error(), Digest, _freeze_extensions(), _freeze_json(), _is_sha256(), _object_dict() (+12 more)

### Community 15 - "SocrataAdapter"
Cohesion: 0.07
Nodes (35): BaseAdapter, Dataset, Query, Resource, SearchResult, Socrata adapter implementation. Communicates with the Socrata Discovery API and…, Fetch a dataset (view) by its 4x4 identifier., Return resources for *dataset_id*. (+27 more)

### Community 16 - "_contract_error"
Cohesion: 0.31
Nodes (7): _contract_error(), _object_dict(), Decode one strict catalog locator envelope., Decode one strict, tagged ResourceLocator envelope., Decode one strict direct locator envelope., resource_locator_from_dict(), _validate_uri()

### Community 17 - "compression.py"
Cohesion: 0.10
Nodes (18): BaseException, apply_compression(), _detect_format(), _ErrorTranslatingReader, PeekableReader, Any, Transparent decompression decorator pipeline (DATA-06, D-P4-12). Sits BETWEEN…, Wrap a ``BinaryIO`` byte source with one-chunk lookahead. Buffers peeked bytes… (+10 more)

### Community 18 - "_ApplicationServices"
Cohesion: 0.10
Nodes (13): DataSluiceError, DetectionResult, _ApplicationServices, detect_portal(), Portal, Detect a portal through caller-supplied infrastructure., Private coordinator for application operations over one composition substrate., Retrieve catalog metadata through the private session substrate. (+5 more)

### Community 20 - "BatchStream"
Cohesion: 0.11
Nodes (18): BatchCursor, BatchStream, Any, Yield batches with the closed cursor for the next unread row group. In…, Release the underlying reader and any owned closeables; idempotent (WR-02).…, Return a PyCapsule for zero-copy Arrow interop (Phase 6 terminals). Delegates…, Closed continuation cursor for the next unread batch. ``next_batch_index``…, Context-managed Arrow RecordBatch stream. Wraps a ``pa.RecordBatchReader``… (+10 more)

### Community 21 - "StoragePort"
Cohesion: 0.25
Nodes (4): Protocol, Storage port Protocol returning URI references (CORR-05)., Boundary protocol for byte storage addressed by path/URI strings., StoragePort

### Community 22 - "checks.py"
Cohesion: 0.10
Nodes (32): BaseAdapter, Protocol that every portal adapter must implement. Subclasses translate portal-…, Search for datasets matching *query*., _check_dataset_ids_stable(), _check_get_dataset_returns_dataset_with_resources(), _check_isinstance_searchable_catalog(), _check_pagination_no_duplicates(), _check_publishes_catalog_capabilities() (+24 more)

### Community 23 - "application.py"
Cohesion: 0.14
Nodes (19): CatalogResourceLocator, DirectResourceLocator, _locator_from_resource(), materialize(), Resource, Public application facade, locators, and opened-resource lifecycle., A validated, serializable catalog resource reference., Return a fresh, secret-free locator envelope. (+11 more)

### Community 24 - "CI Workflow"
Cohesion: 0.16
Nodes (19): Conventional Commits Release Pipeline (release-please -> publish), uv package manager (used across all workflows), Dependabot Config (github-actions + pip weekly), CI Workflow, CI All checks pass gate (alls-green), CI Build & validate package job (uv build + twine), CI Coverage job (combine + report), CI Lint & format job (ruff) (+11 more)

### Community 25 - "json.py"
Cohesion: 0.27
Nodes (7): _first_non_whitespace_byte(), JSONReader, Any, BaseFormatReader, Streaming JSON reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-10).…, Stream a JSON / JSONL ``BinaryIO`` source into Arrow ``RecordBatch`` objects., Yield ``RecordBatch`` objects from a JSON array or JSONL source. Args: source:…

### Community 26 - "FileCache"
Cohesion: 0.22
Nodes (7): FileCache, Path, A time-based file cache. Args: cache_dir: Directory to store cached files. ttl:…, Return cached bytes for *key*, or ``None`` if missing/expired., Store *data* under *key* and return the cache file path., Return ``True`` if *key* is cached and not expired., Remove all entries from the cache.

### Community 27 - "to_arrow"
Cohesion: 0.09
Nodes (21): BatchStream, Any, to_arrow terminal: materialize a BatchStream into a pa.Table (INTG-01,…, Materialize *stream* into a ``pa.Table`` (INTG-01, D-P6-02). The shared…, to_arrow(), datasluice_source(), mirror_dlt_state(), Any (+13 more)

### Community 28 - "HttpClient"
Cohesion: 0.19
Nodes (10): HttpClient, Any, BaseAuth, CredentialScope, RateLimiter, Perform an HTTP request and return the raw response body. Raises: PortalError:…, GET *url* and return the response as text., GET *url* and return the response as parsed JSON. (+2 more)

### Community 30 - "HttpxTransport"
Cohesion: 0.12
Nodes (20): ConditionalFetchResult, Response, _host_credential_provider_type(), HttpxTransport, Any, Yield a :class:`StreamResponse` and deterministically close its response., HTTP transport backed by httpx, satisfying Transport + StreamingTransport.…, Close the pooled HTTP client exactly once. (+12 more)

### Community 31 - "FsspecStorage"
Cohesion: 0.16
Nodes (10): FsspecStorage, _has_parent_segments(), Any, Adapter wrapping an fsspec ``AbstractFileSystem`` to satisfy ``StoragePort``…, Persist *data* under *path* and return the resulting URI string. Args: data:…, Read and return the bytes stored under *path*., Return ``True`` if *path* exists in this filesystem., Return the backend-native path for *path*. Absolute URIs pass through… (+2 more)

### Community 32 - "PluginManager"
Cohesion: 0.15
Nodes (9): PluginFailure, PluginManager, Any, Record of a failed plugin discovery or load. Attributes: name: Entry-point name…, Registry-free connector manager backed by ``importlib.metadata``. Built-in…, Register *factory* programmatically (used by tests, D-06)., Return the factory callable for *name*. Raises: AdapterNotFoundError: If no…, Return a sorted list of all registered connector names. (+1 more)

### Community 33 - "batch_stream.py"
Cohesion: 0.18
Nodes (9): ParquetRowGroupPosition, BatchStream — context-managed Arrow RecordBatch stream (DATA-01, DATA-02,…, Logical position at the next unread Parquet row group., Any, DuckDB integration (read paths removed per D-P4-18; SEC-03 utility preserved).…, Validate that *table_name* is a safe SQL identifier. Args: table_name: The…, Register *stream* as a named DuckDB relation (INTG-04, D-P6-14). No SQL string…, to_duckdb() (+1 more)

### Community 34 - "DataSluice"
Cohesion: 0.11
Nodes (16): Query, SearchResult, DataSluice, Search one portal through injected session dependencies., Search through the injected composition substrate., Search through the facade without exposing a connector., Canonical public facade for discovery, resource access, and materialization., Return a stable Portal wrapper for *url*. (+8 more)

### Community 35 - ".__init__"
Cohesion: 0.06
Nodes (29): BaseAdapter, CachePort, Dataset, Organization, CKANAdapter, CKAN adapter implementation. Communicates with the CKAN Action API…, Fetch a dataset via ``package_show``., Return resources for *dataset_id*. (+21 more)

### Community 37 - "open_filesystem"
Cohesion: 0.24
Nodes (9): AbstractFileSystem, open_filesystem(), Any, Centralised filesystem factory (INFRA-05). All fsspec backend instantiation…, Instantiate the correct fsspec backend for *uri* (INFRA-05, D-P3-20). Delegates…, Best-effort removal of *path* on *fs*; ignore absence and secondary OSError.…, safe_remove(), __getattr__() (+1 more)

### Community 38 - "detect"
Cohesion: 0.06
Nodes (35): Argument, callback, help, is_eager, Option, main(), help, Option (+27 more)

### Community 39 - "checksums.py"
Cohesion: 0.29
Nodes (10): compute_hash(), compute_md5(), compute_sha256(), Path, Checksum (hash) computation and verification., Compute the hex digest of *file_path* using *algorithm*., Return the SHA-256 hex digest of *file_path*., Return the MD5 hex digest of *file_path*. (+2 more)

### Community 41 - "load_fixture"
Cohesion: 0.36
Nodes (7): load_fixture(), load_fixture_set(), Any, Path, Fixture loading helpers for the conformance suite. Hand-authored portal-…, Load a single hand-authored portal-response fixture from *path*. Args: path:…, Load a keyed fixture set: ``{fixture_name: parsed fixture JSON}``.

### Community 42 - "Transport"
Cohesion: 0.12
Nodes (13): AbstractContextManager, Lazily initialised HTTP transport., ConditionalFetchResult, ConditionalTransport, Any, Protocol, Transport port Protocols for HTTP-like request execution. Three narrow runtime-…, Transport boundary Protocol satisfied structurally by HTTP clients. (+5 more)

### Community 43 - "search"
Cohesion: 0.20
Nodes (13): _dataset_json(), Any, Argument, help, Option, ``datasluice search`` command., Serialize one catalog dataset into a JSON-safe summary., Build one machine-readable search result envelope. (+5 more)

### Community 44 - "ports/__init__.py"
Cohesion: 0.14
Nodes (15): Organization, An organization or publisher of open-data datasets. Attributes: id: Portal-…, CatalogPort, OrganizationCatalog, Protocol, Catalog port Protocols for portal catalog connectors., Marker base protocol all catalog connectors share. Attributes: portal_type:…, Capability protocol for dataset search. (+7 more)

### Community 45 - "PortalError"
Cohesion: 0.21
Nodes (15): map_ckan_error(), CKAN-specific error mapping., Map a CKAN HTTP error response to a DataSluice exception. Args: status_code:…, map_datagouv_error(), data.gouv.fr (udata) specific error mapping., Map a udata HTTP error response to a DataSluice exception., map_socrata_error(), Socrata-specific error mapping. (+7 more)

### Community 46 - "download"
Cohesion: 0.14
Nodes (12): Path, download(), Argument, help, Option, ``datasluice download`` command — raw bulk copy (D-15)., Render raw download results to stdout., Download all resources from a dataset as raw bulk copies. (+4 more)

### Community 47 - "exceptions.py"
Cohesion: 0.11
Nodes (27): Exception, AdapterError, AdapterNotFoundError, AuthenticationError, ConfigError, DataSluiceError, OpenedResourceConsumedError, PortalDetectionError (+19 more)

### Community 48 - "inspect"
Cohesion: 0.21
Nodes (11): _dataset_json(), inspect(), Any, Argument, help, Option, ``datasluice inspect`` command., Serialize one catalog dataset into a JSON-safe metadata envelope. (+3 more)

### Community 49 - "Datasluice Brand Logo (no background)"
Cohesion: 0.29
Nodes (10): DataSluice Logo (Brand Mark), Datasluice Brand Logo (no background), Circuit Pathways Connectivity Element, Code Brackets (</>) Programming Icon, Funnel Processing Element, Filtered Output Basin, Padlock Security Icon, Table/Grid Tech Icon (+2 more)

### Community 50 - "CachePort"
Cohesion: 0.25
Nodes (4): CachePort, Protocol, Cache port Protocol for content-addressed byte caching., Boundary protocol for a simple key/byte cache.

### Community 51 - "data/__init__.py"
Cohesion: 0.17
Nodes (12): __getattr__(), Streaming data-plane package: BatchStream, byte-source adapter, schema mapper.…, Lazily export BatchStream, IterableBytesIO, to_arrow_schema,…, Any, Domain Schema → Arrow Schema mapper and batch unification helper. The…, Derive a ``pa.Schema`` from a domain :class:`Schema` for display. Maps known…, Concatenate ``RecordBatch`` objects under a unified ``pa.Schema`` (DATA-08,…, to_arrow_schema() (+4 more)

### Community 52 - "_effective_origin"
Cohesion: 0.33
Nodes (5): _default_port(), _effective_origin(), Return whether sensitive headers must be stripped on this redirect hop. Mirrors…, Return the IANA default port for *scheme*, or ``None`` when unknown (CR-06)., Return ``(scheme, hostname, effective_port)`` for a parsed URL (CR-06).…

### Community 53 - "domain/access.py"
Cohesion: 0.18
Nodes (13): HttpDownload, LocalFile, ObjectStorage, QueryAccess, ResourceAccess sum-type describing how a resource is reached., Base descriptor for how a resource is accessed. Subclasses discriminate on…, Resource fetched over HTTP(S). Attributes: url: Absolute URL to download.…, Resource stored in object storage (S3, GCS, Azure Blob). Attributes: uri:… (+5 more)

### Community 54 - "Adapters Layer (datasluice.adapters)"
Cohesion: 0.11
Nodes (17): Adapter isolation principle, Adapter 4-module pattern (adapter/mapper/pagination/errors), Adapters Layer (datasluice.adapters), Auth Layer (datasluice.auth), BaseAdapter protocol, CKAN adapter, CKAN portal platform, Composable transport (decorators) principle (+9 more)

### Community 56 - "_hashing.py"
Cohesion: 0.48
Nodes (6): _encode(), logical_sha256(), Any, Serialization-stable logical hashing for Arrow tables., Return a SHA-256 digest over an Arrow table's schema and logical rows., _schema_fingerprint()

### Community 57 - ".__init__"
Cohesion: 0.50
Nodes (3): BaseAuth, CredentialScope, RateLimiter

### Community 60 - "Pre-commit Configuration"
Cohesion: 0.50
Nodes (5): pre-commit-hooks (basic hooks), pytest testing framework, Ruff linter and formatter, ty type checker (Astral), Pre-commit Configuration

### Community 61 - "logging.py"
Cohesion: 0.10
Nodes (20): Logger, LogRecord, fsspec-backed storage adapter satisfying :class:`StoragePort` (INFRA-02,…, configure_logging(), get_logger(), Any, Structured logging utilities for DataSluice. Owns the canonical…, Return a logger for *name*, defaulting to the package logger. Args: name:… (+12 more)

### Community 62 - "transport/__init__.py"
Cohesion: 0.10
Nodes (17): __getattr__(), Transport layer: HTTP client, retry, rate-limiting, and pagination.…, Lazily resolve httpx-backed symbols on first attribute access (PEP 562)., paginate(), PaginationConfig, T, Generic pagination helpers shared across adapters., Common pagination parameters. Attributes: page_size: Number of items per page.… (+9 more)

### Community 63 - "domain/__init__.py"
Cohesion: 0.09
Nodes (20): ABC, Abstract base class for all portal adapters., Fetch a single dataset by its portal-native *dataset_id*., Return all downloadable resources for *dataset_id*., Dataset, Dataset model representing a collection of related open-data resources., A dataset is a logical grouping of one or more resources. Attributes: id:…, Portal-agnostic domain models for DataSluice. (+12 more)

### Community 65 - "SECURITY.md"
Cohesion: 0.67
Nodes (3): CodeQL (security scanning), Dependabot (dependency updates), Zizmor (workflow audit)

### Community 66 - "OpenCodeReview PR Review Workflow (alibaba open-code-review)"
Cohesion: 0.83
Nodes (4): AI-assisted PR automation pattern, Renovate bot exclusion pattern (skip AI review for bots), OpenCodeReview PR Review Workflow (alibaba open-code-review), PR Agent Workflow (/describe auto-generate title & description)

### Community 67 - "Formats Layer (datasluice.formats)"
Cohesion: 0.50
Nodes (4): Formats Layer (datasluice.formats), Integrations Layer (datasluice.integrations), IO Layer (datasluice.io), Lazy imports of optional deps principle

### Community 70 - "_same_origin"
Cohesion: 0.40
Nodes (5): ParseResult, _effective_port(), Normalize an explicit port against the scheme default (None when default)., Return True when both URLs share hostname (case-insensitive) and effective port., _same_origin()

### Community 73 - "Defense-in-depth CI security scanning pattern"
Cohesion: 1.00
Nodes (3): Defense-in-depth CI security scanning pattern, CodeQL Workflow (actions + python security analysis), Zizmor Workflow Security Analysis

### Community 74 - "Datasluice Brand Identity"
Cohesion: 1.00
Nodes (3): Datasluice Brand Identity, Datasluice Logo / Brand Mark, Datasluice Documentation Logo Asset

### Community 75 - "Pull Request Template (affected areas + AI provenance)"
Cohesion: 0.67
Nodes (3): Bug Report Issue Template, Feature Request Issue Template, Pull Request Template (affected areas + AI provenance)

### Community 82 - "BaseAuth"
Cohesion: 0.05
Nodes (32): APIKeyAuth, Any, API-key authentication strategy. Supports passing the key via a header (default…, Authenticate requests using an API key. Args: api_key: The API key value.…, BaseAuth, ABC, Any, Abstract base for authentication strategies. (+24 more)

### Community 87 - "CKANPage"
Cohesion: 0.25
Nodes (5): CKANPage, Pagination helpers for the CKAN Action API., Parameters for a single CKAN search results page. CKAN uses ``start`` (offset)…, Return the parameters for the following page., Convert to query-string parameters for the CKAN API.

### Community 88 - "ckan/mapper.py"
Cohesion: 0.13
Nodes (22): _coerce_int(), map_dataset(), map_license(), map_organization(), map_resource(), Any, Dataset, License (+14 more)

### Community 90 - "DataGouvPage"
Cohesion: 0.25
Nodes (5): DataGouvPage, Pagination helpers for the data.gouv.fr (udata) API. udata uses page-number /…, Parameters for a single udata results page (1-based)., Return the parameters for the following page., Convert to query-string parameters for the udata API.

### Community 91 - "DataGouvAdapter"
Cohesion: 0.06
Nodes (38): DataGouvAdapter, BaseAdapter, Dataset, Organization, Query, Resource, SearchResult, data.gouv.fr (udata) adapter implementation. Communicates with the udata REST… (+30 more)

### Community 95 - "downloader.py"
Cohesion: 0.12
Nodes (22): ChecksumMismatchError, DownloadError, Raised when a resource's access kind has no reader implementation (D-P4-09).…, Raised when a resource download fails., Raised when a downloaded file's checksum does not match., UnsupportedAccessError, Simple file-based cache for downloaded resources., Downloader (+14 more)

### Community 96 - "geojson.py"
Cohesion: 0.16
Nodes (15): _batch_from_rows(), _fmt_coord(), Any, Streaming GeoJSON reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-11).…, Format a coordinate value: integers without trailing ``.0``; floats as-is., Yield ``RecordBatch`` objects by flattening GeoJSON Features. Args: source: A…, Build a single ``RecordBatch`` from a chunk of feature rows., Encode a GeoJSON ``geometry`` object as WKT (or fall back to raw JSON). Handles… (+7 more)

### Community 98 - "CatalogCapabilities"
Cohesion: 0.40
Nodes (3): CatalogCapabilities, CatalogCapabilities model — query-field-level capability contract (D-07)., Capabilities a catalog connector advertises to the runtime. Phase 5's reject…

### Community 101 - "httpx_transport.py"
Cohesion: 0.18
Nodes (16): Raised on HTTP 5xx responses, or transport-level failures, that should be…, RetryableHTTPError, _parse_retry_after(), HTTP client with retry, rate-limiting, and authentication support., Parse a ``Retry-After`` header into a delay in seconds. Supports both delta-…, Render *body* as text, truncating to *limit* characters., _truncate_body(), httpx-backed HTTP transport satisfying the Transport + StreamingTransport… (+8 more)

### Community 102 - "SocrataPage"
Cohesion: 0.25
Nodes (5): Pagination helpers for the Socrata SODA2 API. Socrata uses offset / limit…, Parameters for a single Socrata results page., Return the parameters for the following page., Convert to query-string parameters for the SODA2 API., SocrataPage

### Community 106 - "Storage"
Cohesion: 0.18
Nodes (7): ABC, Storage abstraction for reading and writing resource files. Currently supports…, Abstract storage backend., Persist *data* under *key* and return the storage URI/path., Read and return the bytes stored under *key*., Return ``True`` if *key* exists in storage., Storage

### Community 115 - "IterableBytesIO"
Cohesion: 0.06
Nodes (25): CredentialScope, HTTPMessage, IO, RawIOBase, Request, IterableBytesIO, WriteableBuffer, IterableBytesIO — adapt an ``Iterable[bytes]`` into a non-seekable BinaryIO… (+17 more)

### Community 116 - "CSVReader"
Cohesion: 0.22
Nodes (8): CSVReader, Any, BaseFormatReader, Streaming CSV reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-10).…, Stream a CSV ``BinaryIO`` source into Arrow ``RecordBatch`` objects. Args:…, Yield ``RecordBatch`` objects by delegating to ``pyarrow.csv.open_csv``. Args:…, Drain a ``pa.RecordBatchReader`` and re-chunk into ``batch_size``-row batches., _rechunk_reader()

### Community 119 - "xlsx.py"
Cohesion: 0.24
Nodes (8): BaseFormatReader, _batch_from_rows(), Any, Streaming XLSX reader yielding Arrow ``RecordBatch`` (DATA-03, D-P4-10).…, Stream an XLSX ``BinaryIO`` source into Arrow ``RecordBatch`` objects., Yield ``RecordBatch`` objects by chunking openpyxl ``iter_rows``. Args: source:…, Build a single ``RecordBatch`` from a chunk of row dicts., XLSXReader

### Community 121 - "_reject.py"
Cohesion: 0.20
Nodes (7): _is_set(), Pre-flight reject policy for unsupported ``Query`` filter fields (D-P5-06). The…, Raise ``UnsupportedQueryFieldError`` if *query* uses a field not in…, Return ``True`` when *value* counts as a set filter field. ``None``, empty…, _reject_unsupported_fields(), Raised when a caller sets a ``Query`` filter field the connector rejects…, UnsupportedQueryFieldError

### Community 124 - "domain/credentials.py"
Cohesion: 0.50
Nodes (3): CredentialScope, Credential scoping model for host-bound credential policy., Host-scoped policy controlling where credentials may be sent. Attributes:…

### Community 125 - "StreamResponse"
Cohesion: 0.33
Nodes (3): Backend-agnostic streaming response wrapper (D-P3-07). Iterable for byte chunks…, Release the underlying httpx response., StreamResponse

## Knowledge Gaps
- **21 isolated node(s):** `Funding Config (Buy Me a Coffee: nitishraj)`, `Bug Report Issue Template`, `Issue Template Config (blank issues enabled)`, `Feature Request Issue Template`, `CI Lint & format job (ruff)` (+16 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **42 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `BatchStream` connect `BatchStream` to `batch_stream.py`, `data/access.py`, `pipeline.py`, `data/__init__.py`, `to_arrow`?**
  _High betweenness centrality (0.055) - this node is a cross-community bridge._
- **Why does `HttpxTransport` connect `HttpxTransport` to `httpx_transport.py`, `PortalError`, `_effective_origin`, `.__init__`, `transport/__init__.py`?**
  _High betweenness centrality (0.030) - this node is a cross-community bridge._
- **Why does `PluginManager` connect `PluginManager` to `discovery/detector.py`, `.__init__`, `logging.py`?**
  _High betweenness centrality (0.027) - this node is a cross-community bridge._
- **Are the 6 inferred relationships involving `BatchStream` (e.g. with `DataPlaneResourceReader` and `_StreamClosingBytesIO`) actually correct?**
  _`BatchStream` has 6 INFERRED edges - model-reasoned connections that need verification._
- **Are the 3 inferred relationships involving `PortalError` (e.g. with `HttpClient` and `HttpxTransport`) actually correct?**
  _`PortalError` has 3 INFERRED edges - model-reasoned connections that need verification._
- **Are the 6 inferred relationships involving `BaseAuth` (e.g. with `APIKeyAuth` and `BasicAuth`) actually correct?**
  _`BaseAuth` has 6 INFERRED edges - model-reasoned connections that need verification._
- **What connects `Funding Config (Buy Me a Coffee: nitishraj)`, `Bug Report Issue Template`, `Issue Template Config (blank issues enabled)` to the rest of the system?**
  _21 weakly-connected nodes found - possible documentation gaps or missing edges._
