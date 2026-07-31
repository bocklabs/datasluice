# DataSluice

## What This Is

DataSluice is a portal-agnostic open-data toolkit — a Python SDK and CLI that discovers open data across government portals (CKAN, data.gouv, Socrata), resolves and reads resources reliably, normalizes them into a consistent format, and exposes them to downstream tools like pandas, Polars, DuckDB, dlt, and Airflow. It is evolving from a working v0.1.0 prototype into a v1.0.0 streaming, plugin-based, hexagonal-architecture platform.

## Core Value

Discover open data, resolve resources, read them reliably, normalize them, and expose them to downstream tools — without becoming a general-purpose ETL engine.

## Business Context

- **Customer**: Developers and data analysts who consume open-data portals
- **Revenue model**: Open-source library (PyPI), no direct monetization
- **Success metric**: Adoption — developers can search, stream, and materialize open data from any supported portal with a single, consistent API
- **Strategy notes**: Architecture audit (July 2026) defines the target state

## Requirements

### Validated

- ✓ Portal-agnostic domain models (Dataset, Resource, Organization, License, Query) — existing frozen dataclasses
- ✓ Adapter pattern with per-portal subpackages (CKAN, data.gouv, Socrata, Custom) — existing
- ✓ Typer CLI with search, inspect, download, detect commands — existing
- ✓ HTTP transport with retry, rate-limiting, auth injection — existing (urllib-based)
- ✓ File format readers (CSV, JSON, Parquet, XLSX, GeoJSON) — existing
- ✓ Integrations with pandas, Polars, dlt, DuckDB, Airflow — existing (thin/independent)
- ✓ File cache + checksum verification + local storage — existing
- ✓ Portal auto-detection via fingerprints — existing
- ✓ Auth strategies (None, APIKey, Bearer, Basic, Headers) — existing
- ✓ Lazy optional dependency imports — existing
- ✓ CI/CD pipeline (lint, typecheck, test, publish) — existing
- ✓ Documentation site (Zensical/MkDocs Material) — existing
- ✓ README examples valid + stale repo URLs corrected — Phase 1 (CORR-01/02)
- ✓ CLI `download --format` filters resources before download — Phase 1 (CORR-03)
- ✓ Credentials host-scoped via `CredentialScope` + cross-host redirect header stripping — Phase 1 (SEC-01/02)
- ✓ Auth secrets redacted in `repr()` — Phase 1 (SEC-04)
- ✓ Retry classification: 5xx/429/connection retry w/ jitter; 4xx fail-fast — Phase 1 (SEC-05)
- ✓ URL-derived cache filenames replaced with SHA-256 hashes — Phase 1 (SEC-06)
- ✓ DuckDB SQL injection fixed (Python relation API + validated identifiers) — Phase 1 (SEC-03)
- ✓ Dead `Settings` system removed (no `DATASLUICE_` env vars; defaults demoted to plain constants) — Phase 2 (CORR-04)
- ✓ `DataSluiceSession` composition root (zero-config facade, wires PluginManager + transport + auth) — Phase 2 (ARCH-03)
- ✓ Narrow Protocol ports: `CatalogPort`/`SearchableCatalog`/`OrganizationCatalog` split from `ResourceReader`; plus `Transport`, `CredentialProvider`, `StoragePort`, `CachePort`, `StateStore`, `PortalDetector` — Phase 2 (ARCH-02, ARCH-07)
- ✓ Six new frozen domain models: `Schema`, `ResourceAccess` + 5 access kinds, `DetectionResult`, `Artifact`, `SyncState`, `CatalogCapabilities` — Phase 2 (ARCH-01)
- ✓ `PluginManager` with entry-point discovery + per-entry error isolation replaces global side-effect registry — Phase 2 (ARCH-04, ARCH-05, ARCH-06, QUAL-09)
- ✓ Python entry-points (`datasluice.connectors` group) for third-party connector discovery — Phase 2 (ARCH-04)
- ✓ `adapters/` renamed to `connectors/` with per-connector factory functions — Phase 2 (D-03, D-04, D-13)
- ✓ `BatchStream` (context-managed Arrow `RecordBatch` wrapper) + `IterableBytesIO` non-seekable byte adapter — Phase 4 (DATA-01, DATA-02)
- ✓ All 5 format readers (CSV, JSON, Parquet, XLSX, GeoJSON) yield `RecordBatch` streams conforming to the domain `Schema`; old `formats/` package + dict-buffering integration read paths removed — Phase 4 (DATA-03)
- ✓ `DataPlaneResourceReader` with `ResourceAccess`-kind dispatch (HttpDownload streaming via `StreamingTransport` + urllib fallback, ObjectStorage, LocalFile, Query/Stream stub) — Phase 4 (DATA-04)
- ✓ Streaming HTTP responses with bounded memory (chunked transfer-encoding endpoint + subprocess peak-RSS proof via `resource.getrusage`) — Phase 4 (DATA-05)
- ✓ Transparent compression decorators (GZIP/BZIP2/ZSTD streaming + ZIP spool, largest-member selection, truncated-frame → `DecompressionError`) — Phase 4 (DATA-06)
- ✓ Domain `Schema` model → Arrow mapping (`to_arrow_schema`) + documented type-promotion unification via `pa.concat_tables` (`unify_batches`) — Phase 4 (DATA-07, DATA-08)
- ✓ Peak-RSS subprocess test proves streaming data plane keeps memory bounded on large inputs — Phase 4 (QUAL-05)
- ✓ Incremental StateStore implementations with conditional fetch, CAS transitions, and per-session swapping — Phase 7 (SYNC-01, SYNC-02)
- ✓ Cursor/watermark state, per-batch checkpoint emission, physical Parquet resume, and source/destination replacement handling — Phase 7 (SYNC-03, SYNC-04, SYNC-05)
- ✓ ETag/Last-Modified conditional GETs, idempotent materialization, destination health checks, and secret-free durable metadata — Phase 7 (SYNC-06, SYNC-07)
- ✓ dlt resources yield real Arrow data and round-trip DataSluice incremental state — Phase 7 (INTG-05, INTG-06)

### Active

- [ ] Introduce capability metadata (`CatalogCapabilities`) with reject/warn policy for unsupported filters
- [ ] Remove the non-functioning `CustomAdapter`
- [ ] Adopt fsspec for storage (local, S3, GCS, Azure Blob, HTTP)
- [ ] Composable transformation pipeline (SelectColumns, RenameColumns, CastSchema, NormalizeTimestamps, Filtering, Flattening)
- [ ] Consistent terminal conversions (to_arrow, to_pandas, to_polars, to_duckdb) consuming shared `BatchStream`
- [ ] Separate Airflow provider distribution (`apache-airflow-provider-datasluice`)
- [ ] Connector contract test suite (reusable per-connector conformance tests)
- [ ] Raise coverage threshold from 50% to 80-85%

### Out of Scope

- Building warehouse destinations or loading semantics into core — dlt handles destination loading, merge, schema migration
- Workflow scheduling / orchestration engine — Airflow handles this
- A general-purpose ETL framework — DataSluice resolves and exposes resources; downstream tools transform and load
- Internal DataFrame abstraction — use Apache Arrow as the canonical tabular representation
- `pluggy` plugin framework — standard `importlib.metadata.entry_points()` + explicit factories are sufficient until broadcast hooks are needed
- Splitting every connector into separate repositories — keep modular monolith with entry-point contract for optional external distribution

## Context

**Current state (v0.1.0):** The project has a clean initial package structure with per-portal connector subpackages, separated mappers, lazy optional deps, and a working CLI + library API on PyPI. Through Phases 1–7 the correctness/security issues are fixed, a hexagonal composition root with narrow Protocol ports and plugin-based connectors is in place, the data plane streams Arrow `RecordBatch` with bounded memory, and incremental sync now supports checkpointed resume, idempotent materialization, conditional fetches, and real dlt resource data. Remaining v1.0.0 work is the final application layer, CLI assembly, separate Airflow provider, end-to-end coverage, and release gate in Phase 8.

**Architecture audit (July 2026):** A comprehensive audit recommended evolving to hexagonal architecture (ports and adapters) with capability-based connector plugins, streaming Arrow-oriented processing, credential scoping, fsspec storage, rebuilt integrations, and incremental synchronization. The audit is accepted wholesale as the design contract for v1.0.0.

**Key architectural principles for v1.0.0:**
- Hexagonal architecture — ports only at unstable external boundaries, not a class per operation
- Capability protocols — connectors implement only what they support (small Protocols, not one fat base class)
- Streaming data plane — `BatchStream` with Arrow `RecordBatch`, not `list[dict]` buffering
- Plugin-based connectors — `importlib.metadata.entry_points()` discovery + `PluginManager`
- Separation of concerns — catalog connector (finds/describes) vs resource reader (opens/streams) vs format reader (decodes)

**User:** Solo developer, personal project, no external users to migrate. Breaking changes are fully acceptable.

## Constraints

- **Tech stack**: Python 3.12+ (PEP 695 type params, union syntax). Keep `uv` as the sole package manager. Never call `pip` directly.
- **Dependencies**: Add `httpx` (replace urllib) and `pyarrow` (Arrow data plane) as optional extras. Add `fsspec` for storage abstraction. Keep lazy import discipline for all heavy deps.
- **Code style**: Line length 120, ruff (E, W, F, I, B, UP), Google-style docstrings, no comments unless requested.
- **Version target**: v1.0.0 — this milestone defines the stable architecture.
- **Architecture**: Hexagonal (ports and adapters) implemented as a modular monolith. Strategy/Abstract Factory for connectors, Pipeline/Chain of Responsibility for transforms, Facade for public API.
- **Backward compatibility**: None required — v0.1.0 with no external users. Full API redesign is acceptable.
- **Testing**: Coverage threshold raised to 80-85%. Connector contract test suite for conformance.

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Adopt hexagonal architecture (ports and adapters) | Separates unstable external boundaries from domain logic; enables plugin extensibility | — Pending |
| Replace urllib with httpx | Connection pooling, streaming responses, explicit timeouts, redirect policy, retry for 5xx | — Pending |
| Use Apache Arrow RecordBatch as canonical tabular representation | Streaming, schema-aware, zero-copy interop with pandas/Polars/DuckDB | — Pending |
| Use fsspec for storage abstraction | Unified API for local/S3/GCS/Azure/HTTP; avoids custom filesystem code | — Pending |
| Python entry_points for plugin discovery | Standard packaging mechanism; enables separately distributed connectors | — Pending |
| Separate Airflow provider distribution | Avoids forcing core CI to install full Airflow stack | — Pending |
| Keep modular monolith (not microrepos) | Simplicity; entry-point contract allows external distribution when needed | — Pending |
| DuckDB Python relation API (not f-string SQL) for injection-proofing | URL flows as a C-string bind arg, never parsed as SQL; table_name regex-validated | ✓ Done — Phase 1 |
| Credential scoping via frozen `CredentialScope` + opener-based redirect handler | Credentials never leak cross-host or on scheme downgrade; zero-config safe default | ✓ Done — Phase 1 |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-07-31 after Phase 7 — incremental sync and dlt integration complete (checkpoint/resume, conditional fetch, idempotent materialization, secure durable state, and real Arrow resources). 614 tests passing, 6 skipped.*
