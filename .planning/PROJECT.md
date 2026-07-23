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

### Active

- [ ] Correct README examples and stale repository URLs
- [ ] Fix CLI `download --format` filtering bug
- [ ] Scope credentials to hosts (prevent cross-host token leakage)
- [ ] Fix retry classification (retry 5xx, connection errors)
- [ ] Wire or remove dead settings (page_size, cache_dir, cache_ttl, api_key, bearer_token)
- [ ] Replace URL-derived cache filenames with content hashes
- [ ] Introduce `DataSluiceSession` runtime object (composition root)
- [ ] Split `CatalogPort` from `ResourceReader` (separate catalog from resource access)
- [ ] Introduce capability metadata (`CatalogCapabilities`) with reject/warn policy for unsupported filters
- [ ] Replace global side-effect registry with injected `PluginManager`
- [ ] Add Python entry-point discovery for third-party connectors
- [ ] Remove the non-functioning `CustomAdapter`
- [ ] Make portal detection injectable and evidence-based (`DetectionResult` with confidence + evidence)
- [ ] Streaming HTTP responses (bounded memory)
- [ ] Adopt fsspec for storage (local, S3, GCS, Azure Blob, HTTP)
- [ ] `ResourceAccess` descriptors (HttpDownload, ObjectStorage, Query, Stream, LocalFile)
- [ ] Batch/stream readers with Arrow `RecordBatch` support
- [ ] Compression/archive decorators
- [ ] Domain `Schema` model
- [ ] Composable transformation pipeline (SelectColumns, RenameColumns, CastSchema, NormalizeTimestamps, Filtering, Flattening)
- [ ] Consistent terminal conversions (to_arrow, to_pandas, to_polars, to_duckdb) consuming shared `BatchStream`
- [ ] Rebuild dlt integration to yield actual resource data
- [ ] Separate Airflow provider distribution (`apache-airflow-provider-datasluice`)
- [ ] Fix DuckDB SQL injection vulnerability (parameterized queries / validated identifiers)
- [ ] Artifact-oriented outputs (`Artifact` dataclass with URI, media_type, checksum)
- [ ] ETag/Last-Modified state for files
- [ ] Cursor/watermark state for API resources
- [ ] Checkpoint emission and resume behavior
- [ ] State-store ports and idempotent materialization
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

**Current state (v0.1.0):** The project has a clean initial package structure with per-portal adapter subpackages, separated mappers, lazy optional deps, and a working CLI + library API on PyPI. However, several abstractions are ahead of the actual architecture — the public API has correctness bugs, the fat `BaseAdapter` conflates catalog operations with resource access, the data plane fully buffers everything into memory, credentials are shared unrestricted across hosts, and integrations each implement independent (and inconsistent) read paths.

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
*Last updated: 2026-07-23 after initialization*
