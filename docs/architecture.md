# Architecture

This document describes the internal architecture of DataSluice.

## Overview

DataSluice is built around a layered architecture that separates the public
connector contract from domain models, transport concerns, and the
direct-resource data plane. The goal is a small, explicit, typed surface for
catalog platforms — every supported behavior is a declared operation with a
pinned capability profile, not an implicit convention.

```
┌───────────────────────────────────────────────────────────┐
│                        Public API                         │
│   datasluice.connectors.catalog.{ckan,udata,socrata}      │
│   datasluice.contracts.catalog   ·   datasluice.DataSluice│
├───────────┬───────────┬───────────┬───────────────────────┤
│   CLI     │  pandas / │  dlt /    │  DuckDB / Airflow     │
│ (Typer)   │  Polars   │  DuckDB   │  (separate provider)  │
├───────────┴───────────┴───────────┴───────────────────────┤
│                  Catalog Contract Layer                    │
│  protocols · profiles · fakes · runner · report ·         │
│  certification                                            │
├───────────────────────────────────────────────────────────┤
│                Catalog Domain Models                       │
│  ids · models · operations · profiles · patches ·         │
│  receipts · safety · resilience · observability           │
├───────────────────────────────────────────────────────────┤
│             Cross-cutting Concerns                         │
│  transport · credentials · discovery · io · data · config │
└───────────────────────────────────────────────────────────┘
```

## Layers

### 1. Public connector packages (`datasluice.connectors.catalog`)

Three platform packages — `ckan`, `udata`, `socrata` — each exporting one
connector façade class and one factory function. Factories accept a
`CatalogConnectorContext` (injected sync/async executors, normalized and
native service projections, effective capability profile) and narrow the
generic projections to the platform's typed native service groups at the
façade boundary. The catalog namespace re-exports nothing; imports are
always explicit and package-level.

### 2. Catalog contract layer (`datasluice.contracts.catalog`)

The executable contract API for built-in and third-party connectors:

- **Protocols** — the small normalized catalog client surface
  (`SyncCatalogClient` / `AsyncCatalogClient`) plus complete typed
  platform-native service group projections, with separate sync and async
  operations executors as the injected dispatch seam.
- **Profiles and fixtures** — versioned capability profiles
  (`ckan-2.11.json`, `udata-17.3.json`, `socrata-soda3.json`) and pinned
  reference fixture sets encoding official platform evidence.
- **Reference fakes** — deterministic, fixture-backed sync and async
  clients that satisfy the Protocols for PR-time contract testing.
- **Runner and report** — `run_catalog_contract` executes declared cases
  and emits a machine-readable `ComplianceReport` covering core, optional,
  authenticated, unauthorized, and unavailable behavior.
- **Certification** — `certify_catalog_report` validates explicit
  manifests, declared profiles, fixture fingerprints, and complete
  runner-owned report evidence.

### 3. Catalog domain models (`datasluice.domain.catalog`)

Immutable, typed values shared by every contract participant: opaque
platform-scoped IDs (`CatalogId`, `CatalogPlatform`, `ResourceKind`),
normalized and native records with lossless extension mappings, versioned
JSON-safe envelopes, separate create/patch request models with `UNSET`
semantics, operation specifications, effective capability profiles, and
redacted mutation receipts with bulk checkpoints.

### 4. Direct-resource data plane (`datasluice.data`, `datasluice.io`)

Streaming format readers that decode CSV, JSON, JSONL, XLSX, Parquet, and
GeoJSON into Arrow `RecordBatch` objects via a shared `BatchStream`
contract, plus file downloading, caching, checksum verification, and
storage abstraction for materializing resources locally. The public
`DataSluice` facade (`datasluice.application`) exposes this plane with
`resolve`, `open`, `materialize`, and `open_catalog` for one
caller-selected connector.

### 5. Transport (`datasluice.transport`)

One dual-mode HTTP runtime shared by every connector. Verified TLS is the
default, retries and circuit breakers are platform-aware and
explicitly configured, and sync and async clients keep independent pools
with injectable test transports.

### 6. Credentials and auth (`datasluice.credentials`, `datasluice.auth`)

Platform-specific typed credential providers behind a common resolver.
Explicit injection always wins; environment, keychain, or secret-manager
discovery is opt-in. Known scopes and effective permissions are exposed so
determinably unauthorized operations are rejected before dispatch.

### 7. Discovery and plugins (`datasluice.discovery`, `datasluice.runtime`)

Entry-point discovery of built-in and third-party connectors through the
injected plugin manager. Built-in connector IDs are namespaced
(`datasluice/ckan`, `datasluice/udata`, `datasluice/socrata`); a listing
probes nothing and claims no identity, and third-party manifests stay
inactive until a caller explicitly selects their namespaced ID.

### 8. Integrations (`datasluice.integrations`)

Optional integrations with the broader data ecosystem: pandas, Polars,
dlt, and DuckDB, all rebuilt over the shared `BatchStream`. Integrations
that need catalog data accept caller-owned typed clients — for example,
dlt extraction takes a `SyncCatalogClient` and a typed `resources.list`
operation. The Apache Airflow provider is a separate distribution with
metadata only until live platform executors ship.

## Phase boundary

Phase 1 delivers the executable contract layer above: typed Protocols,
models, capability profiles, reference fakes, the compliance runner, and
the factory-constructed façades over injected executors. Live CKAN, uData,
and Socrata endpoint clients are implemented in Phases 3–5 after pinned
profiles and controlled endpoint evidence are recorded; Phase 2 owns
connector packaging and the named install extras. Nothing in Phase 1
contacts a live deployment.

## Design Principles

- **Explicit typed contracts** — every operation is declared, typed, and
  capability-guarded before dispatch; there is no raw HTTP escape hatch.
- **Sync/async parity** — separate context-managed clients with identical
  operation surfaces, shared models, and independent lifecycles.
- **Evidence-gated claims** — platform behavior is claimed only from
  pinned profiles plus recorded endpoint evidence; capability evidence is
  scoped per operation ID.
- **Safe by default** — verified TLS, redacted logs and receipts, explicit
  credentials, and destructive operations behind explicit mutation
  policies.
- **Lazy imports** — heavy optional dependencies (pandas, dlt, etc.) are
  imported on demand.
