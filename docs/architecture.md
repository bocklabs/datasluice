# Architecture

This document describes the internal architecture of DataSluice.

## Overview

DataSluice is built around a layered architecture that separates domain models,
transport concerns, and portal-specific adapters. The goal is to provide a single,
consistent interface across heterogeneous open-data portals.

```
┌──────────────────────────────────────────────────────┐
│                   Public API                         │
│            datasluice.DataSluice                      │
├──────────┬──────────┬──────────┬─────────────────────┤
│   CLI    │ Pandas/  │  dlt /   │   Airflow / DuckDB  │
│ (Typer)  │  Polars  │ DuckDB   │   integrations      │
├──────────┴──────────┴──────────┴─────────────────────┤
│                  Adapters Layer                        │
│   base · registry · factory                            │
├─────────┬──────────┬──────────┬──────────────────────┤
│  CKAN   │ data.gouv│ Socrata  │  Custom adapters      │
├─────────┴──────────┴──────────┴──────────────────────┤
│                Domain Models                           │
│  Dataset · Resource · Organization · License · Query  │
├───────────────────────────────────────────────────────┤
│              Cross-cutting Concerns                    │
│  transport · auth · discovery · io · formats · config │
└───────────────────────────────────────────────────────┘
```

## Layers

### 1. Domain Models (`datasluice.domain`)

Plain dataclasses and types that represent open-data concepts in a
portal-agnostic way. These are the lingua franca of the library—adapters map
portal-native responses into these models and consumers work with them directly.

### 2. Adapters (`datasluice.adapters`)

Each open-data portal has a dedicated adapter that implements the
`BaseAdapter` protocol. Adapters are responsible for:

- Translating portal-native API responses into domain models.
- Handling portal-specific pagination strategies.
- Raising normalized errors.

The registry keeps track of known adapters; the factory resolves which adapter
to use for a given portal URL or type.

### 3. Transport (`datasluice.transport`)

A shared HTTP client layer with built-in retry, rate-limiting, pagination, and
user-agent management. All adapters delegate network I/O to this layer.

### 4. Auth (`datasluice.auth`)

Pluggable authentication strategies (API key, bearer token, basic, custom
headers). Auth strategies are injected into the transport layer.

### 5. Discovery (`datasluice.discovery`)

Auto-detection of portal types from URLs using fingerprints and heuristics, so
users can pass a portal URL without knowing its software.

### 6. IO (`datasluice.io`)

File downloading, caching, checksum verification, and storage abstraction for
materializing resources locally.

### 7. Formats (`datasluice.formats`)

Format-specific readers that normalize CSV, JSON, XLSX, Parquet, and GeoJSON
into common in-memory representations.

### 8. Integrations (`datasluice.integrations`)

Optional integrations with the broader data ecosystem: pandas, Polars, dlt,
Apache Airflow, and DuckDB.

## Design Principles

- **Portal-agnostic domain models** — consumers never touch portal-native JSON.
- **Adapter isolation** — each portal's quirks live in its adapter subpackage.
- **Composable transport** — retry, rate-limiting, and pagination are decorators,
  not baked into adapters.
- **Lazy imports** — heavy optional dependencies (pandas, dlt, etc.) are imported
  on demand.
