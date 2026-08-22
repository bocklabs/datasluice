# Connectors

Connectors are typed façades over DataSluice's canonical catalog contract.
Each platform lives in its own package and exposes exactly one connector class
and one factory function — nothing else is public.

## Platform packages

| Package | Public surface | Pinned profile |
|---------|----------------|----------------|
| `datasluice.connectors.catalog.ckan` | `CKANConnector`, `create_ckan_connector` | CKAN `2.11.5`, Action API v3 |
| `datasluice.connectors.catalog.udata` | `UDataConnector`, `create_udata_connector` | uData `17.3.0`, API v1 |
| `datasluice.connectors.catalog.socrata` | `SocrataConnector`, `create_socrata_connector` | Socrata `3.0`, SODA 3 |

Imports are always explicit and package-level:

```python
from datasluice.connectors.catalog.ckan import CKANConnector, create_ckan_connector
from datasluice.connectors.catalog.socrata import SocrataConnector, create_socrata_connector
from datasluice.connectors.catalog.udata import UDataConnector, create_udata_connector
```

The `datasluice.connectors.catalog` namespace itself re-exports nothing:
platform APIs never leak through a shared root, and the package root
exports only shared models, typed catalog errors, and the retained
data-plane API.

## How a connector is built

Every factory accepts a single `CatalogConnectorContext` and validates it
before construction. The context carries:

- **Sync and async operation executors** — the injected execution seam that
  dispatches typed `CatalogOperationRequest` payloads behind guards.
- **Normalized service projections** — the small cross-platform client
  Protocols (`SyncCatalogClient` / `AsyncCatalogClient`) covering genuinely
  shared datasets, resources, and organizations behavior.
- **Native service projections** — complete typed platform-specific service
  groups (datasets, resources, organizations, users, admin) defined as
  separate sync and async Protocols. There is no raw HTTP escape hatch.
- **The effective capability profile** — a versioned, pinned
  `EffectiveCapabilityProfile` that must match the platform's locked
  profile version, or construction fails.
- **Explicit executor ownership flags** — the context states whether the
  façade owns executor closing, so sync and async lifecycles stay
  independent and deterministic.

Phase 1 façades consume caller-supplied executors. The deterministic
reference fakes in `datasluice.contracts.catalog.fakes` satisfy every
Protocol and back the executable contract suite; live CKAN, uData, and
Socrata endpoint clients are implemented in Phases 3–5 after controlled
endpoint evidence is recorded.

## Capability profiles and evidence

Each platform's behavior is encoded in a versioned profile committed to the
repository (`ckan-2.11.json`, `udata-17.3.json`, `socrata-soda3.json` under
`datasluice.contracts.catalog.profiles`). Profiles distinguish core,
optional, authenticated, and deployment-unavailable operations, and
capability evidence is scoped to a single operation ID — a public read
never implies write or admin access.

At call time, normalized operations pass through an
`EffectiveCapabilityProfile` guard that fails **before dispatch** with a
typed remedy for unavailable, unauthorized, forbidden, and
deployment-disabled states. Safety-sensitive behavior — retries,
destructive operations, insecure TLS, raw diagnostics — stays locked
behind explicit safe-policy choices rather than defaults.

## Third-party connectors

Third-party connectors are opt-in plugins:

- Connector IDs are namespaced `vendor/platform`; built-ins cannot be
  overridden without explicit caller selection.
- A connector ships a validated `ConnectorManifest` and declared
  capability profile, and stays inactive until the caller explicitly
  selects its namespaced ID.
- Certification runs the public contract suite from
  `datasluice.contracts.catalog` and publishes the resulting compliance
  report — summary claims from the connector itself are never trusted.

See [Platform Contracts](supported-portals.md) for the pinned platform
profiles and [Architecture](architecture.md) for how the layers fit
together.
