# Platform Contracts

DataSluice targets three catalog platforms. In Phase 1 each platform is
published as an explicit, typed connector contract pinned to the latest
stable officially documented API — not as a live endpoint client. Live
CKAN, uData, and Socrata clients ship in Phases 3–5 after controlled
endpoint evidence is recorded against each pinned profile.

## CKAN

- **Package:** `datasluice.connectors.catalog.ckan`
- **Pinned profile:** CKAN `2.11.5`, Action API v3 (`ckan-2.11.json`)
- **Coverage:** the pinned profile spans the complete CKAN Action API
  surface — datasets, resources, organizations, users, and administrative
  groups — classified per operation as core, optional, authenticated, or
  deployment-unavailable.

```python
from datasluice.connectors.catalog.ckan import CKANConnector, create_ckan_connector
```

## uData

- **Package:** `datasluice.connectors.catalog.udata`
- **Pinned profile:** uData `17.3.0`, API v1 (`udata-17.3.json`)
- **Coverage:** the pinned profile encodes the uData v1 operation
  inventory across datasets, reuse, organizations, and community
  features, with the same per-operation capability classification.

```python
from datasluice.connectors.catalog.udata import UDataConnector, create_udata_connector
```

uData is the open-source platform software behind data.gouv.fr;
DataSluice contracts with the platform API, not with any single
deployment.

## Socrata

- **Package:** `datasluice.connectors.catalog.socrata`
- **Pinned profile:** Socrata `3.0`, SODA 3 (`socrata-soda3.json`)
- **Coverage:** the pinned profile declares the SODA 3 operation surface.
  Deployments still serving older API generations are unsupported — a
  newer vendor API requires a new capability profile, contract
  verification, and a reviewed release.

```python
from datasluice.connectors.catalog.socrata import SocrataConnector, create_socrata_connector
```

## Profile coverage and locked behavior

Every platform profile distinguishes core, optional, authenticated, and
deployment-unavailable operations, and capability evidence is scoped to a
single operation ID — a public read never implies write or admin access.
Normalized calls are guarded before dispatch: unavailable, unauthorized,
forbidden, and deployment-disabled states fail with typed remedies rather
than silently hiding behavior.

The following safety choices are locked and require explicit opt-outs
rather than configuration-by-default:

- Verified TLS on every connection; insecure transport only through a
  narrowly scoped explicit override for controlled environments.
- Credentials resolve from explicit injection first; environment,
  keychain, or secret-manager discovery is opt-in.
- Destructive operations require an explicit mutation policy; retries
  cover only safe/idempotent operations.
- Raw diagnostic bodies, redaction bypasses, and durable audit sinks are
  explicitly selected, never implicit.
- Third-party connector manifests stay inactive until the caller
  explicitly selects their namespaced ID; runtime dependency download is
  forbidden.

## Verifying a connector

The public compliance runner in `datasluice.contracts.catalog` executes
pinned fixture cases against any sync/async client pair and emits a
machine-readable `ComplianceReport`. Built-in and third-party connectors
certify through the same runner — see [Connectors](connectors.md) for
factory construction and third-party manifests, and the
[API Reference](api.md) for the full contract surface.
