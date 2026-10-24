# Platform Contracts

DataSluice targets three catalog platforms. Each platform has an explicit,
typed connector profile pinned to a documented API version. The profile
defines the supported operations and capability classes.

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
- **Evidence baseline:** uData `17.6.0`, API v1 and v2 (`udata-17.6.json`)
- **Runtime range:** `>=17.0.0,<18.0.0`, loaded from the profile
- **Coverage:** the pinned profile encodes the uData v1 operation
  inventory across datasets, reuse, organizations, and community
  features, with the same per-operation capability classification.

```python
from datasluice.connectors.catalog.udata import UDataConnector, create_udata_connector
```

uData is the open-source platform software behind data.gouv.fr.
DataSluice contracts with the platform API, not with any single
deployment.

DataSluice requires a valid `17.x` version from anonymous `/api/1/site/` evidence.
It rejects missing, malformed, ambiguous, 16.x, and 18.x versions before API dispatch.
The `17.6.0` profile remains the controlled evidence baseline.

Each typed `/api/1` or `/api/2` operation must match the deployment's anonymous Swagger method and path.
The client caches the needed Swagger document with the capability TTL.
An absent route or method returns `UnsupportedCapabilityError` before the target request.
Unavailable, malformed, oversized, or redirecting Swagger evidence returns `CatalogUnavailableError`.
Call `client.invalidate()` to discard cached evidence.

The anonymous site bootstrap and `/oauth` routes are the only Swagger exemptions.
OAuth still requires a supported version. Its 404 and 405 responses return a typed unsupported error.
Route presence does not prove response compatibility. Changed payloads still fail through typed decoding.
New deployment routes do not create typed methods. Public mutations remain prohibited.

### Known public deployments

uData does not publish an official registry of all public instances. The
table below covers every uData deployment named by the
[uData showcase](https://github.com/opendatateam/getudata.org/tree/master/data/showcase)
and [dataportals.org](https://dataportals.org/) on 2026-10-24. It does not
cover unlisted or private installations.

| Portal | Origin | Version evidence | DataSluice support |
|---|---|---|---|
| France | <https://www.data.gouv.fr> | `17.16.0` from the [site API](https://www.data.gouv.fr/api/1/site/) | Yes, for declared operations present in Swagger. Bounded reads and redirect resolution passed on 2026-10-24. |
| Luxembourg | <https://data.public.lu> | Not reported by the [site API](https://data.public.lu/api/1/site/) | No. The version gate rejects a missing `version`. |
| Portugal | <https://dados.gov.pt> | `15.1.0` from the [site API](https://dados.gov.pt/api/1/site/). The [site footer](https://dados.gov.pt/) reports `16.0.0`. | No. The reported version is outside the `17.x` range. |
| Serbia | <https://data.gov.rs> | `10.1.4.dev0` in the [site footer](https://data.gov.rs/). The [site API](https://data.gov.rs/api/1/site/) omits `version`. | No. The version gate rejects a missing `version`. |
| Tunisia | <https://www.data4tunisia.org> | `17.3.0` from the [site API](https://www.data4tunisia.org/api/1/site/) | Yes, for declared operations present in Swagger. Bounded reads and redirect resolution passed on 2026-10-24. |

The version and route gates stop unsupported requests before the target operation starts.
The live checks used anonymous dataset search, dataset detail, resource metadata, and resource redirect resolution.
They did not download resource bytes or mutate public deployments.
Versions, routes, and response shapes can change after a portal upgrade.

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
