# Connectors

Connectors are typed façades over DataSluice's canonical catalog contract.
Each platform lives in its own package and exposes exactly one connector class
and one factory function — nothing else is public.

## Platform packages

| Package | Public surface | Pinned profile |
|---------|----------------|----------------|
| `datasluice.connectors.catalog.ckan` | `CKANConnector`, `create_ckan_connector` | CKAN `2.11.5`, Action API v3 |
| `datasluice.connectors.catalog.udata` | `UDataConnector`, `create_udata_connector` | uData `17.6.0`, API v1 |
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
repository (`ckan-2.11.json`, `udata-17.6.json`, `socrata-soda3.json` under
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

### Record serialization and retained evidence

Runtime record payloads and extension mappings are not evidence artifacts.
They can contain personal data, private content, and credentials from the deployment.
The dataset summary mapper redacts credential-bearing fields before it returns the record.
Other family decoders can apply their existing field allowlists or credential redaction.

`NativeRecord.to_dict()` and `MappingRecord.to_dict()` return bounded copies.
Each payload or extension projection permits at most 256 visited values, eight nesting levels, and 32 entries per container.
Keys and strings permit at most 256 characters.
Large integers and omitted entries use `[TRUNCATED]`.
These methods mask credential fields, contact fields, private fields, internal fields, and origin fields.
They also remove credential-shaped URL and authorization values from strings and keys.
They do not identify every form of personal data in free text.
Serialization does not change the runtime payload.
Serialized copies are not lossless backups.

Use `record.to_evidence_dict()` for retained fixtures, reports, and review evidence.
This method records container sizes and JSON types only.
It excludes identifiers, field names, scalar values, and extension values, including public taxonomy metadata.
The same structural limits apply to each record.
`ResultEnvelope.to_evidence_dict()` includes at most 32 item projections and reports the original item count.
It excludes pagination URLs, warning text, and deployment metadata.
It rejects items without an evidence projection instead of using their ordinary serializer.
Ordinary `ResultEnvelope.to_dict()` remains a runtime envelope serializer, not a bounded evidence export.
Do not retain runtime payloads, dataclass conversions, or arbitrary result wrappers as evidence.

## uData security contracts

Authorization-code exchange requires an `OAuthAuthorizationBinding` and the received callback state.
Call `binding.token_request(code, state=received_state)`.
The binding checks the expected state, client, redirect URI, token endpoint, and PKCE verifier.
OAuth forms reveal secrets only at dispatch and cannot be replayed automatically.

Composite confirmation targets percent-encode each identifier before joining the components with colons.
For transfers, use `TransferRequestInput.mutation_target()` instead of constructing the target string.
Transfer targets also include the subject and recipient classes.
Unescaped colon-delimited targets no longer authorize distinct identifier pairs.

Native buffered responses always have a finite byte limit, including calls that pass `None` internally.
Upload sources must make each read finite because deadline checks cannot interrupt an arbitrary blocked source read.
Synchronous job waits run observations inline and do not leave observation threads after they return.

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

## Drift readiness

The CKAN connector ships a runnable single-shot drift-read checker at
`datasluice.connectors.catalog.ckan.drift`. It exercises representative
public deployments with bounded, read-only typed calls and prints one
redacted JSON advisory line per check; it registers no CLI subcommand.

Three selection criteria govern every configured check, and each
`DriftCheck` documents its rationale string against them:

1. **Bounded** — every check is a single whitelisted typed read
   (`status_show`, `package_list`, `package_show`,
   `current_package_list_with_resources`) with fixed parameters; generic
   action invocation stays unavailable.
2. **Stable ordering** — sequences compare under a declared ordering
   mode; `platform-deterministic` compares the platform sequence
   against the sorted expectation, while `canonicalized` sorts both sides before
   comparing, so accidental reordering never reports as drift.
3. **No time-varying fields** — expectations are exact key-set
   skeletons; volatile keys are excluded by configuration in the
   check definition, never filtered ad hoc at comparison time.

Confirmed representative targets (amended 2026-08-23 per the recorded
D-11 disposition): `demo.ckan.org` as primary — running the pinned
2.11.5 line — and `ckan.publishing.service.gov.uk` as secondary. Known
behaviors are part of the coverage: the secondary hides its version
string (line state propagates as `unverified`) and selectively rejects
some actions with gateway-level 403s, which the checker records as
`unavailable` advisory rows while sibling checks still execute. Drift
findings are always advisory — they advise, never block.

Run it with:

```bash
uv run python -m datasluice.connectors.catalog.ckan.drift
```

Optional flags stay minimal: `--target SUBSTRING` restricts the run to
matching default targets and `--json-out PATH` mirrors the JSON-lines
output to a file. Scheduling and alerting integration live in a later
phase; this module is the runnable proof only.
