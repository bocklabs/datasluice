# CKAN Example

The CKAN connector is an explicit typed contract pinned to CKAN `2.11.5`
(Action API v3). In Phase 1 the contract is executable end to end against
deterministic reference fixtures; the live CKAN endpoint client is
implemented in Phase 3 after controlled endpoint evidence is recorded
against the pinned profile.

## Import the platform package

```python
from datasluice.connectors.catalog.ckan import CKANAdapter, create_ckan_connector
```

`create_ckan_connector` accepts one `CatalogConnectorContext` carrying the
injected sync and async executors, normalized and native CKAN service
projections, and the pinned effective capability profile. The factory
narrows the generic projections to CKAN's typed native service groups at
the façade boundary and rejects any context whose profile is not the
pinned CKAN profile.

## Run the pinned contract matrix

The public compliance runner executes the pinned CKAN fixture cases
against any sync/async client pair and emits a machine-readable
`ComplianceReport` — no network access, no credentials:

```python
from datasluice.contracts.catalog import (
    catalog_contract_cases,
    load_reference_fixture_set,
    run_catalog_contract,
)
from datasluice.contracts.catalog.fakes import (
    AsyncReferenceConnector,
    SyncReferenceConnector,
)

fixture_set = load_reference_fixture_set("ckan")
cases = catalog_contract_cases(fixture_set)

report = run_catalog_contract(
    cases,
    sync_client=SyncReferenceConnector(fixture_set=fixture_set),
    async_client=AsyncReferenceConnector(fixture_set=fixture_set),
    fixture_set=fixture_set,
)

states = {outcome.state for outcome in report.outcomes}
modes = {outcome.mode for outcome in report.outcomes}
print(states, modes, report.connector_id, report.profile_version)
```

Every case covers a declared operation with a core, optional,
authenticated, unauthorized, or unavailable outcome, in both sync and
async modes. Third-party connectors certify through the same runner —
summary claims from the connector itself are never trusted.

## Phase boundary

- **Now (Phase 1):** typed Protocols, pinned capability profiles,
  deterministic reference fakes, and the public compliance runner.
- **Phase 3:** the live CKAN Action API client behind the same factory
  and contract surface, once endpoint evidence is recorded.

See [Connectors](../adapters.md) for factory construction details and
[Platform Contracts](../supported-portals.md) for the pinned CKAN
profile.
