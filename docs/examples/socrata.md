# Socrata Example

The Socrata connector is an explicit typed contract pinned to Socrata
`3.0` (SODA 3). In Phase 1 the contract is executable end to end against
deterministic reference fixtures; the live Socrata endpoint client is
implemented in Phase 5 after controlled endpoint evidence is recorded
against the pinned profile. Deployments still serving older SODA
generations are out of scope until a new capability profile ships.

## Import the platform package

```python
from datasluice.connectors.catalog.socrata import SocrataConnector, create_socrata_connector
```

`create_socrata_connector` accepts one `CatalogConnectorContext` carrying
the injected sync and async executors, normalized and native Socrata
service projections, and the pinned effective capability profile. The
factory rejects any context whose profile is not the pinned SODA 3
profile.

## Run the pinned contract matrix

The public compliance runner executes the pinned Socrata fixture cases
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

fixture_set = load_reference_fixture_set("socrata")
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
async modes. Third-party connectors certify through the same runner.

## Phase boundary

- **Now (Phase 1):** typed Protocols, pinned capability profiles,
  deterministic reference fakes, and the public compliance runner.
- **Phase 5:** the live Socrata SODA 3 client behind the same factory
  and contract surface, once endpoint evidence is recorded.

See [Connectors](../connectors.md) for factory construction details and
[Platform Contracts](../supported-portals.md) for the pinned Socrata
profile.
