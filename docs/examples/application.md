# Application Example

This example walks the explicit public `DataSluice` data-plane flow, the
canonical connector entry points, and the direct-resource CLI commands.

## Facade: streaming and materialization

```python
from datasluice import (
    Artifact,
    DataSluice,
    DirectResourceLocator,
)

SOURCE_FILE = "/tmp/datasluice-source.csv"

with DataSluice() as ds:
    # Direct locator (a local file or URL).
    direct = DirectResourceLocator(uri=SOURCE_FILE)
    resource = ds.resolve(direct)

    # Open a resource lazily and stream/browse it.
    with ds.open(direct) as opened:
        for batch in opened:
            print(batch.num_rows)

    # Fluent transform pipeline to a pandas frame.
    from pandas import DataFrame

    frame: DataFrame = ds.open(direct).to_pandas()
    print(frame.shape)

    artifact: Artifact = ds.materialize(direct, "/tmp/datasluice-out.parquet")
    print(artifact.content_digest, artifact.uri)
```

## Catalog connectors

Catalog behavior never arrives through a URL or an implicit platform
choice. Connectors are imported explicitly from their platform packages
and constructed through their factories with a fully assembled
`CatalogConnectorContext`:

```python
from datasluice.connectors.catalog.ckan import create_ckan_connector
from datasluice.connectors.catalog.socrata import create_socrata_connector
from datasluice.connectors.catalog.udata import create_udata_connector
```

With a context assembled (see [Connectors](../connectors.md)), a facade
opens exactly one caller-selected connector:

```python
# connector = ds.open_catalog(create_ckan_connector, context)
```

The context supplies the injected sync and async executors, normalized
and native service projections, and the pinned effective capability
profile. In Phase 1 the executors are caller-supplied; deterministic
reference fakes satisfy every projection, and live CKAN, uData, and
Socrata endpoint clients arrive in Phases 3–5.

## Verifying contracts with reference fakes

The public compliance runner executes deterministic fixture cases against
any sync/async client pair and emits a machine-readable
`ComplianceReport`:

```python
from datasluice.contracts.catalog import CatalogContractCase, run_catalog_contract
from datasluice.contracts.catalog.fakes import (
    AsyncReferenceConnector,
    SyncReferenceConnector,
)

report = run_catalog_contract(
    CatalogContractCase(operation_id="datasets.get", dataset_id="fixture-dataset"),
    sync_client=SyncReferenceConnector(),
    async_client=AsyncReferenceConnector(),
)
print([(outcome.mode, outcome.state) for outcome in report.outcomes])
```

## CLI

Each command is installed with the `datasluice` console script.

```bash
# Scan (bounded sample) and open (bounded preview) a resource.
datasluice scan ./source.csv --output json
datasluice open ./source.csv --output json
datasluice open ./source.csv --all --output jsonl

# Materialize exactly one resource into a normalized Artifact.
datasluice materialize ./source.csv --destination ./out.parquet --output json
```

## Provider

Apache Airflow users install the separate
`apache-airflow-providers-datasluice` distribution and import from the
`airflow.providers.datasluice` namespace. See [Apache Airflow](airflow.md).
