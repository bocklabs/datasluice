# Application Example

This example walks the complete public `DataSluice` facade flow and all seven
CLI commands against a local portal and a local file.

## Facade: discovery, search, streaming, and materialization

```python
from datasluice import (
    Artifact,
    CatalogResourceLocator,
    DataSluice,
    DirectResourceLocator,
    resource_locator_from_dict,
)

SOURCE_FILE = "/tmp/datasluice-source.csv"

with DataSluice() as ds:
    # Detect a portal and search datasets through the Portal wrapper.
    portal = ds.portal("https://catalog.example.test/api")
    results = portal.search("open data")
    print(results.total)

    # One-shot convenience: ds.search directly.
    results = ds.search("https://catalog.example.test/api", "climate")

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

    # Catalog locator with a strict serialized identity.
    catalog = CatalogResourceLocator(
        portal_url="https://catalog.example.test/api",
        dataset_id="dataset-1",
        resource_id="resource-1",
    )
    artifact: Artifact = ds.materialize(catalog, "/tmp/datasluice-out.parquet")
    print(artifact.content_digest, artifact.uri)

    # Locators round-trip through a versioned JSON envelope.
    payload = direct.to_dict()
    locator = resource_locator_from_dict(payload)
    assert locator == direct
```

## CLI

Each command is installed with the `datasluice` console script.

```bash
# Search and inspect a catalog dataset.
datasluice search "open data" --portal https://catalog.example.test/api --output json
datasluice inspect --portal https://catalog.example.test/api dataset-1 --output json

# Detect a portal's type.
datasluice detect https://catalog.example.test/api --output json

# Scan (bounded sample) and open (bounded preview) a resource.
datasluice scan ./source.csv --output json
datasluice open ./source.csv --output json
datasluice open ./source.csv --all --output jsonl

# Download raw bytes into a directory.
datasluice download --portal https://catalog.example.test/api dataset-1 --dest downloads/ --format CSV

# Materialize exactly one resource into a normalized Artifact.
datasluice materialize ./source.csv --destination ./out.parquet --output json
```

## Provider

Apache Airflow users install the separate
`apache-airflow-providers-datasluice` distribution and import from the
`airflow.providers.datasluice` namespace. See [Apache Airflow](airflow.md).
