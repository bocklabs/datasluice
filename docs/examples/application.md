# Application Example

This example walks the explicit public `DataSluice` data-plane flow and its
direct-resource CLI commands.

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
