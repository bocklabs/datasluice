# dlt Integration

Use DataSluice as a source for [dlt](https://dlthub.com/) pipelines.

Install the extra:

```bash
pip install "datasluice[dlt]"
```

dlt extraction accepts a caller-owned synchronous catalog client and a
typed `resources.list` operation — never a URL or an implicit platform
choice:

```python
import dlt
from datasluice.contracts.catalog.protocols import (
    CatalogOperationRequest,
)
from datasluice.domain.catalog.operations import OperationId
from datasluice.integrations.dlt import datasluice_source

query = CatalogOperationRequest(
    operation_id=OperationId(platform="ckan", service="resources", method="list"),
    payload={"dataset_id": "example-dataset"},
)

# client is your SyncCatalogClient: a live platform client from the
# connector implementation phases, or any implementation you provide.
source = datasluice_source(client, query)

pipeline = dlt.pipeline(
    pipeline_name="opendata",
    destination="duckdb",
    dataset_name="catalog_data",
)

info = pipeline.run(source)
print(info)
```

Each normalized resource record becomes one dlt resource with a
deterministic destination-safe name; extraction reads only direct
resource URLs carried by the normalized records.
