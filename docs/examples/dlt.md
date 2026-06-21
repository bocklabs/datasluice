# dlt Integration

Use DataSluice as a source for [dlt](https://dlthub.com/) pipelines.

```python
import dlt
from datasluice.integrations.dlt import datasluice_source

# Load datasets from a portal into a dlt pipeline
source = datasluice_source(
    portal="https://catalog.data.gov",
    query="climate",
)

pipeline = dlt.pipeline(
    pipeline_name="opendata",
    destination="duckdb",
    dataset_name="climate_data",
)

info = pipeline.run(source)
print(info)
```
