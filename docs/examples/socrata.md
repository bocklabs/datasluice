# Socrata Example

```python
from datasluice import DataSluice

ds = DataSluice("https://data.cityofchicago.org")

# Search for datasets
for dataset in ds.search("crimes"):
    print(dataset.title)

# Read a resource directly into a list of dicts
records = ds.read(dataset.resources[0])
print(len(records), "rows")
```
