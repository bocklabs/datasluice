# data.gouv.fr Example

```python
from datasluice import DataSluice

ds = DataSluice("https://www.data.gouv.fr")

# Search for datasets about housing
for dataset in ds.search("logement"):
    print(dataset.title, dataset.organization)

# Download all CSV resources from a dataset
dataset = ds.get_dataset("some-dataset-id")
ds.download_all(dataset, dest="data/", formats=["CSV"])
```
