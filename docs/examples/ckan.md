# CKAN Example

```python
from datasluice import DataSluice

ds = DataSluice("https://catalog.data.gov")

# Search for climate datasets
for dataset in ds.search("climate change"):
    print(dataset.title, dataset.id)

# Get a specific dataset
dataset = ds.get_dataset("some-dataset-id")
for resource in dataset.resources:
    print(resource.name, resource.url, resource.format)

# Download a resource
ds.download(dataset.resources[0], dest="data/")
```
