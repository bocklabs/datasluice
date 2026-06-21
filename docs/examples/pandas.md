# Pandas Integration

Load open-data resources directly into [pandas](https://pandas.pydata.org/)
DataFrames.

```python
from datasluice import DataSluice

ds = DataSluice("https://catalog.data.gov")

# Search and read into a DataFrame in one step
df = ds.search("climate change", limit=5)[0].to_pandas()
print(df.head())

# Read a specific resource
dataset = ds.get_dataset("some-dataset-id")
df = dataset.resources[0].to_pandas()
```
