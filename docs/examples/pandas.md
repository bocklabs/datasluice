# Pandas Integration

Load open-data resources directly into [pandas](https://pandas.pydata.org/)
DataFrames through the direct-resource data plane.

Install the extra:

```bash
pip install "datasluice[pandas]"
```

Stream any resolved resource into a DataFrame:

```python
from datasluice import DataSluice, DirectResourceLocator

with DataSluice() as ds:
    direct = DirectResourceLocator(uri="https://example.org/data.csv")
    frame = ds.open(direct).to_pandas()
    print(frame.shape)
```

`to_pandas` is a terminal on the opened resource: the underlying batch
stream is consumed exactly once and released on close. The same flow
works for local files, remote URLs, and any resource resolved from a
catalog connector's normalized records.
