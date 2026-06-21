# Adapters

Adapters are the bridge between DataSluice's portal-agnostic domain models and
the specific APIs of individual open-data portals.

## How adapters work

Every adapter implements the `BaseAdapter` protocol defined in
`datasluice.adapters.base`. The core operations are:

| Method           | Description                                        |
|------------------|----------------------------------------------------|
| `search()`       | Search datasets matching a `Query`.                |
| `get_dataset()`  | Fetch a single dataset by ID.                      |
| `list_resources()`| Enumerate downloadable resources for a dataset.  |
| `get_organization()`| Fetch publisher/organization metadata.          |

Each adapter consists of four modules:

- **`adapter.py`** — the adapter class implementing `BaseAdapter`.
- **`mapper.py`** — translates portal-native JSON into domain models.
- **`pagination.py`** — handles portal-specific pagination.
- **`errors.py`** — maps portal errors to DataSluice exceptions.

## Built-in adapters

- **CKAN** — powers data.gov, data.gov.uk, European Data Portal, and hundreds
  of government portals worldwide.
- **data.gouv.fr** — the French national open-data platform.
- **Socrata** — powers many US city, county, and state open-data portals.

## Custom adapters

Implement `BaseAdapter` and register it to support any portal:

```python
from datasluice.adapters import BaseAdapter, registry

class MyAdapter(BaseAdapter):
    ...

registry.register("my_portal", MyAdapter)
```

See [Architecture](architecture.md) for how adapters fit into the overall design.
