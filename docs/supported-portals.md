# Supported Portals

DataSluice supports the following open-data portal platforms out of the box.

## CKAN

CKAN is the world's most widely deployed open-data platform.

- **Software:** [CKAN](https://ckan.org/)
- **API:** CKAN Action API (`/api/3/action/`)
- **Notable portals:** data.gov, data.gov.uk, opendata.swiss, European Data Portal

```python
from datasluice import DataSluice

ds = DataSluice("https://catalog.data.gov")
```

## data.gouv.fr

The French national open-data platform.

- **Software:** [udata](https://github.com/opendatateam/udata)
- **API:** udata REST API (`/api/1/`)
- **Portal:** [data.gouv.fr](https://www.data.gouv.fr/)

```python
from datasluice import DataSluice

ds = DataSluice("https://www.data.gouv.fr")
```

## Socrata

Socrata (now part of Tyler Technologies) powers many US government open-data
portals.

- **Software:** Socrata / Tyler Technologies Socrata
- **API:** Socrata Open Data API (SODA2)
- **Notable portals:** data.cityofchicago.org, data.ny.gov, data.seattle.gov

```python
from datasluice import DataSluice

ds = DataSluice("https://data.cityofchicago.org")
```

## Auto-detection

If you are unsure which platform a portal uses, DataSluice can detect it
automatically:

```python
from datasluice import DataSluice

ds = DataSluice("https://data.example.gov")  # type auto-detected
```

## Adding support for new portals

See [Adapters](adapters.md) for instructions on writing a custom adapter.
