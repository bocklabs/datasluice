# Apache Airflow Integration

Use DataSluice inside [Apache Airflow](https://airflow.apache.org/) by
installing the separate provider distribution
`apache-airflow-providers-datasluice` and importing from the
`airflow.providers.datasluice` namespace.

```bash
pip install apache-airflow-providers-datasluice
```

## Hook

`DataSluiceHook` maps an [Airflow connection](https://airflow.apache.org/docs/)
(default `datasluice_default`) to the DataSluice facade and exposes focused
`search()` and `materialize()` delegates.

```python
from airflow.providers.datasluice.hooks.datasluice import DataSluiceHook

hook = DataSluiceHook(airflow_conn_id="datasluice_default")
results = hook.search("open data")
artifact = hook.materialize({"portal_url": "https://catalog.example.test/api",
                             "dataset_id": "dataset-1", "resource_id": "resource-1"})
```

## Search operator

`DataSluiceSearchOperator` returns bounded, JSON-safe catalog references that
are safe for XCom.

```python
from airflow.providers.datasluice.operators.search import DataSluiceSearchOperator

search = DataSluiceSearchOperator(
    task_id="search_open_data",
    portal_url="https://catalog.example.test/api",
    query="open data",
)
```

## Materialize operator and task mapping

`DataSluiceMaterializeOperator` handles exactly one resource per task and
returns one canonical Artifact dictionary. Airflow task mapping provides
the fan-out.

```python
from datetime import datetime

from airflow.providers.datasluice.operators.materialize import DataSluiceMaterializeOperator
from airflow.providers.datasluice.operators.search import DataSluiceSearchOperator
from airflow.sdk import DAG

with DAG(
    dag_id="datasluice_mapped_materialize",
    start_date=datetime(2026, 1, 1),
    schedule=None,
) as dag:
    search = DataSluiceSearchOperator(
        task_id="search",
        portal_url="https://catalog.example.test/api",
        query="open data",
    )
    materialize = DataSluiceMaterializeOperator.partial(
        task_id="materialize",
        destination_uri="memory://datasluice/{{ ti.map_index }}/result.parquet",
        mode="parquet",
    ).expand(locator=search.output)
```

A runnable copy of this DAG ships with the provider at
`providers/apache-airflow/tests/dags/example_datasluice.py`.
