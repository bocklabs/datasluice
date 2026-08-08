"""Importable Airflow 3 example using mapped DataSluice materialization tasks."""

from __future__ import annotations

from datetime import datetime

from airflow.providers.datasluice.operators.materialize import DataSluiceMaterializeOperator
from airflow.providers.datasluice.operators.search import DataSluiceSearchOperator
from airflow.sdk import DAG

with DAG(
    dag_id="datasluice_mapped_materialize",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
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
