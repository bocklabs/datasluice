"""Airflow operators for DataSluice."""

from airflow.providers.datasluice.operators.materialize import DataSluiceMaterializeOperator

__all__ = ["DataSluiceMaterializeOperator"]
