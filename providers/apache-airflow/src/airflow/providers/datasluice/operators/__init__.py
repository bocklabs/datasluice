"""Airflow operators for DataSluice."""

from __future__ import annotations

from typing import Any

from airflow.providers.datasluice.hooks.datasluice import DatasluiceHook
from airflow.sdk import BaseOperator

from datasluice.runtime.clients import SyncCatalogClient


class DatasluiceCatalogOperator(BaseOperator):
    """Construct a connection-scoped catalog client for later platform operators.

    Live platform actions await the canonical executors delivered in Phases 3-5.
    """

    template_fields = ("airflow_conn_id",)

    def __init__(self, *, airflow_conn_id: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.airflow_conn_id = airflow_conn_id

    def execute(self, context: Any) -> SyncCatalogClient:
        """Defer client construction to the connection-aware hook."""
        del context
        return DatasluiceHook(airflow_conn_id=self.airflow_conn_id).get_conn()


__all__ = ["DatasluiceCatalogOperator"]
