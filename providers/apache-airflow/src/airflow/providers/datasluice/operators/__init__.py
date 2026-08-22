"""Airflow operators for DataSluice."""

from __future__ import annotations

from typing import Any

from airflow.providers.datasluice.hooks.datasluice import DatasluiceHook
from airflow.sdk import BaseOperator


class DatasluiceCatalogOperator(BaseOperator):
    """Validate connection-scoped client construction for later platform operators.

    Live platform actions await the canonical executors delivered in Phases 3-5.
    The operator never pushes its result to XCom: it builds a runtime client from
    the connection to fail fast on bad wiring, closes it immediately, and returns
    a plain serializable descriptor instead of a live client or secret material.
    """

    template_fields = ("airflow_conn_id",)
    do_xcom_push = False

    def __init__(self, *, airflow_conn_id: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.airflow_conn_id = airflow_conn_id

    def execute(self, context: Any) -> dict[str, str]:
        """Validate hook client construction and return a serializable descriptor."""
        del context
        with DatasluiceHook(airflow_conn_id=self.airflow_conn_id).get_conn():
            return {"connection": self.airflow_conn_id}


__all__ = ["DatasluiceCatalogOperator"]
