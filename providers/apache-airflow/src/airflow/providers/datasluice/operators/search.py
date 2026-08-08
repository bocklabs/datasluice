"""Bounded catalog search operator for Airflow 3."""

from __future__ import annotations

from typing import Any

from airflow.providers.datasluice.hooks.datasluice import DataSluiceHook
from airflow.providers.datasluice.operators._xcom import validate_xcom_payload
from airflow.sdk import BaseOperator

from datasluice import CatalogResourceLocator, DataSluiceError, resource_locator_from_dict


class DataSluiceSearchOperator(BaseOperator):
    """Search one portal and return bounded catalog locator dictionaries."""

    template_fields = ("portal_url", "query", "max_results", "airflow_conn_id")

    def __init__(
        self,
        *,
        portal_url: str | None,
        query: str | None = None,
        max_results: int = 50,
        airflow_conn_id: str = "datasluice_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.portal_url = portal_url
        self.query = query
        self.max_results = max_results
        self.airflow_conn_id = airflow_conn_id

    def execute(self, context: Any) -> list[dict[str, object]]:
        """Execute the bounded search and validate its XCom representation."""
        del context
        limit = _validate_max_results(self.max_results)
        hook = DataSluiceHook(airflow_conn_id=self.airflow_conn_id)
        raw_locators = hook.search(self.portal_url, self.query, max_results=limit)
        if not isinstance(raw_locators, list):
            raise DataSluiceError("DataSluice search must return a list of catalog locators")
        if len(raw_locators) > limit:
            raise DataSluiceError("DataSluice search returned more locators than max_results")

        locators: list[dict[str, object]] = []
        for value in raw_locators:
            if not isinstance(value, dict):
                raise DataSluiceError("DataSluice search returned a non-dictionary locator")
            locator = resource_locator_from_dict(value)
            if not isinstance(locator, CatalogResourceLocator):
                raise DataSluiceError("DataSluice search returned a non-catalog locator")
            locators.append(locator.to_dict())
        return validate_xcom_payload(locators)


def _validate_max_results(value: object) -> int:
    if type(value) is not int or not 1 <= value <= 1000:
        raise DataSluiceError("DataSluice search max_results must be between 1 and 1000")
    return value
