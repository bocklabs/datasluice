"""One-resource materialization operator for Airflow 3."""

from __future__ import annotations

from typing import Any

from airflow.providers.datasluice.hooks.datasluice import DataSluiceHook
from airflow.providers.datasluice.operators._xcom import validate_xcom_payload
from airflow.sdk import BaseOperator

from datasluice import (
    Artifact,
    CatalogResourceLocator,
    DataSluiceError,
    DirectResourceLocator,
    resource_locator_from_dict,
)


class DataSluiceMaterializeOperator(BaseOperator):
    """Materialize one locator and return one bounded Artifact dictionary."""

    template_fields = ("locator", "destination_uri", "mode", "airflow_conn_id")

    def __init__(
        self,
        *,
        locator: object,
        destination_uri: str,
        mode: str = "parquet",
        airflow_conn_id: str = "datasluice_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.locator = locator
        self.destination_uri = destination_uri
        self.mode = mode
        self.airflow_conn_id = airflow_conn_id

    def execute(self, context: Any) -> dict[str, object]:
        """Materialize one resource and validate its XCom representation."""
        del context
        locator = _validate_locator(self.locator)
        if not isinstance(self.destination_uri, str) or not self.destination_uri:
            raise DataSluiceError("DataSluice materialize destination_uri must be a non-empty string")
        if not isinstance(self.mode, str) or self.mode not in {"parquet", "raw"}:
            raise DataSluiceError("DataSluice materialize mode must be parquet or raw")

        hook = DataSluiceHook(airflow_conn_id=self.airflow_conn_id)
        artifact = hook.materialize(locator, self.destination_uri, mode=self.mode)
        if not isinstance(artifact, Artifact):
            raise DataSluiceError("DataSluice materialize must return one Artifact")
        return validate_xcom_payload(artifact.to_dict())


def _validate_locator(value: object) -> DirectResourceLocator | CatalogResourceLocator:
    if not isinstance(value, dict):
        raise DataSluiceError("DataSluice materialize requires exactly one locator dictionary")
    locator = resource_locator_from_dict(value)
    if not isinstance(locator, (DirectResourceLocator, CatalogResourceLocator)):
        raise DataSluiceError("DataSluice materialize requires one ResourceLocator")
    return locator
