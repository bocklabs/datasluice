"""Apache Airflow integration: DataSluice operators for DAGs.

Requires ``apache-airflow``: install with ``pip install datasluice[airflow]``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from datasluice.logging import get_logger

if TYPE_CHECKING:
    from airflow.models import BaseOperator

logger = get_logger("integrations.airflow")


def _import_operator() -> type[BaseOperator]:
    """Import the Airflow BaseOperator lazily."""
    try:
        from airflow.models import BaseOperator
    except ImportError as exc:
        raise ImportError(
            "Airflow integration requires 'apache-airflow'. Install with: pip install datasluice[airflow]"
        ) from exc
    return BaseOperator


class DataSluiceOperator:
    """Factory that returns an Airflow ``BaseOperator`` subclass.

    Usage::

        from datasluice.integrations.airflow import DataSluiceOperator

        fetch = DataSluiceOperator(
            task_id="fetch_data",
            portal="https://catalog.data.gov",
            query="climate",
            dest_dir="/tmp/data",
        )
    """

    @staticmethod
    def _build(
        portal: str,
        query: str | None,
        dest_dir: str | None,
        limit: int,
        **kwargs: Any,
    ) -> type:
        BaseOperator = _import_operator()
        from datasluice.domain import Query
        from datasluice.runtime.session import DataSluiceSession

        class _Operator(BaseOperator):  # ty: ignore[unsupported-base]
            template_fields = ("portal", "query", "dest_dir")

            def __init__(self, **kw: Any) -> None:
                super().__init__(**kw)

            def execute(self, context: dict[str, Any]) -> Any:
                ds = DataSluiceSession()
                result = ds.search(portal, Query(text=query, limit=limit))
                paths: list[str] = []
                for dataset in result.datasets:
                    if dest_dir:
                        resources = dataset.resources
                        from datasluice.io.downloader import Downloader

                        downloader = Downloader(cast("Any", ds._transport))
                        paths.extend(str(p) for p in downloader.download_many(resources, dest_dir))
                return paths

        return _Operator

    def __new__(
        cls,
        task_id: str,
        portal: str,
        query: str | None = None,
        dest_dir: str | None = None,
        limit: int = 100,
        **kwargs: Any,
    ) -> Any:
        operator_cls = cls._build(portal, query, dest_dir, limit, **kwargs)
        return operator_cls(task_id=task_id, **kwargs)
