"""Application cutover tests for explicit catalog construction."""

from __future__ import annotations

import inspect
from typing import Any, cast

import datasluice.application as application_module
from datasluice.application import DataSluice, DirectResourceLocator
from datasluice.contracts.catalog.protocols import CatalogConnectorContext


class _Session:
    _transport = object()

    def open_catalog[T](self, factory: object, context: CatalogConnectorContext) -> T:
        return cast(Any, factory)(context)


class _Reader:
    def open(self, resource: object) -> object:
        return object()


def test_application_uses_only_explicit_catalog_handoff() -> None:
    """The application delegates caller-owned factory composition unchanged."""
    context = CatalogConnectorContext(sync_executor=cast(Any, object()), async_executor=cast(Any, object()))
    expected = object()
    data_sluice = DataSluice(session=_Session(), reader=_Reader())

    assert data_sluice.open_catalog(lambda received: expected if received is context else None, context) is expected


def test_application_removes_historical_catalog_surface() -> None:
    """Portal, URL search, and catalog locators do not survive the clean break."""
    source = inspect.getsource(application_module)

    assert "CatalogResourceLocator" not in source
    assert "class Portal" not in source
    assert not hasattr(DataSluice, "portal")
    assert not hasattr(DataSluice, "search")
    assert not hasattr(DataSluice, "detect")
    assert DirectResourceLocator(uri="https://example.test/data.csv").format is None
