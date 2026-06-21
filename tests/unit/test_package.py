"""Tests for the top-level datasluice package."""

from __future__ import annotations

import datasluice


def test_version() -> None:
    assert datasluice.__version__ == "0.1.0"


def test_public_api_exports() -> None:
    assert hasattr(datasluice, "DataSluice")
    assert hasattr(datasluice, "Dataset")
    assert hasattr(datasluice, "Resource")
    assert hasattr(datasluice, "Organization")
    assert hasattr(datasluice, "Query")
    assert hasattr(datasluice, "SearchResult")
    assert hasattr(datasluice, "DataSluiceError")


def test_exceptions_hierarchy() -> None:
    from datasluice.exceptions import NotFoundError, PortalError

    assert issubclass(PortalError, datasluice.DataSluiceError)
    assert issubclass(NotFoundError, PortalError)
