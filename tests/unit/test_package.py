"""Tests for the top-level datasluice package."""

from __future__ import annotations

import re

import pytest

import datasluice


def test_version() -> None:
    assert re.match(r"^\d+\.\d+\.\d+", datasluice.__version__)


def test_public_api_exports() -> None:
    assert hasattr(datasluice, "DataSluice")
    assert hasattr(datasluice, "Dataset")
    assert hasattr(datasluice, "Resource")
    assert hasattr(datasluice, "Organization")
    assert hasattr(datasluice, "Query")
    assert hasattr(datasluice, "SearchResult")
    assert hasattr(datasluice, "DataSluiceError")


def test_legacy_session_is_not_a_top_level_import() -> None:
    with pytest.raises(ImportError):
        exec("from datasluice import DataSluiceSession", {})


def test_exceptions_hierarchy() -> None:
    from datasluice.exceptions import NotFoundError, PortalError

    assert issubclass(PortalError, datasluice.DataSluiceError)
    assert issubclass(NotFoundError, PortalError)
