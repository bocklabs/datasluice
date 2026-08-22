"""Tests for optional-extras execution boundaries."""

from __future__ import annotations

import importlib

import pytest


@pytest.mark.parametrize("platform", ("ckan", "udata", "socrata"))
def test_connector_contract_imports_without_its_extra(platform: str) -> None:
    """Connector contract packages remain importable in a bare installation."""
    importlib.import_module(f"datasluice.connectors.catalog.{platform}")


@pytest.mark.parametrize("platform", ("ckan", "udata", "socrata"))
def test_live_client_requires_its_connector_extra(monkeypatch: pytest.MonkeyPatch, platform: str) -> None:
    """Live construction reports the exact missing platform extra."""
    live = importlib.import_module(f"datasluice.connectors.catalog.{platform}.live")
    monkeypatch.setattr("datasluice.runtime.extras.importlib.util.find_spec", lambda _: None)

    with pytest.raises(ImportError, match=rf"datasluice\[{platform}\]"):
        live.create_live_client()


@pytest.mark.parametrize("platform", ("ckan", "udata", "socrata"))
def test_live_client_reaches_future_implementation_after_extra_gate(
    monkeypatch: pytest.MonkeyPatch, platform: str
) -> None:
    """A satisfied extra reaches the future-client marker instead of the gate."""
    live = importlib.import_module(f"datasluice.connectors.catalog.{platform}.live")
    monkeypatch.setattr(live, "require_extra", lambda _: None)

    with pytest.raises(NotImplementedError, match="not implemented"):
        live.create_live_client()
