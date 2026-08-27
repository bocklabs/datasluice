"""Tests for optional-extras execution boundaries."""

from __future__ import annotations

import importlib
import importlib.util
import sys

import pytest


@pytest.mark.parametrize("platform", ("ckan", "udata", "socrata"))
def test_connector_contract_imports_without_its_extra(monkeypatch: pytest.MonkeyPatch, platform: str) -> None:
    """Connector contract packages remain importable in a bare installation."""
    monkeypatch.setitem(sys.modules, "httpx", None)
    sys.modules.pop(f"datasluice.connectors.catalog.{platform}", None)
    importlib.import_module(f"datasluice.connectors.catalog.{platform}")


@pytest.mark.parametrize("platform", ("socrata",))
def test_live_client_requires_its_connector_extra(monkeypatch: pytest.MonkeyPatch, platform: str) -> None:
    """Stubbed live construction reports the exact missing platform extra."""
    live = importlib.import_module(f"datasluice.connectors.catalog.{platform}.live")
    monkeypatch.setattr("datasluice.runtime.extras.importlib.util.find_spec", lambda _: None)

    with pytest.raises(ImportError, match=rf"datasluice\[{platform}\]"):
        live.create_live_client()


@pytest.mark.parametrize("platform", ("socrata",))
def test_live_client_reaches_future_implementation_after_extra_gate(
    monkeypatch: pytest.MonkeyPatch, platform: str
) -> None:
    """A satisfied extra reaches the future-client marker instead of the gate."""
    live = importlib.import_module(f"datasluice.connectors.catalog.{platform}.live")
    monkeypatch.setattr(live, "require_extra", lambda _: None)

    with pytest.raises(NotImplementedError, match="not implemented"):
        live.create_live_client()


def test_udata_live_seam_reports_retired_no_argument_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The shipped uData live client replaces the no-argument seam with typed factories."""
    live = importlib.import_module("datasluice.connectors.catalog.udata.live")
    monkeypatch.setattr(live, "require_extra", lambda _: None)

    with pytest.raises(NotImplementedError, match="create_sync_client"):
        live.create_live_client()


def test_ckan_live_construction_requires_the_connector_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    """Both real CKAN factories enforce the published datasluice[ckan] gate."""
    from datasluice.connectors.catalog.ckan import CKANClientSettings, create_async_client, create_sync_client

    monkeypatch.setattr("datasluice.runtime.extras.importlib.util.find_spec", lambda _: None)
    settings = CKANClientSettings(base_url="https://demo.ckan.org")

    with pytest.raises(ImportError, match=r"datasluice\[ckan\]"):
        create_sync_client(settings)
    with pytest.raises(ImportError, match=r"datasluice\[ckan\]"):
        create_async_client(settings)


@pytest.mark.skipif(
    importlib.util.find_spec("httpx") is None,
    reason="the real CKAN client requires the datasluice[ckan] extra",
)
def test_ckan_live_construction_reaches_the_real_client_after_the_gate() -> None:
    """A satisfied extra constructs the real transport-backed CKAN client."""
    from datasluice.connectors.catalog.ckan import CKANClientSettings, create_sync_client
    from datasluice.runtime.transport.httpx_transport import HttpxCatalogTransport

    with create_sync_client(CKANClientSettings(base_url="https://demo.ckan.org")) as client:
        assert isinstance(client.transport, HttpxCatalogTransport)
