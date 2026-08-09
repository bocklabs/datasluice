"""Unit tests for the DataSluiceSession composition root.

Covers zero-config construction, explicit auth, page_size
wiring, transport Protocol conformance, PluginManager
injection, and repr safety.
"""

from __future__ import annotations

from datasluice.auth import NoAuth
from datasluice.config.defaults import DEFAULT_PAGE_SIZE
from datasluice.ports import Transport
from datasluice.runtime import PluginManager
from datasluice.runtime.session import DataSluiceSession


def test_zero_config_construction() -> None:
    s = DataSluiceSession()
    assert s.auth is not None
    assert isinstance(s.auth, NoAuth)
    assert s.page_size == DEFAULT_PAGE_SIZE
    assert s.plugins is not None
    assert s._transport is not None


def test_page_size_param() -> None:
    s = DataSluiceSession(page_size=50)
    assert s.page_size == 50


def test_auth_param() -> None:
    auth = NoAuth()
    s = DataSluiceSession(auth=auth)
    assert isinstance(s.auth, NoAuth)
    assert s.auth is auth


def test_transport_satisfies_protocol() -> None:
    s = DataSluiceSession()
    assert isinstance(s._transport, Transport)


def test_plugins_is_plugin_manager() -> None:
    s = DataSluiceSession()
    assert isinstance(s.plugins, PluginManager)


def test_repr_has_no_secrets() -> None:
    s = DataSluiceSession()
    text = repr(s).lower()
    assert "token" not in text
    assert "key" not in text
    assert "password" not in text
