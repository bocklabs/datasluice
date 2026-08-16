"""Unit tests for the DataSluiceSession composition root.

Covers zero-config construction, explicit auth, page_size wiring, transport
Protocol conformance, PluginManager injection, repr safety, and the absence of
retired catalog-resolution members on the session surface.
"""

from __future__ import annotations

import datasluice.runtime as runtime_module
from datasluice.auth import NoAuth
from datasluice.config.defaults import DEFAULT_PAGE_SIZE
from datasluice.ports import Transport
from datasluice.runtime import PluginManager
from datasluice.runtime.session import DataSluiceSession

_RETIRED_SESSION_MEMBERS = ("search", "portal", "detect", "adapters", "discover", "detect_format")


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


def test_session_surface_exposes_no_retired_catalog_members() -> None:
    s = DataSluiceSession()
    for member in _RETIRED_SESSION_MEMBERS:
        assert not hasattr(s, member), f"session must not expose retired member {member!r}"


def test_runtime_module_exports_exactly_the_composition_surface() -> None:
    assert set(runtime_module.__all__) == {
        "DataSluiceSession",
        "PluginManager",
        "PluginFailure",
        "create_default_transport",
    }


def test_session_public_callable_surface_is_exactly_the_retained_operations() -> None:
    s = DataSluiceSession()
    public_callables = {name for name in dir(s) if not name.startswith("_") and callable(getattr(s, name))}
    assert public_callables == {"open_catalog", "sync_resources"}
