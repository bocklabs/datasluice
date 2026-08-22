"""Unit tests for the DataSluiceSession composition root.

Covers zero-config construction, backend-gated default transport selection,
PluginManager injection, repr safety, the absence of retired catalog-resolution
members on the session surface, and the pinned runtime export and public
callable surfaces.
"""

from __future__ import annotations

import importlib.util

import datasluice.runtime as runtime_module
from datasluice.domain.catalog.auth import CredentialResolver
from datasluice.runtime import PluginManager
from datasluice.runtime.session import DataSluiceSession
from datasluice.runtime.transport.httpx_transport import HttpxCatalogTransport
from datasluice.runtime.transport.urllib_transport import UrllibCatalogTransport

_RETIRED_SESSION_MEMBERS = ("search", "portal", "detect", "adapters", "discover", "detect_format")


def test_zero_config_construction() -> None:
    s = DataSluiceSession()
    assert s.credentials == CredentialResolver()
    assert s.plugins is not None
    assert s._transport is not None


def test_transport_uses_the_backend_gated_default() -> None:
    s = DataSluiceSession()
    expected = HttpxCatalogTransport if importlib.util.find_spec("httpx") is not None else UrllibCatalogTransport
    assert isinstance(s._transport, expected)


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
        "SyncCatalogClient",
        "AsyncCatalogClient",
        "DiscoveryProvider",
        "create_default_sync_transport",
        "create_default_async_transport",
    }


def test_session_public_callable_surface_is_exactly_the_retained_operations() -> None:
    s = DataSluiceSession()
    public_callables = {name for name in dir(s) if not name.startswith("_") and callable(getattr(s, name))}
    assert public_callables == {"async_client", "open_catalog", "sync_client", "sync_resources"}
