"""Composition root and plugin machinery for DataSluice."""

from __future__ import annotations

from datasluice.runtime.clients import AsyncCatalogClient, SyncCatalogClient
from datasluice.runtime.credentials import DiscoveryProvider
from datasluice.runtime.defaults import create_default_async_transport, create_default_sync_transport
from datasluice.runtime.plugin_manager import PluginFailure, PluginManager
from datasluice.runtime.session import DataSluiceSession

__all__ = [
    "AsyncCatalogClient",
    "DataSluiceSession",
    "DiscoveryProvider",
    "PluginFailure",
    "PluginManager",
    "SyncCatalogClient",
    "create_default_async_transport",
    "create_default_sync_transport",
]
