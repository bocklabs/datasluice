"""Lazy selection of the base and optional HTTP transports."""

from __future__ import annotations

import importlib.util

from datasluice.runtime.transport.httpx_transport import AsyncHttpxCatalogTransport, HttpxCatalogTransport
from datasluice.runtime.transport.urllib_transport import UrllibCatalogTransport


def create_sync_transport() -> UrllibCatalogTransport | HttpxCatalogTransport:
    """Create the richest available synchronous transport without eager httpx import."""
    if importlib.util.find_spec("httpx") is None:
        return UrllibCatalogTransport()
    return HttpxCatalogTransport()


def create_async_transport() -> AsyncHttpxCatalogTransport:
    """Create the optional asynchronous transport or explain how to enable it."""
    if importlib.util.find_spec("httpx") is None:
        raise ImportError("Async catalog clients require the HTTP extra: install datasluice[http].")
    return AsyncHttpxCatalogTransport()
