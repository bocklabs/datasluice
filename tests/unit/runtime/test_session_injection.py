"""Session injection tests for non-catalog ownership and canonical catalog handoff.

Covers the ``transport=``/``storage=``/``cache=``/``credential_provider=``/
``state_store=`` injectables the session owns directly, plus the canonical
``open_catalog`` handoff where a caller-selected factory receives one exact
:class:`CatalogConnectorContext`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

import pytest

from datasluice.contracts.catalog.protocols import (
    AsyncCatalogOperationExecutor,
    CatalogConnectorContext,
    SyncCatalogOperationExecutor,
)
from datasluice.domain.catalog.auth import CredentialResolver
from datasluice.runtime.session import DataSluiceSession
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse
from datasluice.sync import InMemoryStateStore


class _StubTransport:
    """User-defined catalog transport stub satisfying the runtime seam."""

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        return RuntimeResponse(200, {}, b"{}")

    def close(self) -> None:
        return None


class _StubStorage:
    """Stub satisfying the StoragePort Protocol."""

    def write(self, data: bytes, path: str) -> str:
        return f"mem://{path}"

    def read(self, path: str) -> bytes:
        return b""

    def exists(self, path: str) -> bool:
        return False


class _StubCache:
    """Stub satisfying the CachePort Protocol."""

    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}

    def get(self, key: str) -> bytes | None:
        return self.store.get(key)

    def put(self, key: str, data: bytes) -> None:
        self.store[key] = data

    def delete(self, key: str) -> None:
        self.store.pop(key, None)


class _SyncExecutor:
    """Structural sync executor double for canonical context construction."""

    def execute(self, operation: object, guard: object) -> object:
        return object()

    def close(self) -> None:
        return None


class _AsyncExecutor:
    """Structural async executor double for canonical context construction."""

    async def execute(self, operation: object, guard: object) -> object:
        return object()

    async def aclose(self) -> None:
        return None


def _catalog_context() -> CatalogConnectorContext:
    """Build one canonical context from structural executor doubles."""
    return CatalogConnectorContext(
        sync_executor=cast(SyncCatalogOperationExecutor, _SyncExecutor()),
        async_executor=cast(AsyncCatalogOperationExecutor, _AsyncExecutor()),
    )


def test_custom_transport_injection() -> None:
    """A user-supplied catalog transport is wired without code modification."""
    stub = _StubTransport()
    session = DataSluiceSession(transport=stub)
    assert session._transport is stub


def test_runtime_options_do_not_replace_an_injected_transport() -> None:
    """When transport= is injected, runtime options do not construct another transport."""
    stub = _StubTransport()
    session = DataSluiceSession(transport=stub)
    assert session._transport is stub


def test_credential_resolver_is_explicit_only_by_default() -> None:
    """The default resolver carries no credential discovery sources."""
    session = DataSluiceSession()
    assert session.credentials == CredentialResolver()


def test_storage_injectable() -> None:
    """An injected StoragePort is stored on the session."""
    storage = _StubStorage()
    session = DataSluiceSession(storage=storage)
    assert session.storage is storage


def test_cache_injectable() -> None:
    """An injected CachePort is stored as the session cache."""
    cache = _StubCache()
    session = DataSluiceSession(cache=cache)
    assert session._cache is cache


def test_state_store_injectable() -> None:
    """An injected StateStore is retained by the session."""
    state_store = InMemoryStateStore()
    session = DataSluiceSession(state_store=state_store)
    assert session.state_store is state_store


def test_session_has_no_download_method() -> None:
    """Sync composition does not add download or materialize facade methods."""
    session = DataSluiceSession()
    assert not hasattr(session, "download")
    assert not hasattr(session, "materialize")


def test_open_catalog_hands_the_exact_context_to_the_caller_factory() -> None:
    """The session delegates canonical construction once and retains no connector."""
    context = _catalog_context()
    calls: list[CatalogConnectorContext] = []
    connector = object()

    def factory(received: CatalogConnectorContext) -> object:
        calls.append(received)
        return connector

    session = DataSluiceSession(transport=_StubTransport())

    assert session.open_catalog(factory, context) is connector
    assert calls == [context]
    assert not hasattr(session, "catalogs")
    assert not hasattr(session, "_catalogs")


def test_open_catalog_rejects_non_callable_factory() -> None:
    """Catalog construction requires a callable factory."""
    session = DataSluiceSession(transport=_StubTransport())
    non_callable = cast("Callable[[CatalogConnectorContext], object]", object())
    with pytest.raises(TypeError, match="callable factory"):
        session.open_catalog(non_callable, _catalog_context())


def test_open_catalog_rejects_portal_shaped_context() -> None:
    """Only a CatalogConnectorContext is accepted; portal-shaped doubles are rejected."""

    class _PortalShapedContext:
        portal_type = "ckan"
        base_url = "https://data.example.gov"

    session = DataSluiceSession(transport=_StubTransport())
    portal_context = cast(CatalogConnectorContext, _PortalShapedContext())
    with pytest.raises(TypeError, match="CatalogConnectorContext"):
        session.open_catalog(lambda received: received, portal_context)
