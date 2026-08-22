"""Session injection tests for non-catalog ownership and canonical catalog handoff.

Covers the ``transport=``, ``credentials=``, ``storage=``, ``cache=``, and
``state_store=`` injectables the session owns directly, plus the canonical
``open_catalog`` handoff where a caller-selected factory receives one exact
:class:`CatalogConnectorContext`.
"""

from __future__ import annotations

import asyncio
import importlib.util
from collections.abc import Callable
from datetime import date
from typing import cast

import pytest

from datasluice.contracts.catalog.protocols import (
    AsyncCatalogOperationExecutor,
    CatalogConnectorContext,
    SyncCatalogOperationExecutor,
)
from datasluice.domain.catalog.auth import CKANCredential, CredentialResolver, CredentialSource, SecretValue
from datasluice.domain.catalog.observability import TLSPolicy
from datasluice.domain.catalog.operations import (
    Atomicity,
    AuthClass,
    CapabilityClass,
    ConcurrencyRequirement,
    Idempotency,
    MutationClass,
    OperationId,
    OperationSpec,
    OperationTier,
)
from datasluice.domain.catalog.profiles import DeclaredCapabilityProfile
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.runtime.session import DataSluiceSession
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse, TransportFailure
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


class _ClosingSpyTransport(_StubTransport):
    """Stub recording whether close() was invoked."""

    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _AsyncClosingSpyTransport:
    """Async stub recording whether aclose() was invoked."""

    def __init__(self) -> None:
        self.closed = False

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        return RuntimeResponse(200, {}, b"{}")

    async def aclose(self) -> None:
        self.closed = True


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


def _minimal_profile() -> DeclaredCapabilityProfile:
    """Build one minimal declared profile for client construction."""
    operation_id = OperationId(platform="ckan", service="api", method="package_list")
    spec = OperationSpec(
        id=operation_id,
        tier=OperationTier.NATIVE,
        request_type="catalog-request",
        response_type="catalog-response",
        auth_class=AuthClass.PUBLIC,
        mutation_class=MutationClass.READ,
        idempotency=Idempotency.SAFE,
        concurrency=ConcurrencyRequirement.NONE,
        atomicity=Atomicity.NONE,
        capability_class=CapabilityClass.CORE,
    )
    return DeclaredCapabilityProfile(
        profile_version="1.0.0",
        schema_version="1.0",
        platform_api_version="1.0",
        official_source_uri="https://data.example.test/api",
        source_accessed_at=date(2026, 1, 1),
        fixture_fingerprint="sha256:deadbeef",
        operations={operation_id: spec},
    )


def test_custom_transport_injection() -> None:
    """A user-supplied catalog transport is wired without code modification."""
    stub = _StubTransport()
    session = DataSluiceSession(transport=stub)
    assert session._transport is stub


def test_runtime_options_do_not_replace_an_injected_transport() -> None:
    """When transport= is injected, runtime knobs do not construct another transport."""
    stub = _StubTransport()
    session = DataSluiceSession(
        transport=stub,
        budget=TimeBudget(connect=1.0, read=2.0, write=3.0, total=4.0),
        tls_policy=TLSPolicy(verify=False, override_scope="development"),
    )
    assert session._transport is stub


def test_credential_resolver_is_explicit_only_by_default() -> None:
    """The default resolver carries no explicit credential and discovers nothing."""
    session = DataSluiceSession()
    resolver = session.credentials
    assert resolver.explicit is None
    discovered = {CredentialSource.ENVIRONMENT: CKANCredential(api_token=SecretValue("discovered-secret"))}
    assert resolver.resolve(discovered) is None


def test_credentials_injection_identity() -> None:
    """An injected CredentialResolver is retained by identity."""
    resolver = CredentialResolver()
    session = DataSluiceSession(credentials=resolver)
    assert session.credentials is resolver


def test_session_created_client_close_keeps_session_transport_open() -> None:
    """Session-created clients borrow the transport; closing them never closes it."""
    stub = _ClosingSpyTransport()
    session = DataSluiceSession(transport=stub)
    client = session.sync_client(_minimal_profile())

    assert client.transport is stub
    client.close()
    assert stub.closed is False
    assert session._transport is stub


def test_async_client_reuses_one_cached_transport() -> None:
    """Repeated async_client() calls share a single session-owned transport."""
    spy = _AsyncClosingSpyTransport()
    session = DataSluiceSession(async_transport=spy)
    first = session.async_client(_minimal_profile())
    second = session.async_client(_minimal_profile())

    assert first.transport is spy
    assert second.transport is spy
    assert session._async_transport is spy

    asyncio.run(first.aclose())

    assert spy.closed is False


def test_session_caches_the_lazily_created_default_async_transport() -> None:
    """The default async transport is created once, reused across calls, and disposed by the session."""
    if importlib.util.find_spec("httpx") is None:
        pytest.skip("the default async transport requires the http extra")
    session = DataSluiceSession()
    try:
        first = session.async_client(_minimal_profile())
        second = session.async_client(_minimal_profile())

        assert first.transport is second.transport
        assert session._async_transport is not None

        async def dispose() -> None:
            await second.aclose()

        asyncio.run(dispose())
    finally:

        async def teardown() -> None:
            await session.aclose()
            await session.aclose()

        asyncio.run(teardown())


def test_session_aclose_disposes_owned_default_transports_idempotently() -> None:
    """Repeated aclose/close calls dispose owned default transports exactly once."""
    if importlib.util.find_spec("httpx") is None:
        pytest.skip("the default transports require the http extra")
    session = DataSluiceSession()
    _ = session.sync_client(_minimal_profile())
    _ = session.async_client(_minimal_profile())

    async def dispose_twice() -> None:
        await session.aclose()
        await session.aclose()

    asyncio.run(dispose_twice())

    with pytest.raises(TransportFailure, match="closed"):
        session._transport.send(RuntimeRequest("GET", "https://example.test/"))
    if session._async_transport is not None:
        with pytest.raises(TransportFailure, match="closed"):
            asyncio.run(session._async_transport.send(RuntimeRequest("GET", "https://example.test/")))
    session.close()
    session.close()


def test_session_close_directs_to_aclose_when_owned_async_transport_is_open() -> None:
    """Sync close never bridges into async execution; it directs to aclose instead."""
    if importlib.util.find_spec("httpx") is None:
        pytest.skip("the default transports require the http extra")
    session = DataSluiceSession()
    _ = session.async_client(_minimal_profile())

    with pytest.raises(RuntimeError, match="await aclose"):
        session.close()

    async def dispose() -> None:
        await session.aclose()

    asyncio.run(dispose())
    session.close()
    session.close()


def test_session_aclose_leaves_injected_transports_open() -> None:
    """Injected transports stay borrowed: aclose never closes caller-owned dependencies."""
    sync_spy = _ClosingSpyTransport()
    async_spy = _AsyncClosingSpyTransport()
    session = DataSluiceSession(transport=sync_spy, async_transport=async_spy)

    async def dispose_twice() -> None:
        await session.aclose()
        await session.aclose()

    asyncio.run(dispose_twice())

    assert sync_spy.closed is False
    assert async_spy.closed is False


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
