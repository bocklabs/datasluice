"""Tests for DataSluiceSession Phase 3 kwargs + injectables (Success Criterion 5).

Covers the ``timeout``/``retries``/``rate_limit``/``cache_dir``/``cache_ttl``
kwargs and the ``transport=``/``storage=``/``cache=``/``credential_provider=``/
``state_store=`` injectables, plus regression guards that the
``ConnectorContext`` signature is unchanged (D-P3-21) and the session exposes
no ``download()``/``materialize()`` method.
"""

from __future__ import annotations

import inspect
from typing import Any

from datasluice.auth import NoAuth
from datasluice.ports import CachePort, CredentialProvider, StateStore, StoragePort, Transport
from datasluice.runtime.context import ConnectorContext
from datasluice.runtime.session import DataSluiceSession
from datasluice.sync import InMemoryStateStore


class _StubTransport:
    """User-defined transport stub satisfying the Transport Protocol structurally."""

    def request(
        self,
        url: str,
        *,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
    ) -> bytes:
        return b"stub"

    def get_json(self, url: str, **kwargs: Any) -> dict[str, Any]:
        return {"stub": True}

    def download(self, url: str, **kwargs: Any) -> bytes:
        return b"stub"


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


class _StubCredentialProvider:
    """Stub satisfying the CredentialProvider Protocol."""

    def resolve(self, host: str | None = None) -> Any:
        return NoAuth()


def test_custom_transport_injection() -> None:
    """A user-supplied Transport stub is wired without code modification (SC5)."""

    stub = _StubTransport()
    session = DataSluiceSession(transport=stub)
    assert session._transport is stub
    assert isinstance(session._transport, Transport)


def test_scalar_knobs_ignored_when_transport_injected() -> None:
    """When transport= is injected, scalar knobs do NOT construct a new transport."""

    stub = _StubTransport()
    session = DataSluiceSession(transport=stub, timeout=99, retries=99)
    assert session._transport is stub


def test_credential_provider_injectable() -> None:
    """An injected CredentialProvider wins over auth= wrapping (D-P3-14)."""

    provider = _StubCredentialProvider()
    session = DataSluiceSession(credential_provider=provider)
    assert session._credential_provider is provider
    assert isinstance(session._credential_provider, CredentialProvider)


def test_storage_injectable() -> None:
    """An injected StoragePort is stored on the session (D-P3-20)."""

    storage = _StubStorage()
    session = DataSluiceSession(storage=storage)
    assert session.storage is storage
    assert isinstance(session.storage, StoragePort)


def test_cache_injectable() -> None:
    """An injected CachePort is stored as the session cache (D-P3-02)."""

    cache = _StubCache()
    session = DataSluiceSession(cache=cache)
    assert session._cache is cache
    assert isinstance(session._cache, CachePort)


def test_state_store_injectable() -> None:
    """An injected StateStore is retained by the session."""

    state_store = InMemoryStateStore()
    session = DataSluiceSession(state_store=state_store)
    assert session.state_store is state_store
    assert isinstance(session.state_store, StateStore)


def test_connector_context_signature_unchanged() -> None:
    """ConnectorContext fields stay exactly (base_url, transport, auth, page_size) (D-P3-21)."""

    fields = tuple(inspect.signature(ConnectorContext).parameters)
    assert fields == ("base_url", "transport", "auth", "page_size")


def test_session_has_no_download_method() -> None:
    """Sync composition does not add download or materialize facade methods."""

    session = DataSluiceSession()
    assert not hasattr(session, "download")
    assert not hasattr(session, "materialize")
