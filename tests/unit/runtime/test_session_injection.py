"""Tests for DataSluiceSession kwargs + injectables (Success Criterion 5).

Covers the ``timeout``/``retries``/``rate_limit``/``cache_dir``/``cache_ttl``
kwargs and the ``transport=``/``storage=``/``cache=``/``credential_provider=``/
``state_store=`` injectables, plus regression guards that the
``ConnectorContext`` signature is unchanged and the session exposes
no ``download()``/``materialize()`` method.
"""

from __future__ import annotations

import inspect
import os
from typing import Any

import pytest

import datasluice.application as application_module
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
    """An injected CredentialProvider wins over auth= wrapping."""

    provider = _StubCredentialProvider()
    session = DataSluiceSession(credential_provider=provider)
    assert session._credential_provider is provider
    assert isinstance(session._credential_provider, CredentialProvider)


def test_storage_injectable() -> None:
    """An injected StoragePort is stored on the session."""

    storage = _StubStorage()
    session = DataSluiceSession(storage=storage)
    assert session.storage is storage
    assert isinstance(session.storage, StoragePort)


def test_cache_injectable() -> None:
    """An injected CachePort is stored as the session cache."""

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
    """ConnectorContext fields stay exactly (base_url, transport, auth, page_size)."""

    fields = tuple(inspect.signature(ConnectorContext).parameters)
    assert fields == ("base_url", "transport", "auth", "page_size")


def test_session_has_no_download_method() -> None:
    """Sync composition does not add download or materialize facade methods."""

    session = DataSluiceSession()
    assert not hasattr(session, "download")
    assert not hasattr(session, "materialize")


class _CloseSpy:
    def __init__(self, error: BaseException | None = None) -> None:
        self._error = error
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1
        if self._error is not None:
            raise self._error


class _OwnedSession:
    def __init__(self) -> None:
        self._transport = _CloseSpy(RuntimeError("transport close failed"))
        self._cache = _CloseSpy()
        self.storage = _CloseSpy()
        self.state_store = _CloseSpy()
        self.plugins = _CloseSpy()


@pytest.mark.skipif(
    os.environ.get("DATASLUICE_TDD_RED") == "1", reason="owned-cleanup implementation pending GREEN phase"
)
def test_facade_closes_each_owned_dependency_once_and_preserves_first_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Facade-created dependencies are all closed, even when one close fails."""
    session = _OwnedSession()
    reader = _CloseSpy()
    monkeypatch.setattr(application_module, "DataSluiceSession", lambda **kwargs: session)
    monkeypatch.setattr(application_module, "DataPlaneResourceReader", lambda **kwargs: reader)
    data_sluice = application_module.DataSluice()

    with pytest.raises(RuntimeError, match="transport close failed"):
        data_sluice.close()
    data_sluice.close()

    assert reader.close_calls == 1
    assert session._transport.close_calls == 1
    assert session._cache.close_calls == 1
    assert session.storage.close_calls == 1
    assert session.state_store.close_calls == 1
    assert session.plugins.close_calls == 1


@pytest.mark.skipif(
    os.environ.get("DATASLUICE_TDD_RED") == "1", reason="owned-cleanup implementation pending GREEN phase"
)
def test_facade_leaves_injected_dependencies_open() -> None:
    """Caller-provided session and reader dependencies remain borrowed."""
    session = _OwnedSession()
    reader = _CloseSpy()
    data_sluice = application_module.DataSluice(session=session, reader=reader)

    data_sluice.close()

    assert reader.close_calls == 0
    assert session._transport.close_calls == 0
    assert session._cache.close_calls == 0
    assert session.storage.close_calls == 0
    assert session.state_store.close_calls == 0
    assert session.plugins.close_calls == 0
