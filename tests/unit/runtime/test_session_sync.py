"""Tests for session-level incremental sync composition."""

from __future__ import annotations

import importlib
import os
from collections.abc import Iterator
from typing import Any

import pytest

from datasluice.domain import SyncState
from datasluice.runtime.plugin_manager import PluginManager
from datasluice.sync import FileStateStore, InMemoryStateStore

session_module = importlib.import_module("datasluice.runtime.session")
if not hasattr(session_module, "_SESSION_SYNC_READY") and os.environ.get("DATASLUICE_TDD_RED") != "1":
    pytest.skip("session sync composition pending GREEN phase", allow_module_level=True)


class _StateStoreSpy:
    def __init__(self) -> None:
        self.values: dict[str, Any] = {}

    def get(self, key: str) -> Any | None:
        return self.values.get(key)

    def put(self, key: str, state: Any) -> None:
        self.values[key] = state

    def delete(self, key: str) -> None:
        self.values.pop(key, None)


def test_session_sync_uses_selected_state_store(monkeypatch: Any) -> None:
    from datasluice.data import access as access_module
    from datasluice.runtime.session import DataSluiceSession
    from datasluice.sync import sync as sync_module

    state_store = _StateStoreSpy()
    resources = [object()]
    reader = object()
    sentinel = iter((object(),))
    captured: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def forbidden_reader(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("caller-provided reader must suppress default reader construction")

    def sync_spy(*args: Any, **kwargs: Any) -> Iterator[Any]:
        captured.append((args, kwargs))
        return sentinel

    monkeypatch.setattr(access_module, "DataPlaneResourceReader", forbidden_reader)
    monkeypatch.setattr(sync_module, "sync_resources", sync_spy)

    session_class: Any = DataSluiceSession
    session = session_class(state_store=state_store)
    result = session.sync_resources(
        resources,
        destination_uri="file:///tmp/datasluice-sync",
        reader=reader,
        resume=True,
    )

    assert result is sentinel
    assert session.state_store is state_store
    assert len(captured) == 1
    args, kwargs = captured[0]
    assert args == (resources,)
    assert kwargs["destination_uri"] == "file:///tmp/datasluice-sync"
    assert kwargs["state_store"] is state_store
    assert kwargs["transport"] is session._transport
    assert kwargs["cache"] is session._cache
    assert kwargs["reader"] is reader
    assert kwargs["resume"] is True


def test_default_state_store_is_per_session() -> None:
    from datasluice.runtime.session import DataSluiceSession

    session_class: Any = DataSluiceSession
    first = session_class()
    second = session_class()

    assert isinstance(first.state_store, InMemoryStateStore)
    assert isinstance(second.state_store, InMemoryStateStore)
    assert first.state_store is not second.state_store


def test_omitted_state_store_never_constructs_file_store(monkeypatch: Any) -> None:
    from datasluice.runtime.session import DataSluiceSession
    from datasluice.sync import state_store as state_store_module

    def forbidden(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("omitting state_store must not construct a durable store")

    monkeypatch.setattr(state_store_module, "FileStateStore", forbidden)
    session_class: Any = DataSluiceSession
    session = session_class()

    assert isinstance(session.state_store, InMemoryStateStore)


def test_sessions_swap_file_and_memory_state_stores(monkeypatch: Any, tmp_path: Any) -> None:
    from datasluice.runtime.session import DataSluiceSession
    from datasluice.sync import sync as sync_module

    transport = object()
    plugins = PluginManager()
    reader = object()
    resources = [object()]
    destination_uri = "file:///tmp/datasluice-sync"
    file_store = FileStateStore(f"file://{tmp_path}/state")
    memory_store = InMemoryStateStore()
    captured: list[dict[str, Any]] = []

    def sync_spy(*args: Any, **kwargs: Any) -> Iterator[Any]:
        assert args == (resources,)
        captured.append(kwargs)
        return iter(())

    monkeypatch.setattr(sync_module, "sync_resources", sync_spy)

    common: dict[str, Any] = {"transport": transport, "plugins": plugins}
    file_session = DataSluiceSession(**common, state_store=file_store)
    memory_session = DataSluiceSession(**common, state_store=memory_store)

    file_session.sync_resources(resources, destination_uri=destination_uri, reader=reader)
    memory_session.sync_resources(resources, destination_uri=destination_uri, reader=reader)

    assert len(captured) == 2
    assert captured[0]["state_store"] is file_store
    assert captured[1]["state_store"] is memory_store
    for call in captured:
        assert call["transport"] is transport
        assert call["cache"] is None
        assert call["reader"] is reader
        assert call["destination_uri"] == destination_uri
        assert call["resume"] is False

    file_state = SyncState(cursor={"file-key": '"file-watermark"'})
    memory_state = SyncState(cursor={"memory": "memory-watermark"})
    file_store.put("file-key", file_state)
    memory_store.put("memory-key", memory_state)

    assert file_store.get("file-key") == file_state
    assert memory_store.get("memory-key") == memory_state
    assert file_store.get("memory-key") is None
    assert memory_store.get("file-key") is None
