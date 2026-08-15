"""Unit tests for the plugin manager and plugin failure record.

Covers (entry-point discovery), (per-entry error isolation),
 (programmatic registration), and (the connector registry is an
injected PluginManager instance, never a module-level singleton).
"""

from __future__ import annotations

import dataclasses
import types
from typing import Any

import pytest

from datasluice.exceptions import AdapterNotFoundError
from datasluice.runtime.plugin_manager import PluginFailure, PluginManager


def test_entry_point_discovery() -> None:
    pm = PluginManager()
    connectors = pm.list_connectors()
    assert "datasluice/ckan" in connectors
    assert "datasluice/udata" in connectors
    assert "datasluice/socrata" in connectors


def test_programmatic_registration() -> None:
    pm = PluginManager()

    def fake_factory(ctx: object) -> str:
        return "fake"

    pm.register("fake_portal", fake_factory)
    assert "fake_portal" in pm.list_connectors()
    assert pm.get("fake_portal") is fake_factory


def test_get_unknown_raises() -> None:
    pm = PluginManager()
    with pytest.raises(AdapterNotFoundError):
        pm.get("nonexistent")


def test_list_failures_empty_when_clean() -> None:
    pm = PluginManager.__new__(PluginManager)
    pm._factories = {"ckan": lambda ctx: None}
    pm._failures = []
    assert pm.list_failures() == []


def test_error_isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    def _broken_load() -> Any:
        raise ImportError("missing dependency")

    broken_ep = types.SimpleNamespace(name="broken", load=_broken_load)

    monkeypatch.setattr(
        "datasluice.runtime.plugin_manager.entry_points",
        lambda group=None: [broken_ep],
    )

    pm = PluginManager()
    assert "broken" not in pm.list_connectors()
    failures = pm.list_failures()
    assert len(failures) == 1
    assert failures[0].name == "broken"
    assert "missing dependency" in failures[0].error

    pm.register("still_works", lambda ctx: "ok")
    assert "still_works" in pm.list_connectors()


def test_duplicate_entry_point_recorded_as_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    dup_a = types.SimpleNamespace(name="dup", load=lambda: lambda ctx: "a")
    dup_b = types.SimpleNamespace(name="dup", load=lambda: lambda ctx: "b")
    monkeypatch.setattr(
        "datasluice.runtime.plugin_manager.entry_points",
        lambda group=None: [dup_a, dup_b],
    )
    pm = PluginManager()
    failures = pm.list_failures()
    assert len(failures) == 1
    assert failures[0].name == "dup"
    assert "duplicate" in failures[0].error


def test_plugin_failure_is_frozen() -> None:
    failure = PluginFailure("test", "error")
    with pytest.raises(dataclasses.FrozenInstanceError):
        failure.name = "other"  # ty: ignore[invalid-assignment]: asserts frozen dataclass raises at runtime


def test_get_error_message_lists_available() -> None:
    pm = PluginManager.__new__(PluginManager)
    pm._factories = {"ckan": lambda ctx: None, "socrata": lambda ctx: None}
    pm._failures = []
    with pytest.raises(AdapterNotFoundError) as excinfo:
        pm.get("missing")
    message = str(excinfo.value)
    assert "missing" in message
    assert "ckan" in message
    assert "socrata" in message


def test_list_failures_returns_copy() -> None:
    pm = PluginManager.__new__(PluginManager)
    pm._factories = {}
    pm._failures = []
    first = pm.list_failures()
    first.append(PluginFailure("mutation", "should not persist"))
    assert pm.list_failures() == []
