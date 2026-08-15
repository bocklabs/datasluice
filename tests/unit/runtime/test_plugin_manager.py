"""Unit tests for the plugin manager and plugin failure record.

Covers entry-point discovery of namespaced canonical connector IDs, explicit
activation of registered factories, per-entry error isolation, and the
injected-PluginManager (never module-level singleton) contract.
"""

from __future__ import annotations

import dataclasses
import types
from collections.abc import Callable
from typing import Any, cast

import pytest

from datasluice.exceptions import AdapterNotFoundError
from datasluice.runtime.plugin_manager import PluginFailure, PluginManager

CANONICAL_CONNECTOR_IDS = frozenset({"datasluice/ckan", "datasluice/udata", "datasluice/socrata"})


def test_entry_point_discovery_lists_namespaced_canonical_ids() -> None:
    pm = PluginManager()
    connectors = set(pm.list_connectors())
    assert CANONICAL_CONNECTOR_IDS <= connectors
    assert "datagouv" not in connectors
    assert "ckan" not in connectors
    assert "socrata" not in connectors


def test_discovery_never_activates_loaded_factories(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def recording_factory(context: object) -> str:
        calls.append("activated")
        return "connector"

    ep = types.SimpleNamespace(name="acme/archive", load=lambda: recording_factory)
    monkeypatch.setattr(
        "datasluice.runtime.plugin_manager.entry_points",
        lambda group=None: [ep],
    )

    pm = PluginManager()

    assert calls == []
    assert pm.get("acme/archive") is recording_factory
    assert calls == []


def test_programmatic_registration_uses_namespaced_ids_and_explicit_activation() -> None:
    pm = PluginManager()
    calls: list[object] = []

    def factory(context: object) -> str:
        calls.append(context)
        return "activated-connector"

    pm.register("acme/inventory", factory)
    assert "acme/inventory" in pm.list_connectors()
    assert calls == []

    activated = cast("Callable[[object], str]", pm.get("acme/inventory"))
    connector = activated({"context": True})
    assert connector == "activated-connector"
    assert calls == [{"context": True}]


def test_former_connector_name_is_not_implicitly_activated() -> None:
    pm = PluginManager.__new__(PluginManager)
    pm._factories = {name: lambda ctx: "connector" for name in CANONICAL_CONNECTOR_IDS}
    pm._failures = []

    with pytest.raises(AdapterNotFoundError):
        pm.get("datagouv")


def test_get_unknown_raises() -> None:
    pm = PluginManager()
    with pytest.raises(AdapterNotFoundError):
        pm.get("nonexistent")


def test_list_failures_empty_when_clean() -> None:
    pm = PluginManager.__new__(PluginManager)
    pm._factories = {"datasluice/ckan": lambda ctx: None}
    pm._failures = []
    assert pm.list_failures() == []


def test_error_isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    def _broken_load() -> Any:
        raise ImportError("missing dependency")

    broken_ep = types.SimpleNamespace(name="acme/broken", load=_broken_load)

    monkeypatch.setattr(
        "datasluice.runtime.plugin_manager.entry_points",
        lambda group=None: [broken_ep],
    )

    pm = PluginManager()
    assert "acme/broken" not in pm.list_connectors()
    failures = pm.list_failures()
    assert len(failures) == 1
    assert failures[0].name == "acme/broken"
    assert "missing dependency" in failures[0].error

    pm.register("acme/resilient", lambda ctx: "ok")
    assert "acme/resilient" in pm.list_connectors()


def test_duplicate_entry_point_recorded_as_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    dup_a = types.SimpleNamespace(name="acme/dup", load=lambda: lambda ctx: "a")
    dup_b = types.SimpleNamespace(name="acme/dup", load=lambda: lambda ctx: "b")
    monkeypatch.setattr(
        "datasluice.runtime.plugin_manager.entry_points",
        lambda group=None: [dup_a, dup_b],
    )
    pm = PluginManager()
    failures = pm.list_failures()
    assert len(failures) == 1
    assert failures[0].name == "acme/dup"
    assert "duplicate" in failures[0].error


def test_plugin_failure_is_frozen() -> None:
    failure = PluginFailure("test", "error")
    with pytest.raises(dataclasses.FrozenInstanceError):
        failure.name = "other"  # ty: ignore[invalid-assignment]: asserts frozen dataclass raises at runtime


def test_get_error_message_lists_available() -> None:
    pm = PluginManager.__new__(PluginManager)
    pm._factories = {"datasluice/ckan": lambda ctx: None, "datasluice/socrata": lambda ctx: None}
    pm._failures = []
    with pytest.raises(AdapterNotFoundError) as excinfo:
        pm.get("missing")
    message = str(excinfo.value)
    assert "missing" in message
    assert "datasluice/ckan" in message
    assert "datasluice/socrata" in message


def test_list_failures_returns_copy() -> None:
    pm = PluginManager.__new__(PluginManager)
    pm._factories = {}
    pm._failures = []
    first = pm.list_failures()
    first.append(PluginFailure("mutation", "should not persist"))
    assert pm.list_failures() == []
