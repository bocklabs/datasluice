"""Dependency-injection tests — detect() consumes caller-injected infra (D-P5-16).

The detector must NOT construct its own ``HttpClient`` or ``PluginManager``;
both are passed as arguments. The legacy ``detect_portal_type`` anti-pattern
(internal construction at detector.py:42-46) is gone.
"""

from __future__ import annotations

from typing import Any

import pytest

try:
    from datasluice.discovery.detector import detect  # ty: ignore[unresolved-import]
except ImportError as _exc:  # pragma: no cover - RED phase only
    pytest.skip(f"detect() not yet implemented: {_exc}", allow_module_level=True)

import datasluice.runtime.plugin_manager as pm_mod
import datasluice.transport as transport_mod
from datasluice.runtime.plugin_manager import PluginManager


class _StubTransport:
    def request(
        self,
        url: str,
        *,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
    ) -> bytes:
        raise OSError(f"miss {url}")


def _pm() -> PluginManager:
    pm = PluginManager()
    pm.register("ckan", lambda ctx: None)
    return pm


def test_detect_does_not_instantiate_httpclient(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """``HttpClient()`` must NOT be constructed inside detect() (D-P5-16)."""

    def _explode(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("detect() constructed an HttpClient — D-P5-16 violation")

    monkeypatch.setattr(transport_mod, "HttpClient", _explode)
    detect("https://example.gov", _StubTransport(), _pm())


def test_detect_does_not_instantiate_pluginmanager(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """``PluginManager()`` must NOT be constructed inside detect() (D-P5-16)."""

    def _explode(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("detect() constructed a PluginManager — D-P5-16 violation")

    monkeypatch.setattr(pm_mod, "PluginManager", _explode)
    detect("https://example.gov", _StubTransport(), _pm())


def test_detect_uses_caller_transport() -> None:
    """The caller's transport receives every probe request (D-P5-16)."""

    class _Recording:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def request(self, url: str, **kwargs: Any) -> bytes:
            self.calls.append(url)
            raise OSError("miss")

    transport = _Recording()
    detect("https://example.gov", transport, _pm())
    assert len(transport.calls) == 2
    assert all(c.startswith("https://example.gov/") for c in transport.calls)


def test_detect_is_reentrant_no_module_level_mutable_state() -> None:
    """Two detect() calls in sequence share no state (backstop truth).

    ``detect()`` is a pure function of (url, transport, plugin_manager) — it
    must not read or write module-level mutable state. This is the structural
    backstop for the concurrency truth in the plan's must_haves list.
    """

    class _Miss:
        def request(self, url: str, **kwargs: Any) -> bytes:
            raise OSError("miss")

    pm1 = PluginManager()
    pm1.register("ckan", lambda ctx: None)
    pm2 = PluginManager()
    pm2.register("datagouv", lambda ctx: None)

    r1 = detect("https://a.example", _Miss(), pm1)
    r2 = detect("https://b.example", _Miss(), pm2)

    assert {ev.check for ev in r1.evidence} == {
        path
        for path, ptype in __import__(
            "datasluice.discovery.fingerprints", fromlist=["PATH_FINGERPRINTS"]
        ).PATH_FINGERPRINTS.items()
        if ptype == "ckan"
    }
    assert {ev.check for ev in r2.evidence} == {
        path
        for path, ptype in __import__(
            "datasluice.discovery.fingerprints", fromlist=["PATH_FINGERPRINTS"]
        ).PATH_FINGERPRINTS.items()
        if ptype == "datagouv"
    }
