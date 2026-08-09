"""Dependency-injection tests — detect consumes caller-injected infra.

The detector must NOT construct its own ``HttpClient`` or ``PluginManager``;
both are passed as arguments. The legacy ``detect_portal_type`` anti-pattern
(internal construction at detector.py:42-46) is gone.
"""

from __future__ import annotations

from typing import Any

import pytest

import datasluice.runtime.plugin_manager as pm_mod
import datasluice.transport as transport_mod
from datasluice.discovery.detector import detect
from datasluice.discovery.fingerprints import PATH_FINGERPRINTS


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

    def get_json(self, url: str, **kwargs: Any) -> dict[str, Any]:
        return {}

    def download(self, url: str, **kwargs: Any) -> bytes:
        return b""


class _StubPM:
    """Minimal PluginManager stub — only ``list_connectors()`` is exercised."""

    def __init__(self, names: list[str]) -> None:
        self._names = list(names)

    def list_connectors(self) -> list[str]:
        return sorted(self._names)


def test_detect_does_not_instantiate_httpclient(monkeypatch: pytest.MonkeyPatch) -> None:
    """``HttpClient`` must NOT be constructed inside detect."""

    def _explode(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("detect() constructed an HttpClient")

    monkeypatch.setattr(transport_mod, "HttpClient", _explode)
    detect("https://example.gov", _StubTransport(), _StubPM(["ckan"]))  # ty: ignore[invalid-argument-type]


def test_detect_does_not_instantiate_pluginmanager(monkeypatch: pytest.MonkeyPatch) -> None:
    """``PluginManager`` must NOT be constructed inside detect."""

    def _explode(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("detect() constructed a PluginManager")

    monkeypatch.setattr(pm_mod, "PluginManager", _explode)
    detect("https://example.gov", _StubTransport(), _StubPM(["ckan"]))  # ty: ignore[invalid-argument-type]


def test_detect_uses_caller_transport() -> None:
    """The caller's transport receives every probe request."""

    class _Recording:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def request(self, url: str, **kwargs: Any) -> bytes:
            self.calls.append(url)
            raise OSError("miss")

        def get_json(self, url: str, **kwargs: Any) -> dict[str, Any]:
            return {}

        def download(self, url: str, **kwargs: Any) -> bytes:
            return b""

    transport = _Recording()
    detect("https://example.gov", transport, _StubPM(["ckan"]))  # ty: ignore[invalid-argument-type]
    ckan_paths = [path for path, ptype in PATH_FINGERPRINTS.items() if ptype == "ckan"]
    assert len(ckan_paths) == 2
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

        def get_json(self, url: str, **kwargs: Any) -> dict[str, Any]:
            raise OSError("miss")

        def download(self, url: str, **kwargs: Any) -> bytes:
            raise OSError("miss")

    r1 = detect("https://a.example", _Miss(), _StubPM(["ckan"]))  # ty: ignore[invalid-argument-type]
    r2 = detect("https://b.example", _Miss(), _StubPM(["datagouv"]))  # ty: ignore[invalid-argument-type]

    assert {ev.check for ev in r1.evidence} == {path for path, ptype in PATH_FINGERPRINTS.items() if ptype == "ckan"}
    assert {ev.check for ev in r2.evidence} == {
        path for path, ptype in PATH_FINGERPRINTS.items() if ptype == "datagouv"
    }
