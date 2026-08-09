"""Tests for ``DataSluiceSession.portal(url, portal_type=)``.

Covers the four behaviours required by -03 :

* ``portal_type=`` bypass does NOT invoke ``detect()``;
* detection failure raises enriched ``PortalDetectionError`` carrying
  ``.detection_result`` (the ``DetectionResult`` with evidence);
* detection success returns a connector without raising;
* ``detect()`` receives the session's own transport and plugin_manager
.
"""

from __future__ import annotations

from typing import Any

import pytest

import datasluice.discovery.detector as detector_mod
from datasluice.discovery.detector import detect as real_detect
from datasluice.domain.detection import DetectionEvidence, DetectionResult
from datasluice.exceptions import PortalDetectionError
from datasluice.runtime.session import DataSluiceSession


class _StubTransport:
    """Transport stub that scripts hits/misses and records every probe."""

    def __init__(self, *, hit_path: str | None = None, raise_factory: Any = None) -> None:
        self.hit_path = hit_path
        self.raise_factory = raise_factory
        self.requested: list[str] = []

    def request(
        self,
        url: str,
        *,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
    ) -> bytes:
        self.requested.append(url)
        if self.raise_factory is not None:
            raise self.raise_factory(url)
        if self.hit_path is not None and self.hit_path in url:
            return b'{"ok": true}'
        raise OSError(f"miss {url}")

    def get_json(self, url: str, **kwargs: Any) -> dict[str, Any]:
        return {}

    def download(self, url: str, **kwargs: Any) -> bytes:
        return b""


class _CkanOnlyPluginManager:
    """PluginManager stub listing only ``ckan`` + a factory returning a sentinel."""

    def __init__(self) -> None:
        self._factory_called = False

    def list_connectors(self) -> list[str]:
        return ["ckan"]

    def get(self, portal_type: str) -> Any:
        def _factory(ctx: Any) -> Any:
            self._factory_called = True
            return object()

        return _factory


def _session_with(transport: _StubTransport) -> DataSluiceSession:
    plugins = _CkanOnlyPluginManager()
    return DataSluiceSession(transport=transport, plugins=plugins)  # ty: ignore[invalid-argument-type]


def test_portal_type_override_bypasses_detection(monkeypatch: pytest.MonkeyPatch) -> None:
    """``portal(url, portal_type="ckan")`` must NOT call ``detect()``."""
    session = _session_with(_StubTransport())

    def _explode(*args: Any, **kwargs: Any) -> DetectionResult:
        raise AssertionError("session.portal(portal_type=) bypassed detection, but detect() was called")

    monkeypatch.setattr(detector_mod, "detect", _explode)
    connector = session.portal("https://example.gov", portal_type="ckan")
    assert connector is not None


def test_detection_failure_raises_enriched_portal_detection_error() -> None:
    """Every probe raising ``PortalError`` → ``PortalDetectionError`` w/ ``.detection_result``."""
    from datasluice.exceptions import PortalError

    session = _session_with(_StubTransport(raise_factory=lambda url: PortalError(f"boom {url}")))
    with pytest.raises(PortalDetectionError) as exc_info:
        session.portal("https://example.gov")
    result = exc_info.value.detection_result
    assert isinstance(result, DetectionResult)
    assert result.portal_type is None
    assert len(result.evidence) > 0
    assert all(not ev.matched for ev in result.evidence)


def test_detection_success_returns_connector_without_raising() -> None:
    """CKAN-shaped hit yields a connector instance and no exception."""
    session = _session_with(_StubTransport(hit_path="/api/3/action/package_search"))
    connector = session.portal("https://example.gov")
    assert connector is not None


def test_session_passes_own_transport_and_plugin_manager_to_detect(monkeypatch: pytest.MonkeyPatch) -> None:
    """``detect`` must receive ``session._transport`` and ``session.plugins``."""
    transport = _StubTransport(hit_path="/api/3/action/package_search")
    session = _session_with(transport)

    captured: dict[str, Any] = {}

    def _spy_detect(url: str, *, transport: Any, plugin_manager: Any) -> DetectionResult:
        captured["url"] = url
        captured["transport"] = transport
        captured["plugin_manager"] = plugin_manager
        return DetectionResult(
            portal_type="ckan",
            confidence=1.0,
            evidence=[DetectionEvidence(check="/api/3/action/package_search", matched=True, detail=url)],
        )

    monkeypatch.setattr("datasluice.discovery.detect", _spy_detect)
    session.portal("https://example.gov")
    assert captured["transport"] is transport
    assert captured["plugin_manager"] is session.plugins


def test_confidence_below_one_raises_portal_detection_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """``confidence < 1.0`` must raise ``PortalDetectionError`` carrying the result."""
    session = _session_with(_StubTransport())

    def _low_confidence(url: str, *, transport: Any, plugin_manager: Any) -> DetectionResult:
        return DetectionResult(portal_type="ckan", confidence=0.5, evidence=[])

    monkeypatch.setattr("datasluice.discovery.detect", _low_confidence)
    with pytest.raises(PortalDetectionError) as exc_info:
        session.portal("https://example.gov")
    assert exc_info.value.detection_result is not None
    assert exc_info.value.detection_result.confidence == 0.5


# Reference the real detect import to keep tooling happy if unused.
_ = real_detect
