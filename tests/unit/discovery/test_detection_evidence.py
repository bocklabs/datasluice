"""Evidence-based detection tests — every probe recorded, any-match confidence (D-P5-15).

Covers the four evidence semantics required by Plan 05-03 Task 1:
* every probe (hit OR miss) appends a ``DetectionEvidence`` row;
* any single hit → ``portal_type`` set and ``confidence == 1.0``;
* zero hits → ``portal_type is None`` and ``confidence == 0.0``;
* ``plugin_manager.list_connectors()`` filters which portal_types are probed;
* real-socket end-to-end via ``start_test_server`` proves a CKAN-shaped hit.
"""

from __future__ import annotations

from typing import Any

import pytest

try:
    from datasluice.discovery.detector import detect  # ty: ignore[unresolved-import]
except ImportError as _exc:  # pragma: no cover - RED phase only
    pytest.skip(f"detect() not yet implemented: {_exc}", allow_module_level=True)

from datasluice.discovery.fingerprints import PATH_FINGERPRINTS
from datasluice.domain.detection import DetectionResult
from datasluice.runtime.plugin_manager import PluginManager


class _StubTransport:
    """Transport stub that scripts hits/misses by URL substring.

    Records every probe in ``self.requested`` so DI tests can assert the
    caller's transport was used (D-P5-16).
    """

    def __init__(self, hit_substring: str | None = None) -> None:
        self.hit_substring = hit_substring
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
        if self.hit_substring is not None and self.hit_substring in url:
            return b'{"ok": true}'
        raise OSError(f"no route to {url}")


def _pm_with_all() -> PluginManager:
    pm = PluginManager()
    pm.register("ckan", lambda ctx: None)
    pm.register("datagouv", lambda ctx: None)
    pm.register("socrata", lambda ctx: None)
    return pm


def _expected_probe_count(pm: PluginManager) -> int:
    registered = set(pm.list_connectors())
    return sum(1 for portal_type in PATH_FINGERPRINTS.values() if portal_type in registered)


def test_evidence_recorded_for_every_probe() -> None:
    """Each registered (path, portal_type) probe produces one evidence row."""

    stub = _StubTransport(hit_substring="/api/3/action/package_search")
    result = detect("https://example.gov", stub, _pm_with_all())
    assert isinstance(result, DetectionResult)
    expected = _expected_probe_count(_pm_with_all())
    assert expected == 6, "fixture sanity: 2 paths × 3 portals = 6 probes"
    assert len(result.evidence) == 6
    fingerprint_paths = set(PATH_FINGERPRINTS.keys())
    assert {ev.check for ev in result.evidence} == fingerprint_paths


def test_any_match_yields_confidence_one() -> None:
    """A single hit pins portal_type at confidence 1.0 (D-P5-15 any-match)."""

    stub = _StubTransport(hit_substring="/api/3/action/package_search")
    result = detect("https://example.gov", stub, _pm_with_all())
    assert result.portal_type == "ckan"
    assert result.confidence == 1.0
    matched = [ev for ev in result.evidence if ev.matched]
    assert len(matched) == 1
    assert matched[0].check == "/api/3/action/package_search"


def test_zero_matches_yields_portal_none_and_confidence_zero() -> None:
    """All probes miss → portal_type is None, confidence 0.0, evidence full."""

    stub = _StubTransport(hit_substring=None)
    result = detect("https://example.gov", stub, _pm_with_all())
    assert result.portal_type is None
    assert result.confidence == 0.0
    assert len(result.evidence) == 6
    assert all(not ev.matched for ev in result.evidence)


def test_plugin_manager_filters_which_portals_are_probed() -> None:
    """A plugin_manager listing only ``ckan`` produces exactly 2 CKAN probes."""

    pm = PluginManager()
    pm.register("ckan", lambda ctx: None)
    stub = _StubTransport(hit_substring=None)
    result = detect("https://example.gov", stub, pm)
    ckan_paths = [path for path, ptype in PATH_FINGERPRINTS.items() if ptype == "ckan"]
    assert len(ckan_paths) == 2
    assert len(result.evidence) == 2
    assert {ev.check for ev in result.evidence} == set(ckan_paths)


def test_real_socket_ckan_hit() -> None:
    """A real-socket CKAN-shaped endpoint yields ckan @ confidence 1.0."""

    from tests.helpers.http_server import MockResponse, start_test_server

    server, base_url = start_test_server(
        {"/api/3/action/package_search": MockResponse(status=200, body=b'{"help": "..."}')}
    )
    try:
        from datasluice.transport import HttpClient

        result = detect(base_url, HttpClient(), _pm_with_all())
        assert result.portal_type == "ckan"
        assert result.confidence == 1.0
        ckan_hits = [ev for ev in result.evidence if ev.matched and "package_search" in ev.detail]
        assert len(ckan_hits) == 1
    finally:
        server.shutdown()
        server.server_close()


def test_url_is_normalized_before_probing() -> None:
    """A scheme-less URL with a trailing path is normalized to its origin."""

    stub = _StubTransport(hit_substring=None)
    detect("catalog.data.gov/datasets", stub, _pm_with_all())
    assert all(u.startswith("https://catalog.data.gov/") for u in stub.requested)
    assert all("/datasets" not in u for u in stub.requested)
