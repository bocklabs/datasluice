"""Evidence-based detection tests — every probe recorded, any-match confidence.

Covers the four evidence semantics required by -03 :
* every probe (hit OR miss) appends a ``DetectionEvidence`` row;
* any single hit → ``portal_type`` set and ``confidence == 1.0``;
* zero hits → ``portal_type is None`` and ``confidence == 0.0``;
* ``plugin_manager.list_connectors()`` filters which portal_types are probed;
* real-socket end-to-end via ``start_test_server`` proves a CKAN-shaped hit.
"""

from __future__ import annotations

from typing import Any

from datasluice.discovery.detector import detect
from datasluice.discovery.fingerprints import PATH_FINGERPRINTS
from datasluice.domain.detection import DetectionResult


class _StubTransport:
    """Transport stub that scripts hits/misses by URL substring.

    Records every probe in ``self.requested`` so DI tests can assert the
    caller's transport was used. Implements the full
    :class:`~datasluice.ports.Transport` Protocol so ty recognises it as
    assignable (matches the pattern in ``test_session_injection.py``).
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

    def get_json(self, url: str, **kwargs: Any) -> dict[str, Any]:
        return {}

    def download(self, url: str, **kwargs: Any) -> bytes:
        return b""


class _StubPM:
    """Minimal PluginManager stub for filtered-portal tests.

    The real :class:`PluginManager` eagerly loads ALL built-in entry points
    (ckan/udata/socrata from ``pyproject.toml``), so a freshly-constructed
    instance cannot exercise the filtering contract. detect() only calls
    ``list_connectors()``, so this stub is sufficient.
    """

    def __init__(self, names: list[str]) -> None:
        self._names = list(names)

    def list_connectors(self) -> list[str]:
        return sorted(self._names)


def _pm_with_all() -> _StubPM:
    return _StubPM(["ckan", "udata", "socrata"])


def _expected_probe_count(pm: _StubPM) -> int:
    registered = set(pm.list_connectors())
    return sum(1 for portal_type in PATH_FINGERPRINTS.values() if portal_type in registered)


def test_evidence_recorded_for_every_probe() -> None:
    """Each registered (path, portal_type) probe produces one evidence row."""

    stub = _StubTransport(hit_substring="/api/3/action/package_search")
    result = detect("https://example.gov", stub, _pm_with_all())  # ty: ignore[invalid-argument-type]
    assert isinstance(result, DetectionResult)
    expected = _expected_probe_count(_pm_with_all())
    assert expected == 6, "fixture sanity: 2 paths × 3 portals = 6 probes"
    assert len(result.evidence) == 6
    fingerprint_paths = set(PATH_FINGERPRINTS.keys())
    assert {ev.check for ev in result.evidence} == fingerprint_paths


def test_any_match_yields_confidence_one() -> None:
    """A single hit pins portal_type at confidence 1.0."""

    stub = _StubTransport(hit_substring="/api/3/action/package_search")
    result = detect("https://example.gov", stub, _pm_with_all())  # ty: ignore[invalid-argument-type]
    assert result.portal_type == "ckan"
    assert result.confidence == 1.0
    matched = [ev for ev in result.evidence if ev.matched]
    assert len(matched) == 1
    assert matched[0].check == "/api/3/action/package_search"


def test_zero_matches_yields_portal_none_and_confidence_zero() -> None:
    """All probes miss → portal_type is None, confidence 0.0, evidence full."""

    stub = _StubTransport(hit_substring=None)
    result = detect("https://example.gov", stub, _pm_with_all())  # ty: ignore[invalid-argument-type]
    assert result.portal_type is None
    assert result.confidence == 0.0
    assert 0.0 <= result.confidence <= 1.0
    assert len(result.evidence) == 6
    assert all(not ev.matched for ev in result.evidence)


def test_matched_result_confidence_in_valid_range() -> None:
    """A matched detection produces confidence exactly 1.0 (in [0,1])."""
    stub = _StubTransport(hit_substring="/api/3/action/package_search")
    result = detect("https://example.gov", stub, _pm_with_all())  # ty: ignore[invalid-argument-type]
    assert result.confidence == 1.0
    assert 0.0 <= result.confidence <= 1.0


def test_plugin_manager_filters_which_portals_are_probed() -> None:
    """A plugin_manager listing only ``ckan`` produces exactly 2 CKAN probes."""

    pm = _StubPM(["ckan"])
    stub = _StubTransport(hit_substring=None)
    result = detect("https://example.gov", stub, pm)  # ty: ignore[invalid-argument-type]
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

        result = detect(base_url, HttpClient(), _pm_with_all())  # ty: ignore[invalid-argument-type]
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
    detect("catalog.data.gov/some/path", stub, _pm_with_all())  # ty: ignore[invalid-argument-type]
    assert all(u.startswith("https://catalog.data.gov/") for u in stub.requested)
    assert all("/some/path" not in u for u in stub.requested)
