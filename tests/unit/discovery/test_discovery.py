"""Unit tests for discovery fingerprints."""

from __future__ import annotations

from datasluice.discovery import HTML_FINGERPRINTS, PATH_FINGERPRINTS, PortalMetadata

_CANONICAL_PLATFORM_IDS = frozenset({"ckan", "udata", "socrata"})
_FORMER_CONNECTOR_NAMES = ("datagouv",)


def _fingerprint_corpus() -> str:
    parts: list[str] = [
        *PATH_FINGERPRINTS.keys(),
        *PATH_FINGERPRINTS.values(),
        *HTML_FINGERPRINTS.keys(),
        *HTML_FINGERPRINTS.values(),
    ]
    return " ".join(parts)


def test_path_fingerprints_use_exactly_canonical_platform_ids() -> None:
    assert set(PATH_FINGERPRINTS.values()) == _CANONICAL_PLATFORM_IDS


def test_html_fingerprints_use_exactly_canonical_platform_ids() -> None:
    assert set(HTML_FINGERPRINTS.values()) == _CANONICAL_PLATFORM_IDS


def test_former_connector_names_are_absent_from_fingerprints() -> None:
    corpus = _fingerprint_corpus()
    for former in _FORMER_CONNECTOR_NAMES:
        assert former not in corpus


def test_portal_metadata() -> None:
    meta = PortalMetadata(portal_type="ckan", base_url="https://example.gov")
    assert meta.portal_type == "ckan"
    assert meta.base_url == "https://example.gov"
    assert meta.title is None
