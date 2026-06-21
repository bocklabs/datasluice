"""Unit tests for discovery fingerprints."""

from __future__ import annotations

from datasluice.discovery import HTML_FINGERPRINTS, PATH_FINGERPRINTS, PortalMetadata


def test_path_fingerprints_populated() -> None:
    assert "ckan" in PATH_FINGERPRINTS.values()
    assert "datagouv" in PATH_FINGERPRINTS.values()
    assert "socrata" in PATH_FINGERPRINTS.values()


def test_html_fingerprints_populated() -> None:
    assert "ckan" in HTML_FINGERPRINTS
    assert "datagouv" in HTML_FINGERPRINTS.values()


def test_portal_metadata() -> None:
    meta = PortalMetadata(portal_type="ckan", base_url="https://example.gov")
    assert meta.portal_type == "ckan"
    assert meta.base_url == "https://example.gov"
    assert meta.title is None
