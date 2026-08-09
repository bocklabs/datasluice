"""Shared fixtures for the ``datasluice.contracts`` conformance tests.

Each portal fixture loads its hand-authored fixture set, scripts the
real-socket test HTTP server with the portal's canned response map ( —
no transport mocking, no network egress), and yields
``(server, base_url, fixture_set)``. ``fixture_set`` includes a
``"dataset_id"`` entry naming the dataset ``get_dataset`` fetches.

The search endpoint is served as a consumed-in-order ``MockResponse`` list
matching :func:`~datasluice.contracts.run_contract_suite`'s documented call
sequence: full page (check 4), first page (check 6), second page (check 6),
full page again (check 8). Socrata's single-path design (search and
``get_dataset`` both hit ``/api/catalog/v1``) scripts one interleaved list:
single (check 3), full (check 4), single (check 5), single (check 5), page 1
(check 6), page 2 (check 6), full (check 8).
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.helpers.http_server import MockResponse, _CapturingServer, start_test_server

_FIXTURES_ROOT = Path(__file__).resolve().parents[2] / "fixtures"

_ResponseMap = dict[str, MockResponse | list[MockResponse]]


def _load_fixtures(directory: Path, names: list[str]) -> dict[str, Any]:
    """Load ``{name}.json`` fixtures under *directory* into a keyed dict."""
    return {name: json.loads((directory / f"{name}.json").read_text(encoding="utf-8")) for name in names}


def _json_body(payload: Any) -> bytes:
    return json.dumps(payload).encode()


def _ckan_responses(fixture_set: dict[str, Any]) -> _ResponseMap:
    """Script the CKAN Action API responses for the conformance suite."""
    result = fixture_set["package_search"]["result"]
    results = result["results"]
    count = result["count"]
    page1 = {"success": True, "result": {"count": count, "results": results[:2]}}
    page2 = {"success": True, "result": {"count": count, "results": results[2:4]}}
    full = _json_body(fixture_set["package_search"])
    return {
        "/api/3/action/package_search": [
            MockResponse(status=200, body=full),
            MockResponse(status=200, body=_json_body(page1)),
            MockResponse(status=200, body=_json_body(page2)),
            MockResponse(status=200, body=full),
        ],
        "/api/3/action/package_show": MockResponse(status=200, body=_json_body(fixture_set["package_show"])),
        "/api/3/action/organization_show": MockResponse(status=200, body=_json_body(fixture_set["organization_show"])),
    }


def _datagouv_responses(fixture_set: dict[str, Any]) -> _ResponseMap:
    """Script the udata REST API responses for the conformance suite."""
    page = fixture_set["datasets"]
    data = page["data"]
    total = page["total"]
    page1 = {**page, "data": data[:2], "total": total}
    page2 = {**page, "data": data[2:4], "total": total}
    full = _json_body(page)
    org_slug = fixture_set["organization"]["slug"]
    return {
        "/api/1/datasets/": [
            MockResponse(status=200, body=full),
            MockResponse(status=200, body=_json_body(page1)),
            MockResponse(status=200, body=_json_body(page2)),
            MockResponse(status=200, body=full),
        ],
        f"/api/1/datasets/{fixture_set['dataset_id']}/": MockResponse(
            status=200, body=_json_body(fixture_set["dataset"])
        ),
        f"/api/1/organizations/{org_slug}/": MockResponse(status=200, body=_json_body(fixture_set["organization"])),
    }


def _socrata_responses(fixture_set: dict[str, Any]) -> _ResponseMap:
    """Script the Socrata Discovery API responses for the conformance suite.

    Search and ``get_dataset`` share the single ``/api/catalog/v1`` path, so
    the list interleaves in the suite's documented call order.
    """
    catalog = fixture_set["catalog_v1"]
    results = catalog["results"]
    size = catalog["resultSetSize"]
    page1 = {**catalog, "results": results[:2], "resultSetSize": size}
    page2 = {**catalog, "results": results[2:4], "resultSetSize": size}
    full = _json_body(catalog)
    single = _json_body(fixture_set["catalog_v1_single"])
    return {
        "/api/catalog/v1": [
            MockResponse(status=200, body=single),
            MockResponse(status=200, body=full),
            MockResponse(status=200, body=single),
            MockResponse(status=200, body=single),
            MockResponse(status=200, body=_json_body(page1)),
            MockResponse(status=200, body=_json_body(page2)),
            MockResponse(status=200, body=full),
        ],
    }


@pytest.fixture
def ckan_server() -> Iterator[tuple[_CapturingServer, str, dict[str, Any]]]:
    """Serve the CKAN fixture set over a real socket for the conformance suite."""
    fixture_set = _load_fixtures(_FIXTURES_ROOT / "ckan", ["package_search", "package_show", "organization_show"])
    fixture_set["dataset_id"] = fixture_set["package_show"]["result"]["id"]
    server, base = start_test_server(_ckan_responses(fixture_set))
    try:
        yield server, base, fixture_set
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def datagouv_server() -> Iterator[tuple[_CapturingServer, str, dict[str, Any]]]:
    """Serve the data.gouv fixture set over a real socket for the conformance suite."""
    fixture_set = _load_fixtures(_FIXTURES_ROOT / "datagouv", ["datasets", "dataset", "organization"])
    fixture_set["dataset_id"] = fixture_set["dataset"]["id"]
    server, base = start_test_server(_datagouv_responses(fixture_set))
    try:
        yield server, base, fixture_set
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def socrata_server() -> Iterator[tuple[_CapturingServer, str, dict[str, Any]]]:
    """Serve the Socrata fixture set over a real socket for the conformance suite."""
    fixture_set = _load_fixtures(_FIXTURES_ROOT / "socrata", ["catalog_v1", "catalog_v1_single"])
    fixture_set["dataset_id"] = fixture_set["catalog_v1_single"]["results"][0]["resource"]["id"]
    server, base = start_test_server(_socrata_responses(fixture_set))
    try:
        yield server, base, fixture_set
    finally:
        server.shutdown()
        server.server_close()
