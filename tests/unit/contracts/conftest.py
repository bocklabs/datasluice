"""Shared fixtures for the ``datasluice.contracts`` conformance tests.

Each portal fixture loads its hand-authored fixture set, scripts the
real-socket test HTTP server with the portal's canned response map (D-P5-12 —
no transport mocking, no network egress), and yields
``(server, base_url, fixture_set)``.

The search endpoint is served as a consumed-in-order ``MockResponse`` list
matching :func:`~datasluice.contracts.run_contract_suite`'s documented call
sequence: full page (check 4), page 1 (check 6), page 2 (check 6), full page
(check 8).
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.helpers.http_server import MockResponse, _CapturingServer, start_test_server

_FIXTURES_ROOT = Path(__file__).resolve().parents[2] / "fixtures"


def _load_fixtures(directory: Path, names: list[str]) -> dict[str, Any]:
    """Load ``{name}.json`` fixtures under *directory* into a keyed dict."""
    return {name: json.loads((directory / f"{name}.json").read_text(encoding="utf-8")) for name in names}


def _json_body(payload: Any) -> bytes:
    return json.dumps(payload).encode()


def _ckan_search_sequence(fixture_set: dict[str, Any]) -> list[MockResponse]:
    """Script ``package_search`` responses in the suite's documented call order."""
    result = fixture_set["package_search"]["result"]
    results = result["results"]
    count = result["count"]
    page1 = {"success": True, "result": {"count": count, "results": results[:2]}}
    page2 = {"success": True, "result": {"count": count, "results": results[2:4]}}
    full = _json_body(fixture_set["package_search"])
    return [
        MockResponse(status=200, body=full),
        MockResponse(status=200, body=_json_body(page1)),
        MockResponse(status=200, body=_json_body(page2)),
        MockResponse(status=200, body=full),
    ]


@pytest.fixture
def ckan_server() -> Iterator[tuple[_CapturingServer, str, dict[str, Any]]]:
    """Serve the CKAN fixture set over a real socket for the conformance suite."""
    fixture_set = _load_fixtures(_FIXTURES_ROOT / "ckan", ["package_search"])
    server, base = start_test_server({"/api/3/action/package_search": _ckan_search_sequence(fixture_set)})
    try:
        yield server, base, fixture_set
    finally:
        server.shutdown()
        server.server_close()
