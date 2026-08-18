"""Fixture-owned local servers for catalog reference transport tests."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from tests.helpers.http_server import MockResponse, _CapturingServer, start_test_server


@pytest.fixture
def catalog_fixture_server() -> Iterator[tuple[_CapturingServer, str]]:
    """Provide deterministic catalog transport responses over a real loopback socket."""
    server, base_url = start_test_server(
        {
            "/sync": MockResponse(body=b"sync"),
            "/async": MockResponse(body=b"async"),
            "/cancel": MockResponse(body=b"cancel"),
        }
    )
    try:
        yield server, base_url
    finally:
        server.shutdown()
        server.server_close()
