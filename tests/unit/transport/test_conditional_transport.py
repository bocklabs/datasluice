"""ConditionalTransport + HttpxTransport.conditional_fetch real-socket tests (SYNC-06)."""

from __future__ import annotations

import pytest

pytest.importorskip("httpx")

from datasluice.ports.transport import ConditionalTransport
from datasluice.transport.http_client import HttpClient
from datasluice.transport.httpx_transport import HttpxTransport
from tests.helpers.http_server import MockResponse, start_test_server


@pytest.fixture()
def etag_server():
    server, base_url = start_test_server(
        {
            "/resource.csv": MockResponse(
                headers={"ETag": '"e1"', "Last-Modified": "Wed, 30 Jul 2025 00:00:00 GMT"},
                body=b"id,name\n1,A\n",
            )
        }
    )
    try:
        yield server, base_url
    finally:
        server.shutdown()


def test_304_on_etag_match(etag_server) -> None:
    server, base_url = etag_server
    transport = HttpxTransport()

    result = transport.conditional_fetch(f"{base_url}/resource.csv", if_none_match='"e1"')

    assert result.status_code == 304
    assert result.stream is None
    assert "Content-Length" not in result.headers
    assert server.captured[0]["if-none-match"] == '"e1"'


def test_304_on_last_modified_match(etag_server) -> None:
    _server, base_url = etag_server
    transport = HttpxTransport()

    result = transport.conditional_fetch(
        f"{base_url}/resource.csv",
        if_modified_since="Wed, 30 Jul 2025 00:00:00 GMT",
    )

    assert result.status_code == 304
    assert result.stream is None


def test_200_on_etag_mismatch(etag_server) -> None:
    _server, base_url = etag_server
    transport = HttpxTransport()

    result = transport.conditional_fetch(f"{base_url}/resource.csv", if_none_match='"e2"')

    assert result.status_code == 200
    assert result.stream is not None
    with result.stream as response:
        assert b"".join(response) == b"id,name\n1,A\n"


def test_conditional_headers_sent_not_stripped(etag_server) -> None:
    server, base_url = etag_server
    transport = HttpxTransport()

    result = transport.conditional_fetch(f"{base_url}/resource.csv", if_none_match='W/"e1"')

    assert result.status_code == 200
    assert server.captured[0]["if-none-match"] == 'W/"e1"'
    assert result.stream is not None
    with result.stream:
        pass


def test_isinstance_conditional_transport() -> None:
    assert isinstance(HttpxTransport(), ConditionalTransport)


def test_urllib_not_conditional() -> None:
    assert not isinstance(HttpClient(), ConditionalTransport)
