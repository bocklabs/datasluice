"""Optional httpx catalog transport tests."""

from __future__ import annotations

import httpx
import pytest

from datasluice.runtime.transport.base import RuntimeRequest, TransportFailure
from datasluice.runtime.transport.httpx_transport import HttpxCatalogTransport


def test_httpx_transport_maps_injected_response() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, headers={"Retry-After": "2"}, content=b"fixture")

    transport = HttpxCatalogTransport(transport=httpx.MockTransport(handler))
    response = transport.send(RuntimeRequest("GET", "https://example.test/", {"X-Test": "yes"}))

    assert response.body == b"fixture"
    assert response.retry_after == 2
    assert seen[0].headers["X-Test"] == "yes"


def test_httpx_transport_maps_transport_failure() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("timed out", request=request)

    transport = HttpxCatalogTransport(transport=httpx.MockTransport(handler))

    with pytest.raises(TransportFailure):
        transport.send(RuntimeRequest("GET", "https://example.test/"))


def test_httpx_transport_strips_credentials_and_redacts_sensitive_redirect_query() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if len(seen) == 1:
            return httpx.Response(
                302,
                headers={"Location": "https://other.test/next?token=redirect-secret&keep=value"},
            )
        return httpx.Response(200, content=b"redirected")

    transport = HttpxCatalogTransport(transport=httpx.MockTransport(handler))
    response = transport.send(
        RuntimeRequest("GET", "https://example.test/start", {"Authorization": "Bearer request-secret"})
    )

    assert response.body == b"redirected"
    assert len(seen) == 2
    assert "Authorization" not in seen[1].headers
    assert "redirect-secret" not in str(seen[1].url)
    assert "keep=value" in str(seen[1].url)
