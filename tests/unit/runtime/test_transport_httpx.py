"""Optional httpx catalog transport tests."""

from __future__ import annotations

import asyncio

import httpx
import pytest

from datasluice.runtime.transport.base import RuntimeRequest, TransportFailure
from datasluice.runtime.transport.httpx_transport import AsyncHttpxCatalogTransport, HttpxCatalogTransport


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


def test_httpx_transport_strips_sensitive_headers_and_redacts_sensitive_redirect_query() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.host == "example.test":
            return httpx.Response(
                302,
                headers={"Location": "https://other.test/next?token=redirect-secret&keep=value"},
            )
        return httpx.Response(200, content=b"redirected")

    transport = HttpxCatalogTransport(transport=httpx.MockTransport(handler))
    request_headers = {
        "aUtHoRiZaTiOn": "Bearer request-secret",
        "cOoKiE": "session-secret",
        "X-API-KEY": "api-secret",
        "x-AuTh-ToKeN": "token-secret",
        "X-Benign": "preserve-me",
    }
    response = transport.send(RuntimeRequest("GET", "https://example.test/start", request_headers))
    transport.close()

    assert response.body == b"redirected"
    assert len(seen) == 2
    first_headers = {key.lower(): value for key, value in seen[0].headers.items()}
    second_headers = {key.lower(): value for key, value in seen[1].headers.items()}
    assert all(name in first_headers for name in {"authorization", "cookie", "x-api-key", "x-auth-token"})
    assert all(name not in second_headers for name in {"authorization", "cookie", "x-api-key", "x-auth-token"})
    assert second_headers["x-benign"] == "preserve-me"
    assert "redirect-secret" not in str(seen[1].url)
    assert "keep=value" in str(seen[1].url)


def test_httpx_transport_same_origin_redirect_preserves_caller_headers() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/start":
            return httpx.Response(302, headers={"Location": "/next?keep=value"})
        return httpx.Response(200, content=b"redirected")

    request_headers = {
        "Authorization": "Bearer request-secret",
        "Cookie": "session-secret",
        "X-API-Key": "api-secret",
        "X-Auth-Token": "token-secret",
        "X-Benign": "preserve-me",
    }
    transport = HttpxCatalogTransport(transport=httpx.MockTransport(handler))
    response = transport.send(RuntimeRequest("GET", "https://example.test/start", request_headers))
    transport.close()

    forwarded = {key.lower(): value for key, value in seen[1].headers.items()}
    assert response.body == b"redirected"
    assert all(forwarded[key.lower()] == value for key, value in request_headers.items())


def test_async_httpx_transport_strips_sensitive_headers_and_redacts_sensitive_redirect_query() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.host == "example.test":
            return httpx.Response(
                302,
                headers={"Location": "https://other.test/next?token=redirect-secret&keep=value"},
            )
        return httpx.Response(200, content=b"redirected")

    request_headers = {
        "aUtHoRiZaTiOn": "Bearer request-secret",
        "cOoKiE": "session-secret",
        "X-API-KEY": "api-secret",
        "x-AuTh-ToKeN": "token-secret",
        "X-Benign": "preserve-me",
    }

    async def send() -> None:
        transport = AsyncHttpxCatalogTransport(transport=httpx.MockTransport(handler))
        response = await transport.send(RuntimeRequest("GET", "https://example.test/start", request_headers))
        await transport.aclose()
        assert response.body == b"redirected"

    asyncio.run(send())

    assert len(seen) == 2
    first_headers = {key.lower(): value for key, value in seen[0].headers.items()}
    second_headers = {key.lower(): value for key, value in seen[1].headers.items()}
    assert all(name in first_headers for name in {"authorization", "cookie", "x-api-key", "x-auth-token"})
    assert all(name not in second_headers for name in {"authorization", "cookie", "x-api-key", "x-auth-token"})
    assert second_headers["x-benign"] == "preserve-me"
    assert "redirect-secret" not in str(seen[1].url)
    assert "keep=value" in str(seen[1].url)


def test_async_httpx_transport_same_origin_redirect_preserves_caller_headers() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/start":
            return httpx.Response(302, headers={"Location": "/next?keep=value"})
        return httpx.Response(200, content=b"redirected")

    request_headers = {
        "Authorization": "Bearer request-secret",
        "Cookie": "session-secret",
        "X-API-Key": "api-secret",
        "X-Auth-Token": "token-secret",
        "X-Benign": "preserve-me",
    }

    async def send() -> None:
        transport = AsyncHttpxCatalogTransport(transport=httpx.MockTransport(handler))
        response = await transport.send(RuntimeRequest("GET", "https://example.test/start", request_headers))
        await transport.aclose()
        assert response.body == b"redirected"

    asyncio.run(send())

    forwarded = {key.lower(): value for key, value in seen[1].headers.items()}
    assert all(forwarded[key.lower()] == value for key, value in request_headers.items())
