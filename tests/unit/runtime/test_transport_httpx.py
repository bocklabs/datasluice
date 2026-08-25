"""Optional httpx catalog transport tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from typing import cast

import pytest

httpx = pytest.importorskip("httpx")

from datasluice.domain import CredentialScope
from datasluice.runtime.transport.base import (
    RuntimeRequest,
    TransportFailure,
)
from datasluice.runtime.transport.httpx_transport import (
    AsyncHttpxCatalogTransport,
    HttpxCatalogTransport,
)


def test_httpx_transport_maps_injected_response() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, headers={"Retry-After": "2"}, content=b"fixture")

    transport = HttpxCatalogTransport(transport=httpx.MockTransport(handler))
    try:
        response = transport.send(RuntimeRequest("GET", "https://example.test/", {"X-Test": "yes"}))
    finally:
        transport.close()

    assert response.body == b"fixture"
    assert response.retry_after == 2
    assert seen[0].headers["X-Test"] == "yes"


def test_httpx_transport_parses_retry_after_http_date_form() -> None:
    """An RFC 9110 HTTP-date Retry-After maps to non-negative seconds from now."""
    retry_at = format_datetime(datetime.now(UTC) + timedelta(seconds=30), usegmt=True)

    def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(429, headers={"Retry-After": retry_at}, content=b"")

    transport = HttpxCatalogTransport(transport=httpx.MockTransport(handler))
    try:
        response = transport.send(RuntimeRequest("GET", "https://example.test/"))
    finally:
        transport.close()

    assert response.retry_after is not None
    assert 0 <= response.retry_after <= 120


def test_httpx_transport_absent_retry_after_header_maps_to_none() -> None:
    """A response without Retry-After carries a None delay."""

    def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(200, content=b"no-delay")

    transport = HttpxCatalogTransport(transport=httpx.MockTransport(handler))
    try:
        response = transport.send(RuntimeRequest("GET", "https://example.test/"))
    finally:
        transport.close()

    assert response.retry_after is None


def test_httpx_transport_maps_transport_failure() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("timed out", request=request)

    transport = HttpxCatalogTransport(transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(TransportFailure):
            transport.send(RuntimeRequest("GET", "https://example.test/"))
    finally:
        transport.close()


def test_httpx_transport_strips_sensitive_headers_and_forwards_query_verbatim() -> None:
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
        "X-App-Token": "app-secret",
        "X-Benign": "preserve-me",
    }
    try:
        response = transport.send(RuntimeRequest("GET", "https://example.test/start", request_headers))
    finally:
        transport.close()

    assert response.body == b"redirected"
    assert len(seen) == 2
    first_headers = {key.lower(): value for key, value in seen[0].headers.items()}
    second_headers = {key.lower(): value for key, value in seen[1].headers.items()}
    sensitive = {"authorization", "cookie", "x-api-key", "x-auth-token", "x-app-token"}
    assert all(name in first_headers for name in sensitive)
    assert all(name not in second_headers for name in sensitive)
    assert second_headers["x-benign"] == "preserve-me"
    assert "token=redirect-secret" in str(seen[1].url)
    assert "keep=value" in str(seen[1].url)


def test_httpx_transport_cross_origin_redirect_preserves_presigned_signature() -> None:
    seen: list[httpx.Request] = []
    location = (
        "https://cdn.test/download?X-Amz-Signature=sig123&X-Amz-Credential=AKIA%2F20260822&X-Amz-Expires=900&keep=value"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.host == "example.test":
            return httpx.Response(302, headers={"Location": location})
        return httpx.Response(200, content=b"presigned")

    transport = HttpxCatalogTransport(transport=httpx.MockTransport(handler))
    try:
        response = transport.send(RuntimeRequest("GET", "https://example.test/file"))
    finally:
        transport.close()

    forwarded_url = str(seen[1].url)
    assert response.body == b"presigned"
    assert "X-Amz-Signature=sig123" in forwarded_url
    assert "X-Amz-Credential=AKIA%2F20260822" in forwarded_url
    assert "X-Amz-Expires=900" in forwarded_url
    assert "keep=value" in forwarded_url


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
    try:
        response = transport.send(RuntimeRequest("GET", "https://example.test/start", request_headers))
    finally:
        transport.close()

    forwarded = {key.lower(): value for key, value in seen[1].headers.items()}
    assert response.body == b"redirected"
    assert all(forwarded[key.lower()] == value for key, value in request_headers.items())


@pytest.mark.parametrize("status", [301, 302, 303])
def test_httpx_redirect_rewrites_post_to_bodyless_get(status: int) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/start":
            return httpx.Response(status, headers={"Location": "https://example.test/next"})
        return httpx.Response(200)

    transport = HttpxCatalogTransport(transport=httpx.MockTransport(handler))
    try:
        response = transport.send(
            RuntimeRequest(
                "POST",
                "https://example.test/start",
                {"Content-Type": "application/json"},
                b'{"key": "value"}',
            )
        )
    finally:
        transport.close()

    follow_up = seen[1]
    assert response.status_code == 200
    assert follow_up.method == "GET"
    assert follow_up.read() == b""
    assert "content-type" not in follow_up.headers


@pytest.mark.parametrize("status", [307, 308])
def test_httpx_redirect_preserves_method_and_body(status: int) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/start":
            return httpx.Response(status, headers={"Location": "https://example.test/next"})
        return httpx.Response(200)

    transport = HttpxCatalogTransport(transport=httpx.MockTransport(handler))
    try:
        response = transport.send(
            RuntimeRequest(
                "POST",
                "https://example.test/start",
                {"Content-Type": "application/json"},
                b'{"key": "value"}',
            )
        )
    finally:
        transport.close()

    follow_up = seen[1]
    assert response.status_code == 200
    assert follow_up.method == "POST"
    assert follow_up.read() == b'{"key": "value"}'
    assert follow_up.headers["content-type"] == "application/json"


def test_httpx_exceeding_max_redirects_raises_transport_failure() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(302, headers={"Location": f"https://example.test/loop/{len(seen)}"})

    transport = HttpxCatalogTransport(transport=httpx.MockTransport(handler), max_redirects=3)
    try:
        with pytest.raises(TransportFailure, match="redirect limit"):
            transport.send(RuntimeRequest("GET", "https://example.test/start"))
    finally:
        transport.close()

    assert len(seen) == 4


def test_httpx_refuses_non_http_redirect_target_and_redacts_failure_surface() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(302, headers={"Location": "file:///etc/passwd?token=topsecret&keep=value"})

    transport = HttpxCatalogTransport(transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(TransportFailure, match="file:///etc/passwd") as excinfo:
            transport.send(RuntimeRequest("GET", "https://example.test/start", {"Authorization": "Bearer s"}))
    finally:
        transport.close()

    assert "topsecret" not in str(excinfo.value)
    assert "keep=value" in str(excinfo.value)


def test_httpx_malformed_redirect_location_closes_response_before_failing() -> None:
    closed: list[bool] = []

    def handler(request: httpx.Request) -> httpx.Response:
        del request
        response = httpx.Response(302, headers={"Location": "https://example.test:abc/next"})
        original_close = response.close

        def tracking_close() -> None:
            closed.append(True)
            original_close()

        response.close = tracking_close
        return response

    transport = HttpxCatalogTransport(transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(TransportFailure):
            transport.send(RuntimeRequest("GET", "https://example.test/start"))
    finally:
        transport.close()

    assert closed == [True]


def test_httpx_credential_scope_retains_authorization_on_allowed_hop() -> None:
    scope = CredentialScope(allowed_hosts=("other.test",), allowed_schemes=("https",), send_on_redirect=True)
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.host == "example.test":
            return httpx.Response(302, headers={"Location": "https://other.test/next"})
        return httpx.Response(200)

    transport = HttpxCatalogTransport(transport=httpx.MockTransport(handler), credential_scope=scope)
    try:
        response = transport.send(RuntimeRequest("GET", "https://example.test/start", {"Authorization": "Bearer s"}))
    finally:
        transport.close()

    assert response.status_code == 200
    assert seen[1].headers["authorization"] == "Bearer s"


def test_httpx_credential_scope_strips_authorization_without_send_on_redirect() -> None:
    scope = CredentialScope(allowed_hosts=("example.test",), allowed_schemes=("https",))
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/start":
            return httpx.Response(302, headers={"Location": "/next"})
        return httpx.Response(200)

    transport = HttpxCatalogTransport(transport=httpx.MockTransport(handler), credential_scope=scope)
    try:
        response = transport.send(RuntimeRequest("GET", "https://example.test/start", {"Authorization": "Bearer s"}))
    finally:
        transport.close()

    assert response.status_code == 200
    assert "authorization" not in seen[1].headers


def test_httpx_credential_scope_follows_downgraded_redirect_and_strips_authorization() -> None:
    """An http-scheme redirect hop is still followed; the scope strips Authorization for it."""
    scope = CredentialScope(allowed_hosts=("other.test",), allowed_schemes=("https",), send_on_redirect=True)
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.host == "example.test":
            return httpx.Response(302, headers={"Location": "http://other.test/insecure"})
        return httpx.Response(200)

    transport = HttpxCatalogTransport(transport=httpx.MockTransport(handler), credential_scope=scope)
    try:
        response = transport.send(RuntimeRequest("GET", "https://example.test/start", {"Authorization": "Bearer s"}))
    finally:
        transport.close()

    assert response.status_code == 200
    assert len(seen) == 2
    assert seen[1].url.scheme == "http"
    assert seen[1].url.host == "other.test"
    assert "authorization" not in seen[1].headers


def test_async_httpx_transport_strips_sensitive_headers_and_forwards_query_verbatim() -> None:
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
        try:
            response = await transport.send(RuntimeRequest("GET", "https://example.test/start", request_headers))
            assert response.body == b"redirected"
        finally:
            await transport.aclose()

    asyncio.run(send())

    assert len(seen) == 2
    first_headers = {key.lower(): value for key, value in seen[0].headers.items()}
    second_headers = {key.lower(): value for key, value in seen[1].headers.items()}
    assert all(name in first_headers for name in {"authorization", "cookie", "x-api-key", "x-auth-token"})
    assert all(name not in second_headers for name in {"authorization", "cookie", "x-api-key", "x-auth-token"})
    assert second_headers["x-benign"] == "preserve-me"
    assert "token=redirect-secret" in str(seen[1].url)
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
        try:
            response = await transport.send(RuntimeRequest("GET", "https://example.test/start", request_headers))
            assert response.body == b"redirected"
        finally:
            await transport.aclose()

    asyncio.run(send())

    forwarded = {key.lower(): value for key, value in seen[1].headers.items()}
    assert all(forwarded[key.lower()] == value for key, value in request_headers.items())


async def _send_async_redirect(
    status: int,
    body: bytes | None,
    headers: dict[str, str],
) -> tuple[str, bytes]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/start":
            return httpx.Response(status, headers={"Location": "https://example.test/next"})
        return httpx.Response(200)

    transport = AsyncHttpxCatalogTransport(transport=httpx.MockTransport(handler))
    try:
        await transport.send(RuntimeRequest("POST", "https://example.test/start", headers, body))
    finally:
        await transport.aclose()

    return seen[1].method, seen[1].read()


@pytest.mark.parametrize("status", [301, 302, 303])
def test_async_httpx_redirect_rewrites_post_to_bodyless_get(status: int) -> None:
    method, body = asyncio.run(_send_async_redirect(status, b'{"key": "v"}', {"Content-Type": "application/json"}))

    assert method == "GET"
    assert body == b""


@pytest.mark.parametrize("status", [307, 308])
def test_async_httpx_redirect_preserves_method_and_body(status: int) -> None:
    method, body = asyncio.run(_send_async_redirect(status, b'{"key": "v"}', {"Content-Type": "application/json"}))

    assert method == "POST"
    assert body == b'{"key": "v"}'


def test_async_httpx_exceeding_max_redirects_raises_transport_failure() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(302, headers={"Location": f"https://example.test/loop/{len(seen)}"})

    async def send() -> None:
        transport = AsyncHttpxCatalogTransport(transport=httpx.MockTransport(handler), max_redirects=3)
        try:
            with pytest.raises(TransportFailure, match="redirect limit"):
                await transport.send(RuntimeRequest("GET", "https://example.test/start"))
        finally:
            await transport.aclose()

    asyncio.run(send())

    assert len(seen) == 4


def test_async_httpx_malformed_redirect_location_closes_response_before_failing() -> None:
    closed: list[bool] = []

    def handler(request: httpx.Request) -> httpx.Response:
        del request
        response = httpx.Response(302, headers={"Location": "https://example.test:abc/next"})
        original_aclose = response.aclose

        async def tracking_aclose() -> None:
            closed.append(True)
            await original_aclose()

        response.aclose = tracking_aclose
        return response

    async def send() -> None:
        transport = AsyncHttpxCatalogTransport(transport=httpx.MockTransport(handler))
        try:
            with pytest.raises(TransportFailure):
                await transport.send(RuntimeRequest("GET", "https://example.test/start"))
        finally:
            await transport.aclose()

    asyncio.run(send())

    assert closed == [True]


def test_async_httpx_refuses_non_http_redirect_target_and_redacts_failure_surface() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(302, headers={"Location": "file:///etc/passwd?token=topsecret&keep=value"})

    async def send() -> object:
        transport = AsyncHttpxCatalogTransport(transport=httpx.MockTransport(handler))
        try:
            with pytest.raises(TransportFailure, match="file:///etc/passwd") as excinfo:
                await transport.send(RuntimeRequest("GET", "https://example.test/start", {"Authorization": "Bearer s"}))
            return excinfo.value
        finally:
            await transport.aclose()

    failure = cast("TransportFailure", asyncio.run(send()))

    assert "topsecret" not in str(failure)
    assert "keep=value" in str(failure)
