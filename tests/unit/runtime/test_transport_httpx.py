"""Optional httpx catalog transport tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from typing import TYPE_CHECKING, cast
from urllib.parse import urlencode

import pytest

httpx = pytest.importorskip("httpx")

if TYPE_CHECKING:
    from httpx import Request, Response

from datasluice.domain import CredentialScope
from datasluice.runtime.transport.base import (
    RedirectPolicy,
    RuntimeRequest,
    TransportFailure,
    UploadPart,
)
from tests.helpers.httpx_probe import (
    AsyncProbe,
    SyncProbe,
    at_host,
    at_path,
    fixed,
    header_view,
    redirect_loop,
    redirect_then_fail,
    redirect_to,
)

SENSITIVE_HEADERS = {"authorization", "cookie", "x-api-key", "x-auth-token", "x-app-token"}
REDIRECT_BODY_HEADERS = {"Content-Type": "application/x-www-form-urlencoded"}
JSON_HEADERS = {"Content-Type": "application/json"}


def test_httpx_transport_maps_injected_response() -> None:
    with SyncProbe(fixed(200, content=b"fixture", headers={"Retry-After": "2"})) as probe:
        response = probe.send(RuntimeRequest("GET", "https://example.test/", {"X-Test": "yes"}))

    assert response.body == b"fixture"
    assert response.retry_after == 2
    assert probe.requests[0].headers["X-Test"] == "yes"


def test_httpx_no_follow_returns_the_original_redirect_without_contacting_its_target() -> None:
    responder = redirect_then_fail(
        "the redirect target must not receive a request", "https://target.test/secret", 302, "origin.test"
    )

    with SyncProbe(responder) as probe:
        response = probe.send(
            RuntimeRequest("GET", "https://origin.test/root", redirect_policy=RedirectPolicy.NO_FOLLOW)
        )

    assert response.status_code == 302
    assert [str(request.url) for request in probe.requests] == ["https://origin.test/root"]


def test_httpx_transport_parses_retry_after_http_date_form() -> None:
    """An RFC 9110 HTTP-date Retry-After maps to non-negative seconds from now."""
    retry_at = format_datetime(datetime.now(UTC) + timedelta(seconds=30), usegmt=True)

    with SyncProbe(fixed(429, headers={"Retry-After": retry_at})) as probe:
        response = probe.send(RuntimeRequest("GET", "https://example.test/"))

    assert response.retry_after is not None
    assert 0 <= response.retry_after <= 120


def test_httpx_transport_absent_retry_after_header_maps_to_none() -> None:
    """A response without Retry-After carries a None delay."""
    with SyncProbe(fixed(200, content=b"no-delay")) as probe:
        response = probe.send(RuntimeRequest("GET", "https://example.test/"))

    assert response.retry_after is None


def test_httpx_transport_maps_transport_failure() -> None:
    def responder(request: Request) -> Response:
        raise httpx.ConnectTimeout("timed out", request=request)

    probe = SyncProbe(responder)
    with probe, pytest.raises(TransportFailure):
        probe.send(RuntimeRequest("GET", "https://example.test/"))


def test_httpx_transport_strips_sensitive_headers_and_forwards_query_verbatim() -> None:
    request_headers = {
        "aUtHoRiZaTiOn": "Bearer request-secret",
        "cOoKiE": "session-secret",
        "X-API-KEY": "api-secret",
        "x-AuTh-ToKeN": "token-secret",
        "X-App-Token": "app-secret",
        "X-Benign": "preserve-me",
    }
    responder = redirect_to("https://other.test/next?token=redirect-secret&keep=value", 302, at_host("example.test"))

    with SyncProbe(responder) as probe:
        response = probe.send(RuntimeRequest("GET", "https://example.test/start", request_headers))

    assert response.body == b"redirected"
    assert len(probe.requests) == 2
    first_headers = header_view(probe.requests[0])
    second_headers = header_view(probe.requests[1])
    assert all(name in first_headers for name in SENSITIVE_HEADERS)
    assert all(name not in second_headers for name in SENSITIVE_HEADERS)
    assert second_headers["x-benign"] == "preserve-me"
    assert "token=redirect-secret" in str(probe.requests[1].url)
    assert "keep=value" in str(probe.requests[1].url)


def test_httpx_transport_cross_origin_redirect_preserves_presigned_signature() -> None:
    location = (
        "https://cdn.test/download?X-Amz-Signature=sig123&X-Amz-Credential=AKIA%2F20260822&X-Amz-Expires=900&keep=value"
    )
    responder = redirect_to(location, 302, at_host("example.test"))

    with SyncProbe(responder) as probe:
        response = probe.send(RuntimeRequest("GET", "https://example.test/file"))

    forwarded_url = str(probe.requests[1].url)
    assert response.body == b"redirected"
    assert "X-Amz-Signature=sig123" in forwarded_url
    assert "X-Amz-Credential=AKIA%2F20260822" in forwarded_url
    assert "X-Amz-Expires=900" in forwarded_url
    assert "keep=value" in forwarded_url


def test_httpx_transport_same_origin_redirect_preserves_caller_headers() -> None:
    request_headers = {
        "Authorization": "Bearer request-secret",
        "Cookie": "session-secret",
        "X-API-Key": "api-secret",
        "X-Auth-Token": "token-secret",
        "X-Benign": "preserve-me",
    }

    with SyncProbe(redirect_to("/next?keep=value", 302, at_path("/start"))) as probe:
        response = probe.send(RuntimeRequest("GET", "https://example.test/start", request_headers))

    forwarded = header_view(probe.requests[1])
    assert response.body == b"redirected"
    assert all(forwarded[key.lower()] == value for key, value in request_headers.items())


@pytest.mark.parametrize("status", [301, 302, 303])
def test_httpx_redirect_rewrites_post_to_bodyless_get(status: int) -> None:
    with SyncProbe(redirect_to("https://example.test/next", status, at_path("/start"))) as probe:
        response = probe.send(RuntimeRequest("POST", "https://example.test/start", JSON_HEADERS, b'{"key": "value"}'))

    follow_up = probe.requests[1]
    assert response.status_code == 200
    assert follow_up.method == "GET"
    assert follow_up.read() == b""
    assert "content-type" not in follow_up.headers


@pytest.mark.parametrize("status", [307, 308])
def test_httpx_redirect_preserves_method_and_body(status: int) -> None:
    with SyncProbe(redirect_to("https://example.test/next", status, at_path("/start"))) as probe:
        response = probe.send(RuntimeRequest("POST", "https://example.test/start", JSON_HEADERS, b'{"key": "value"}'))

    follow_up = probe.requests[1]
    assert response.status_code == 200
    assert follow_up.method == "POST"
    assert follow_up.read() == b'{"key": "value"}'
    assert follow_up.headers["content-type"] == "application/json"


@pytest.mark.parametrize("status", [307, 308])
def test_httpx_cross_origin_body_redirect_fails_closed(status: int) -> None:
    body = urlencode({"token": "redirect-body-secret"}).encode()
    message = "a cross-origin body-bearing redirect target must not receive the body"

    with SyncProbe(redirect_then_fail(message, "https://other.test/next", status, "origin.test")) as probe:
        with pytest.raises(TransportFailure, match="different redirect origin"):
            probe.send(RuntimeRequest("POST", "https://origin.test/oauth/revoke", REDIRECT_BODY_HEADERS, body))

    assert len(probe.requests) == 1
    assert probe.requests[0].content == body


@pytest.mark.parametrize("status", [307, 308])
def test_httpx_cross_origin_multipart_redirect_fails_closed(status: int) -> None:
    """A 307/308 preserves the parts, so a cross-origin relay must be refused before the upload."""
    message = "a cross-origin files-bearing redirect target must not receive the parts"

    with SyncProbe(redirect_then_fail(message, "https://other.test/next", status, "origin.test")) as probe:
        with pytest.raises(TransportFailure, match="different redirect origin"):
            probe.send(
                RuntimeRequest(
                    "POST",
                    "https://origin.test/upload",
                    files=(
                        UploadPart(
                            field_name="upload", file_name="data.csv", content_type="text/csv", data=b"a,b\n1,2"
                        ),
                    ),
                )
            )

    assert len(probe.requests) == 1


@pytest.mark.parametrize("status", [301, 302, 307, 308])
def test_httpx_cross_origin_non_post_body_redirect_fails_closed(status: int) -> None:
    """A 301/302 keeps a non-POST body, so the refusal cannot key off the status alone."""
    body = urlencode({"token": "redirect-body-secret"}).encode()
    message = "a cross-origin body-bearing redirect target must not receive the body"

    with SyncProbe(redirect_then_fail(message, "https://other.test/next", status, "origin.test")) as probe:
        with pytest.raises(TransportFailure, match="different redirect origin"):
            probe.send(RuntimeRequest("PUT", "https://origin.test/resources/1/extras/", REDIRECT_BODY_HEADERS, body))

    assert len(probe.requests) == 1
    assert probe.requests[0].content == body


def test_async_httpx_cross_origin_body_redirect_fails_closed() -> None:
    body = urlencode({"token": "redirect-body-secret"}).encode()
    message = "a cross-origin body-bearing redirect target must not receive the body"
    probe = AsyncProbe(redirect_then_fail(message, "https://other.test/next", 307, "origin.test"))

    async def send() -> None:
        async with probe:
            await probe.send(RuntimeRequest("POST", "https://origin.test/oauth/revoke", REDIRECT_BODY_HEADERS, body))

    with pytest.raises(TransportFailure, match="different redirect origin"):
        asyncio.run(send())

    assert len(probe.requests) == 1
    assert probe.requests[0].content == body


def test_async_httpx_cross_origin_multipart_redirect_fails_closed() -> None:
    message = "a cross-origin files-bearing redirect target must not receive the parts"
    probe = AsyncProbe(redirect_then_fail(message, "https://other.test/next", 307, "origin.test"))

    async def send() -> None:
        async with probe:
            await probe.send(
                RuntimeRequest(
                    "POST",
                    "https://origin.test/upload",
                    files=(
                        UploadPart(
                            field_name="upload", file_name="data.csv", content_type="text/csv", data=b"a,b\n1,2"
                        ),
                    ),
                )
            )

    with pytest.raises(TransportFailure, match="different redirect origin"):
        asyncio.run(send())

    assert len(probe.requests) == 1
    assert b'name="upload"; filename="data.csv"' in probe.requests[0].content
    assert b"a,b\n1,2" in probe.requests[0].content


def test_httpx_exceeding_max_redirects_raises_transport_failure() -> None:
    with SyncProbe(redirect_loop(302, "https://example.test/loop/"), max_redirects=3) as probe:
        with pytest.raises(TransportFailure, match="redirect limit"):
            probe.send(RuntimeRequest("GET", "https://example.test/start"))

    assert len(probe.requests) == 4


def test_httpx_refuses_non_http_redirect_target_and_redacts_failure_surface() -> None:
    location = "file:///etc/passwd?token=topsecret&keep=value"

    with SyncProbe(fixed(302, headers={"Location": location})) as probe:
        with pytest.raises(TransportFailure, match="file:///etc/passwd") as excinfo:
            probe.send(RuntimeRequest("GET", "https://example.test/start", {"Authorization": "Bearer s"}))

    assert "topsecret" not in str(excinfo.value)
    assert "keep=value" in str(excinfo.value)


def test_httpx_malformed_redirect_location_closes_response_before_failing() -> None:
    closed: list[bool] = []

    def responder(request: Request) -> Response:
        del request
        response = httpx.Response(302, headers={"Location": "https://example.test:abc/next"})
        original_close = response.close

        def tracking_close() -> None:
            closed.append(True)
            original_close()

        response.close = tracking_close
        return response

    probe = SyncProbe(responder)
    with probe, pytest.raises(TransportFailure):
        probe.send(RuntimeRequest("GET", "https://example.test/start"))

    assert closed == [True]


def test_httpx_credential_scope_retains_authorization_on_allowed_hop() -> None:
    scope = CredentialScope(allowed_hosts=("other.test",), allowed_schemes=("https",), send_on_redirect=True)

    with SyncProbe(
        redirect_to("https://other.test/next", 302, at_host("example.test")), credential_scope=scope
    ) as probe:
        response = probe.send(RuntimeRequest("GET", "https://example.test/start", {"Authorization": "Bearer s"}))

    assert response.status_code == 200
    assert probe.requests[1].headers["authorization"] == "Bearer s"


def test_httpx_credential_scope_strips_authorization_without_send_on_redirect() -> None:
    scope = CredentialScope(allowed_hosts=("example.test",), allowed_schemes=("https",))

    with SyncProbe(redirect_to("/next", 302, at_path("/start")), credential_scope=scope) as probe:
        response = probe.send(RuntimeRequest("GET", "https://example.test/start", {"Authorization": "Bearer s"}))

    assert response.status_code == 200
    assert "authorization" not in probe.requests[1].headers


def test_httpx_credential_scope_follows_downgraded_redirect_and_strips_authorization() -> None:
    """An http-scheme redirect hop is still followed; the scope strips Authorization for it."""
    scope = CredentialScope(allowed_hosts=("other.test",), allowed_schemes=("https",), send_on_redirect=True)

    with SyncProbe(
        redirect_to("http://other.test/insecure", 302, at_host("example.test")), credential_scope=scope
    ) as probe:
        response = probe.send(RuntimeRequest("GET", "https://example.test/start", {"Authorization": "Bearer s"}))

    assert response.status_code == 200
    assert len(probe.requests) == 2
    assert probe.requests[1].url.scheme == "http"
    assert probe.requests[1].url.host == "other.test"
    assert "authorization" not in probe.requests[1].headers


def test_async_httpx_no_follow_returns_the_original_redirect_without_contacting_its_target() -> None:
    responder = redirect_then_fail(
        "the redirect target must not receive a request", "https://target.test/secret", 302, "origin.test"
    )
    probe = AsyncProbe(responder)

    async def send() -> None:
        async with probe:
            response = await probe.send(
                RuntimeRequest("GET", "https://origin.test/root", redirect_policy=RedirectPolicy.NO_FOLLOW)
            )
        assert response.status_code == 302

    asyncio.run(send())

    assert [str(request.url) for request in probe.requests] == ["https://origin.test/root"]


def test_async_httpx_transport_strips_sensitive_headers_and_forwards_query_verbatim() -> None:
    request_headers = {
        "aUtHoRiZaTiOn": "Bearer request-secret",
        "cOoKiE": "session-secret",
        "X-API-KEY": "api-secret",
        "x-AuTh-ToKeN": "token-secret",
        "X-Benign": "preserve-me",
    }
    responder = redirect_to("https://other.test/next?token=redirect-secret&keep=value", 302, at_host("example.test"))

    probe = AsyncProbe(responder)

    async def send() -> None:
        async with probe:
            response = await probe.send(RuntimeRequest("GET", "https://example.test/start", request_headers))
            assert response.body == b"redirected"

    asyncio.run(send())

    assert len(probe.requests) == 2
    first_headers = header_view(probe.requests[0])
    second_headers = header_view(probe.requests[1])
    assert all(name in first_headers for name in {"authorization", "cookie", "x-api-key", "x-auth-token"})
    assert all(name not in second_headers for name in {"authorization", "cookie", "x-api-key", "x-auth-token"})
    assert second_headers["x-benign"] == "preserve-me"
    assert "token=redirect-secret" in str(probe.requests[1].url)
    assert "keep=value" in str(probe.requests[1].url)


def test_async_httpx_transport_same_origin_redirect_preserves_caller_headers() -> None:
    request_headers = {
        "Authorization": "Bearer request-secret",
        "Cookie": "session-secret",
        "X-API-Key": "api-secret",
        "X-Auth-Token": "token-secret",
        "X-Benign": "preserve-me",
    }

    probe = AsyncProbe(redirect_to("/next?keep=value", 302, at_path("/start")))

    async def send() -> None:
        async with probe:
            response = await probe.send(RuntimeRequest("GET", "https://example.test/start", request_headers))
            assert response.body == b"redirected"

    asyncio.run(send())

    forwarded = header_view(probe.requests[1])
    assert all(forwarded[key.lower()] == value for key, value in request_headers.items())


async def _send_async_redirect(
    status: int,
    body: bytes | None,
    headers: dict[str, str],
) -> tuple[str, bytes]:
    probe = AsyncProbe(redirect_to("https://example.test/next", status, at_path("/start")))
    async with probe:
        await probe.send(RuntimeRequest("POST", "https://example.test/start", headers, body))

    return probe.requests[1].method, probe.requests[1].read()


@pytest.mark.parametrize("status", [301, 302, 303])
def test_async_httpx_redirect_rewrites_post_to_bodyless_get(status: int) -> None:
    method, body = asyncio.run(_send_async_redirect(status, b'{"key": "v"}', JSON_HEADERS))

    assert method == "GET"
    assert body == b""


@pytest.mark.parametrize("status", [307, 308])
def test_async_httpx_redirect_preserves_method_and_body(status: int) -> None:
    method, body = asyncio.run(_send_async_redirect(status, b'{"key": "v"}', JSON_HEADERS))

    assert method == "POST"
    assert body == b'{"key": "v"}'


def test_async_httpx_exceeding_max_redirects_raises_transport_failure() -> None:
    probe = AsyncProbe(redirect_loop(302, "https://example.test/loop/"), max_redirects=3)

    async def send() -> None:
        async with probe:
            with pytest.raises(TransportFailure, match="redirect limit"):
                await probe.send(RuntimeRequest("GET", "https://example.test/start"))

    asyncio.run(send())

    assert len(probe.requests) == 4


def test_async_httpx_malformed_redirect_location_closes_response_before_failing() -> None:
    closed: list[bool] = []

    def responder(request: Request) -> Response:
        del request
        response = httpx.Response(302, headers={"Location": "https://example.test:abc/next"})
        original_aclose = response.aclose

        async def tracking_aclose() -> None:
            closed.append(True)
            await original_aclose()

        response.aclose = tracking_aclose
        return response

    async def send() -> None:
        probe = AsyncProbe(responder)
        async with probe:
            with pytest.raises(TransportFailure):
                await probe.send(RuntimeRequest("GET", "https://example.test/start"))

    asyncio.run(send())

    assert closed == [True]


def test_async_httpx_refuses_non_http_redirect_target_and_redacts_failure_surface() -> None:
    location = "file:///etc/passwd?token=topsecret&keep=value"

    async def send() -> object:
        probe = AsyncProbe(fixed(302, headers={"Location": location}))
        async with probe:
            with pytest.raises(TransportFailure, match="file:///etc/passwd") as excinfo:
                await probe.send(RuntimeRequest("GET", "https://example.test/start", {"Authorization": "Bearer s"}))
            return excinfo.value

    failure = cast("TransportFailure", asyncio.run(send()))

    assert "topsecret" not in str(failure)
    assert "keep=value" in str(failure)


def test_httpx_send_stream_wraps_midstream_httpx_errors() -> None:
    def responder(request: Request) -> Response:
        del request
        response = httpx.Response(200)

        def failing_bytes(*args: object, **kwargs: object):
            yield b"ok"
            raise httpx.ReadError("connection dropped", request=httpx.Request("GET", "https://example.test/"))

        response.iter_bytes = failing_bytes
        return response

    probe = SyncProbe(responder)
    with probe:
        response = probe.stream(
            RuntimeRequest("GET", "https://example.test/", redirect_policy=RedirectPolicy.NO_FOLLOW)
        )
        with pytest.raises(TransportFailure):
            list(response)


def test_async_httpx_send_stream_wraps_midstream_httpx_errors() -> None:
    def responder(request: Request) -> Response:
        del request
        response = httpx.Response(200)

        async def failing_bytes(*args: object, **kwargs: object):
            yield b"ok"
            raise httpx.ReadError("connection dropped", request=httpx.Request("GET", "https://example.test/"))

        response.aiter_bytes = failing_bytes
        return response

    async def send() -> None:
        probe = AsyncProbe(responder)
        async with probe:
            response = await probe.stream(
                RuntimeRequest("GET", "https://example.test/", redirect_policy=RedirectPolicy.NO_FOLLOW)
            )
            with pytest.raises(TransportFailure):
                async for _ in response:
                    pass

    asyncio.run(send())


def test_httpx_send_enforces_max_response_bytes() -> None:
    with SyncProbe(fixed(200, content=b"abcdef")) as probe:
        with pytest.raises(TransportFailure, match="byte limit"):
            probe.send(RuntimeRequest("GET", "https://example.test/", max_response_bytes=2))


def test_async_httpx_send_enforces_max_response_bytes() -> None:
    async def send() -> None:
        probe = AsyncProbe(fixed(200, content=b"abcdef"))
        async with probe:
            with pytest.raises(TransportFailure, match="byte limit"):
                await probe.send(RuntimeRequest("GET", "https://example.test/", max_response_bytes=2))

    asyncio.run(send())
