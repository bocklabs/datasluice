from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, cast

import pytest

from datasluice.domain import CredentialScope
from datasluice.runtime.transport.base import RuntimeRequest, TransportError
from datasluice.runtime.transport.urllib_transport import UrllibCatalogTransport
from tests.helpers.catalog_transport import _FakeResponse, _RecordingOpener
from tests.helpers.httpx_probe import UPLOAD_PARTS, AsyncProbe, SyncProbe

httpx = pytest.importorskip("httpx")

if TYPE_CHECKING:
    from httpx import Request, Response

_ALLOWED_SCOPE = CredentialScope(allowed_hosts=("other.test",), allowed_schemes=("https",), send_on_redirect=True)
_BODY = b'{"token": "redirect-body-secret"}'
_HEADERS = {"Content-Type": "application/json", "Authorization": "Bearer secret"}
_ORIGIN = "example.test"


def _hop(status: int, *, allow_target: bool) -> Any:
    def responder(request: Request) -> Response:
        if request.url.host != _ORIGIN:
            if not allow_target:
                raise AssertionError("The cross-origin follow-up must never be dispatched.")
            return httpx.Response(200, content=b"redirected")
        return httpx.Response(status, headers={"Location": "https://other.test/next"})

    return responder


@pytest.mark.parametrize("status", [307, 308])
def test_httpx_cross_origin_body_redirect_refuses_even_with_scope_opt_in(status: int) -> None:
    request = RuntimeRequest("POST", f"https://{_ORIGIN}/start", dict(_HEADERS), _BODY)
    with SyncProbe(_hop(status, allow_target=False), credential_scope=_ALLOWED_SCOPE) as probe:
        with pytest.raises(TransportError, match="different redirect origin") as excinfo:
            probe.send(request)

        assert len(probe.requests) == 1
        assert "httpx" in str(excinfo.value)

    assert "redirect-body-secret" not in str(excinfo.value)


@pytest.mark.parametrize("status", [307, 308])
def test_httpx_cross_origin_multipart_redirect_refuses_even_with_scope_opt_in(status: int) -> None:
    request = RuntimeRequest("POST", f"https://{_ORIGIN}/start", dict(_HEADERS), files=UPLOAD_PARTS)
    with SyncProbe(_hop(status, allow_target=False), credential_scope=_ALLOWED_SCOPE) as probe:
        with pytest.raises(TransportError, match="different redirect origin"):
            probe.send(request)

        assert len(probe.requests) == 1


@pytest.mark.parametrize("status", [307, 308])
def test_async_httpx_cross_origin_body_redirect_refuses_even_with_scope_opt_in(status: int) -> None:
    probe = AsyncProbe(_hop(status, allow_target=False), credential_scope=_ALLOWED_SCOPE)

    async def send() -> None:
        request = RuntimeRequest("POST", f"https://{_ORIGIN}/start", dict(_HEADERS), _BODY)
        async with probe:
            with pytest.raises(TransportError, match="different redirect origin"):
                await probe.send(request)

    asyncio.run(send())

    assert len(probe.requests) == 1


@pytest.mark.parametrize("status", [307, 308])
def test_urllib_cross_origin_body_redirect_refuses_even_with_scope_opt_in(status: int) -> None:
    opener = _RecordingOpener([_FakeResponse(status, {"Location": "https://other.test/next"})])
    transport = UrllibCatalogTransport(credential_scope=_ALLOWED_SCOPE)
    cast("Any", transport)._opener = opener

    request = RuntimeRequest("POST", f"https://{_ORIGIN}/start", dict(_HEADERS), _BODY)
    with pytest.raises(TransportError, match="different redirect origin") as excinfo:
        transport.send(request)

    assert len(opener.requests) == 1
    assert opener.requests[0].data == _BODY
    assert "urllib" in str(excinfo.value)
    assert "redirect-body-secret" not in str(excinfo.value)


def test_httpx_scope_opt_in_still_relays_authorization_once_the_body_is_dropped() -> None:
    with SyncProbe(_hop(302, allow_target=True), credential_scope=_ALLOWED_SCOPE) as probe:
        response = probe.send(RuntimeRequest("POST", f"https://{_ORIGIN}/start", dict(_HEADERS), _BODY))

    follow_up = probe.requests[1]
    forwarded = {key.lower(): value for key, value in follow_up.headers.items()}
    assert response.status_code == 200
    assert follow_up.method == "GET"
    assert forwarded["authorization"] == "Bearer secret"


def test_urllib_scope_opt_in_still_relays_authorization_once_the_body_is_dropped() -> None:
    opener = _RecordingOpener([_FakeResponse(302, {"Location": "https://other.test/next"}), _FakeResponse(200, {})])
    transport = UrllibCatalogTransport(credential_scope=_ALLOWED_SCOPE)
    cast("Any", transport)._opener = opener

    transport.send(RuntimeRequest("POST", f"https://{_ORIGIN}/start", dict(_HEADERS), _BODY))

    forwarded = {key.lower(): value for key, value in opener.requests[1].header_items()}
    assert forwarded["authorization"] == "Bearer secret"
    assert opener.requests[1].data is None
