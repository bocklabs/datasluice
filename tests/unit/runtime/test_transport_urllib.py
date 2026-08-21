"""Stdlib catalog transport safety tests."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast
from urllib.request import Request

import pytest

from datasluice.domain.catalog.observability import TLSPolicy
from datasluice.runtime.transport.base import RuntimeRequest
from datasluice.runtime.transport.urllib_transport import UrllibCatalogTransport
from tests.helpers.catalog_transport import SyncLoopbackTransport


class _FakeResponse:
    def __init__(self, status: int, headers: Mapping[str, str], body: bytes = b"") -> None:
        self.status = status
        self.headers = headers
        self._body = body

    def read(self) -> bytes:
        return self._body


class _RecordingOpener:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self.requests: list[Request] = []
        self._responses = responses

    def open(self, request: Request, *, timeout: float) -> _FakeResponse:
        self.requests.append(request)
        return self._responses[len(self.requests) - 1]


def test_urllib_transport_uses_verified_tls_by_default() -> None:
    assert UrllibCatalogTransport()._tls_policy.verify


def test_tls_policy_rejects_unscoped_disablement() -> None:
    with pytest.raises(ValueError, match="explicit narrow override"):
        TLSPolicy(verify=False)


def test_loopback_sync_transport_refuses_non_loopback_targets() -> None:
    transport = SyncLoopbackTransport()

    with pytest.raises(ValueError):
        transport.get("https://127.0.0.1:443/")
    with pytest.raises(ValueError):
        transport.get("http://example.test:80/")


def test_runtime_request_freezes_header_mapping() -> None:
    request = RuntimeRequest("GET", "http://127.0.0.1:8000/", {"Authorization": "Bearer secret"})

    assert dict(request.headers) == {"Authorization": "Bearer secret"}


def test_urllib_cross_origin_redirect_strips_sensitive_headers_case_insensitively() -> None:
    opener = _RecordingOpener(
        [
            _FakeResponse(
                302,
                {"Location": "https://other.test/next?token=redirect-secret&keep=value"},
            ),
            _FakeResponse(200, {}),
        ]
    )
    transport = UrllibCatalogTransport()
    cast(Any, transport)._opener = opener
    request_headers = {
        "aUtHoRiZaTiOn": "Bearer request-secret",
        "cOoKiE": "session-secret",
        "X-API-KEY": "api-secret",
        "x-AuTh-ToKeN": "token-secret",
        "X-Benign": "preserve-me",
    }
    request = RuntimeRequest("GET", "https://example.test/start", request_headers)

    response = transport.send(request)

    forwarded = {key.lower(): value for key, value in opener.requests[1].header_items()}
    assert response.status_code == 200
    assert len(opener.requests) == 2
    assert all(name not in forwarded for name in {"authorization", "cookie", "x-api-key", "x-auth-token"})
    assert forwarded["x-benign"] == "preserve-me"
    assert dict(request.headers) == request_headers


def test_urllib_same_origin_redirect_preserves_caller_headers() -> None:
    opener = _RecordingOpener(
        [
            _FakeResponse(302, {"Location": "https://example.test/next?keep=value"}),
            _FakeResponse(200, {}),
        ]
    )
    transport = UrllibCatalogTransport()
    cast(Any, transport)._opener = opener
    request_headers = {
        "Authorization": "Bearer request-secret",
        "Cookie": "session-secret",
        "X-API-Key": "api-secret",
        "X-Auth-Token": "token-secret",
        "X-Benign": "preserve-me",
    }

    transport.send(RuntimeRequest("GET", "https://example.test/start", request_headers))

    forwarded = {key.lower(): value for key, value in opener.requests[1].header_items()}
    assert forwarded == {key.lower(): value for key, value in request_headers.items()}
