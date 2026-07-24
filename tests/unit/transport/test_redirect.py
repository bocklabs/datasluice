"""Integration tests for credential-aware redirect handling over real sockets."""

from __future__ import annotations

import urllib.request
from http.client import HTTPMessage
from io import BytesIO

from datasluice.auth import BearerAuth
from datasluice.domain import CredentialScope
from datasluice.exceptions import PortalError
from datasluice.transport import HttpClient
from datasluice.transport.redirect import CredentialAwareRedirectHandler
from tests.helpers.http_server import MockResponse, start_test_server


def test_cross_host_redirect_strips_authorization() -> None:
    server_b, base_b = start_test_server({"/target": MockResponse(body=b"ok")})
    server_a, base_a = start_test_server(
        {"/start": MockResponse(status=302, headers={"Location": f"{base_b}/target"}, body=b"redirect")}
    )
    try:
        client = HttpClient(auth=BearerAuth("test-secret-token"))
        try:
            client.request(f"{base_a}/start")
        except PortalError:
            pass
        captured_b = server_b.captured
        assert captured_b, "server B should have received the redirected request"
        for headers in captured_b:
            assert "authorization" not in headers, "Authorization leaked to a different host on redirect"
    finally:
        server_a.shutdown()
        server_a.server_close()
        server_b.shutdown()
        server_b.server_close()


def test_same_host_redirect_with_allowed_scope_retains_authorization() -> None:
    server, base = start_test_server({})
    server.responses["/old"] = MockResponse(status=302, headers={"Location": f"{base}/new"}, body=b"redirect")
    server.responses["/new"] = MockResponse(body=b"ok")
    scope = CredentialScope(allowed_hosts=("127.0.0.1",), allowed_schemes=("http",), send_on_redirect=True)
    try:
        client = HttpClient(auth=BearerAuth("test-secret-token"), credential_scope=scope)
        client.request(f"{base}/old")
        new_requests = [h for h in server.captured if "authorization" in h]
        assert new_requests, "Authorization should be retained on same-host redirect within allowed scope"
    finally:
        server.shutdown()
        server.server_close()


def test_redirect_handler_strips_on_scheme_downgrade_directly() -> None:
    handler = CredentialAwareRedirectHandler()
    req = urllib.request.Request("https://a.example.com/start", headers={"Authorization": "Bearer secret"})
    new_req = handler.redirect_request(req, BytesIO(b""), 302, "Found", HTTPMessage(), "http://a.example.com/target")
    assert new_req is not None
    assert "Authorization" not in new_req.headers
