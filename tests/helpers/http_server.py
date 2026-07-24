"""Reusable local HTTP test server for transport and integration tests.

Spawns a :class:`ThreadingHTTPServer` on an ephemeral port with a scriptable
request handler, so tests can exercise redirect/retry/auth behaviour over real
sockets without hitting the network.
"""

from __future__ import annotations

import threading
import urllib.parse
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast


@dataclass
class MockResponse:
    """Configurable HTTP response.

    Attributes:
        status: HTTP status code.
        headers: Response headers (e.g. ``{"Location": ...}``).
        body: Raw response bytes.
    """

    status: int = 200
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b"OK"


class _CapturingServer(ThreadingHTTPServer):
    """ThreadingHTTPServer that records received requests and exposes the script."""

    captured: list[dict[str, str]]
    responses: dict[str, MockResponse | list[MockResponse]]


class _ScriptableHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        server = cast(_CapturingServer, self.server)
        path = urllib.parse.urlparse(self.path).path
        entry = server.responses.get(path)
        resp: MockResponse
        if isinstance(entry, list):
            resp = cast("MockResponse", entry.pop(0)) if entry else MockResponse(status=404, body=b"exhausted")
        elif entry is None:
            resp = MockResponse(status=404, body=b"not found")
        else:
            resp = entry
        server.captured.append({k.lower(): v for k, v in self.headers.items()})
        self.send_response(resp.status)
        for name, value in resp.headers.items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(resp.body)))
        self.end_headers()
        self.wfile.write(resp.body)

    def log_message(self, format: str, *args: Any) -> None:
        return


def start_test_server(
    responses: dict[str, MockResponse | list[MockResponse]] | None = None,
) -> tuple[_CapturingServer, str]:
    """Start a scriptable test HTTP server on an ephemeral port.

    Args:
        responses: Mapping of request path to a :class:`MockResponse` or a list
            of :class:`MockResponse` (consumed sequentially per request).

    Returns:
        ``(server, base_url)`` where ``server`` exposes ``captured`` (list of
        received-header dicts) and ``responses`` (mutable script) attributes.
    """
    server = _CapturingServer(("127.0.0.1", 0), _ScriptableHandler)
    server.captured = []
    server.responses = dict(responses or {})
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    base_url = f"http://127.0.0.1:{port}"
    return server, base_url
