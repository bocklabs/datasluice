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
            extra_headers: Additional raw header pairs, including duplicate names.
            body: Raw response bytes.
            chunk_size: When set, the handler emits the body using HTTP/1.1
                ``Transfer-Encoding: chunked`` instead of ``Content-Length`` —
                each ``chunk_size``-byte slice is written as ``<hex-size> CRLF
                <bytes> CRLF`` followed by the terminating ``0 CRLF CRLF`` frame.
                This simulates a real streaming HTTP server for tests
    . Mutually exclusive with a pre-set ``Content-Length``
                header (the spec forbids both).
    """

    status: int = 200
    headers: dict[str, str] = field(default_factory=dict)
    extra_headers: tuple[tuple[str, str], ...] = ()
    body: bytes = b"OK"
    chunk_size: int | None = None


class _CapturingServer(ThreadingHTTPServer):
    """ThreadingHTTPServer that records received requests and exposes the script.

    ``captured`` holds per-request lowercased header dicts; ``captured_paths``
    holds the raw request target (path + query string) so tests can assert the
    serialized wire format (e.g. repeated keys for list-valued params).
    """

    captured: list[dict[str, str]]
    captured_paths: list[str]
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
        server.captured_paths.append(self.path)
        if self._not_modified(resp):
            self.send_response(304)
            self.end_headers()
            return
        self.send_response(resp.status)
        for name, value in resp.headers.items():
            if name.lower() in ("content-length", "transfer-encoding"):
                continue
            self.send_header(name, value)
        for name, value in resp.extra_headers:
            if name.lower() not in ("content-length", "transfer-encoding"):
                self.send_header(name, value)
        if resp.chunk_size is not None:
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            self._write_chunked(resp.body, resp.chunk_size)
        else:
            self.send_header("Content-Length", str(len(resp.body)))
            self.end_headers()
            self.wfile.write(resp.body)

    def _not_modified(self, resp: MockResponse) -> bool:
        """Return whether request validators match *resp*."""
        if_none_match = self.headers.get("if-none-match")
        if_modified_since = self.headers.get("if-modified-since")
        etag = resp.headers.get("ETag")
        last_modified = resp.headers.get("Last-Modified")
        return bool(
            (if_none_match is not None and etag is not None and if_none_match == etag)
            or (if_modified_since is not None and last_modified is not None and if_modified_since >= last_modified)
        )

    def _write_chunked(self, body: bytes, chunk_size: int) -> None:
        """Emit *body* using HTTP/1.1 chunked transfer-encoding."""

        if chunk_size <= 0:
            chunk_size = 1
        offset = 0
        while offset < len(body):
            chunk = body[offset : offset + chunk_size]
            self.wfile.write(f"{len(chunk):X}\r\n".encode("ascii"))
            self.wfile.write(chunk)
            self.wfile.write(b"\r\n")
            self.wfile.flush()
            offset += chunk_size
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()

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
        received-header dicts), ``captured_paths`` (raw request targets), and
        ``responses`` (mutable script) attributes.
    """
    server = _CapturingServer(("127.0.0.1", 0), _ScriptableHandler)
    server.captured = []
    server.captured_paths = []
    server.responses = dict(responses or {})
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    base_url = f"http://127.0.0.1:{port}"
    return server, base_url
