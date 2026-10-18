from __future__ import annotations

import socket
from typing import TYPE_CHECKING

import pytest

from datasluice.domain import Resource
from datasluice.exceptions import DownloadError
from datasluice.io.content_cache import ContentCache
from datasluice.io.downloader import Downloader
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse, TransportError
from datasluice.runtime.transport.user_agent import build_user_agent

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(autouse=True)
def _public_dns(monkeypatch: pytest.MonkeyPatch) -> None:
    answers = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0))]
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: answers)


def test_downloader_rejects_dns_answers_with_private_addresses(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    answers = [
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0)),
        (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("::ffff:127.0.0.1", 0, 0, 0)),
    ]
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: answers)
    transport = _Transport(RuntimeResponse(200, {}, b"private"))

    with pytest.raises(DownloadError, match="locality"):
        Downloader(transport).download(_resource(), tmp_path)

    assert transport.requests == []


class _Transport:
    def __init__(self, response: RuntimeResponse | BaseException) -> None:
        self.response = response
        self.requests: list[RuntimeRequest] = []

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        if isinstance(self.response, BaseException):
            raise self.response
        return self.response

    def close(self) -> None:
        return None


def _resource(url: str = "https://catalog.example.test/data.csv") -> Resource:
    return Resource(id="resource-1", name="data.csv", url=url)


def test_downloader_returns_successful_body_and_sends_user_agent(tmp_path: Path) -> None:
    transport = _Transport(RuntimeResponse(200, {}, b"id\n1\n"))

    path = Downloader(transport).download(_resource(), tmp_path)

    assert path.read_bytes() == b"id\n1\n"
    assert transport.requests[0].headers["User-Agent"] == build_user_agent()


def test_downloader_rejects_unsuccessful_status_with_sanitized_url(tmp_path: Path) -> None:
    transport = _Transport(RuntimeResponse(404, {}, b"missing"))
    resource = _resource("https://catalog.example.test/data.csv?token=raw-secret")

    downloader = Downloader(transport)
    with pytest.raises(DownloadError, match="HTTP 404") as raised:
        downloader.download(resource, tmp_path)

    assert "raw-secret" not in str(raised.value)


def test_downloader_wraps_transport_failure_with_preserved_cause(tmp_path: Path) -> None:
    failure = TransportError("connection closed")

    transport = _Transport(failure)
    downloader = Downloader(transport)
    resource = _resource()
    with pytest.raises(DownloadError) as raised:
        downloader.download(resource, tmp_path)

    assert raised.value.__cause__ is failure


def test_downloader_cache_hit_skips_transport(tmp_path: Path) -> None:
    resource = _resource()
    cache = ContentCache(str(tmp_path / "cache"))
    cache.put(resource.url or "", b"cached")
    transport = _Transport(RuntimeResponse(500, {}, b"unused"))

    path = Downloader(transport, cache=cache).download(resource, tmp_path / "downloads")

    assert path.read_bytes() == b"cached"
    assert transport.requests == []


def test_downloader_rejects_private_redirect_before_dispatch(tmp_path: Path) -> None:
    from tests.helpers.httpx_probe import SyncProbe, at_host, redirect_to

    responder = redirect_to("http://169.254.169.254/credentials", 302, at_host("catalog.example.test"))
    with SyncProbe(responder) as transport, pytest.raises(DownloadError, match="locality"):
        Downloader(transport).download(_resource(), tmp_path)
    assert len(transport.requests) == 1


def test_downloader_private_target_requires_explicit_allowance(tmp_path: Path) -> None:
    transport = _Transport(RuntimeResponse(200, {}, b"local"))
    resource = _resource("http://127.0.0.1/data")
    with pytest.raises(DownloadError, match="locality"):
        Downloader(transport).download(resource, tmp_path)
    assert transport.requests == []
    assert Downloader(transport, allow_private_http=True).download(resource, tmp_path).read_bytes() == b"local"


def test_downloader_reports_redirect_limit(tmp_path: Path) -> None:
    from tests.helpers.httpx_probe import SyncProbe, fixed

    responder = fixed(302, headers={"Location": "https://example.test/next"})
    resource = _resource()
    with SyncProbe(responder) as transport:
        downloader = Downloader(transport)
        with pytest.raises(DownloadError, match="redirect limit"):
            downloader.download(resource, tmp_path)
    assert len(transport.requests) == 11


def test_downloader_reports_unusable_redirect_location(tmp_path: Path) -> None:
    from tests.helpers.httpx_probe import SyncProbe, fixed

    responder = fixed(302, headers={"Location": "http://["})
    resource = _resource()
    with SyncProbe(responder) as transport:
        downloader = Downloader(transport)
        with pytest.raises(DownloadError, match="unusable redirect Location"):
            downloader.download(resource, tmp_path)
    assert len(transport.requests) == 1


def test_bad_checksum_does_not_populate_cache(tmp_path: Path) -> None:
    from datasluice.exceptions import ChecksumMismatchError

    cache = ContentCache(str(tmp_path / "cache"))
    transport = _Transport(RuntimeResponse(200, {}, b"corrupt"))
    resource = _resource()
    downloader = Downloader(transport, cache=cache)
    with pytest.raises(ChecksumMismatchError):
        downloader.download(resource, tmp_path, verify_hash="0" * 64)
    assert cache.get(resource.url or "") is None


def test_bad_cached_checksum_evicts_the_poisoned_entry(tmp_path: Path) -> None:
    import hashlib

    from datasluice.exceptions import ChecksumMismatchError

    cache = ContentCache(str(tmp_path / "cache"))
    cache.put("https://catalog.example.test/data.csv", b"corrupt")
    transport = _Transport(RuntimeResponse(200, {}, b"good"))
    resource = _resource()
    digest = hashlib.sha256(b"good").hexdigest()
    with pytest.raises(ChecksumMismatchError):
        Downloader(transport, cache=cache).download(resource, tmp_path, verify_hash=digest)
    assert cache.get(resource.url or "") is None
    assert transport.requests == []
