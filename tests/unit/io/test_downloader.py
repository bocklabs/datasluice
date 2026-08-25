"""Downloader transport-boundary tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from datasluice.domain import Resource
from datasluice.exceptions import DownloadError
from datasluice.io.cache import FileCache
from datasluice.io.downloader import Downloader
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse, TransportFailure
from datasluice.runtime.transport.user_agent import build_user_agent


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

    with pytest.raises(DownloadError, match="HTTP 404") as raised:
        Downloader(transport).download(resource, tmp_path)

    assert "raw-secret" not in str(raised.value)


def test_downloader_wraps_transport_failure_with_preserved_cause(tmp_path: Path) -> None:
    failure = TransportFailure("connection closed")

    with pytest.raises(DownloadError) as raised:
        Downloader(_Transport(failure)).download(_resource(), tmp_path)

    assert raised.value.__cause__ is failure


def test_downloader_cache_hit_skips_transport(tmp_path: Path) -> None:
    resource = _resource()
    cache = FileCache(tmp_path / "cache")
    cache.put(resource.url or "", b"cached")
    transport = _Transport(RuntimeResponse(500, {}, b"unused"))

    path = Downloader(transport, cache=cache).download(resource, tmp_path / "downloads")

    assert path.read_bytes() == b"cached"
    assert transport.requests == []
