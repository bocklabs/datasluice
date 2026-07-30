"""Shared fixtures for datasluice.sync unit tests."""

from __future__ import annotations

from typing import Any

import pytest

from datasluice.domain import HttpDownload, Resource
from datasluice.sync.state_store import FileStateStore, InMemoryStateStore
from tests.helpers.http_server import MockResponse, start_test_server

CSV_BYTES = b"id,name\n1,A\n2,B\n"


class FaultInjectingStateStore:
    """Raise on a selected state checkpoint while preserving earlier writes."""

    def __init__(self, inner: InMemoryStateStore, *, raise_on_put: int) -> None:
        self.inner = inner
        self.raise_on_put = raise_on_put
        self.put_count = 0

    def get(self, key: str):
        return self.inner.get(key)

    def put(self, key: str, state: Any) -> None:
        self.put_count += 1
        if self.put_count == self.raise_on_put:
            raise RuntimeError("injected crash")
        self.inner.put(key, state)

    def delete(self, key: str) -> None:
        self.inner.delete(key)


class WriteCountingFS:
    """Count destination writes while delegating filesystem operations."""

    def __init__(self, fs: Any) -> None:
        self.fs = fs
        self.pipe_file_count = 0

    def pipe_file(self, path: str, data: bytes) -> None:
        self.pipe_file_count += 1
        self.fs.pipe_file(path, data)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.fs, name)


def write_counting_fs(fs: Any) -> WriteCountingFS:
    """Wrap a filesystem with write counting."""
    return WriteCountingFS(fs)


@pytest.fixture()
def memory_store() -> FileStateStore:
    """FileStateStore on the in-process fsspec ``memory://`` backend."""
    return FileStateStore("memory://state")


@pytest.fixture()
def file_store(tmp_path) -> FileStateStore:
    """FileStateStore on a local ``file://{tmp_path}/state`` directory."""
    return FileStateStore(f"file://{tmp_path}/state")


@pytest.fixture()
def csv_server(request):
    """Return a factory for scriptable CSV servers."""
    servers = []

    def factory(
        path: str = "/data.csv",
        *,
        body: bytes = CSV_BYTES,
        headers: dict[str, str] | None = None,
    ):
        server, base_url = start_test_server({path: MockResponse(body=body, headers=headers or {})})
        servers.append(server)
        return server, f"{base_url}{path}"

    def shutdown() -> None:
        for server in servers:
            server.shutdown()

    request.addfinalizer(shutdown)
    return factory


@pytest.fixture()
def csv_server_multi(request):
    """Return a factory for multi-resource CSV servers."""
    servers = []

    def factory(paths: dict[str, bytes | MockResponse]):
        responses: dict[str, MockResponse | list[MockResponse]] = {
            path: value if isinstance(value, MockResponse) else MockResponse(body=value)
            for path, value in paths.items()
        }
        server, base_url = start_test_server(responses)
        servers.append(server)
        return server, base_url

    def shutdown() -> None:
        for server in servers:
            server.shutdown()

    request.addfinalizer(shutdown)
    return factory


@pytest.fixture()
def make_resource():
    """Return a factory for synthetic HTTP resources."""

    def factory(url: str, format: str = "CSV", resource_id: str = "resource-1") -> Resource:
        return Resource(
            id=resource_id,
            name=resource_id,
            url=url,
            format=format,
            access=HttpDownload(url=url),
        )

    return factory


@pytest.fixture()
def inmemory_state() -> InMemoryStateStore:
    """Return a fresh in-memory state store."""
    return InMemoryStateStore()
