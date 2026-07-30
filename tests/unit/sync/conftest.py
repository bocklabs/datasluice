"""Shared fixtures for datasluice.sync unit tests."""

from __future__ import annotations

import pytest

from datasluice.domain import HttpDownload, Resource
from datasluice.sync.state_store import FileStateStore, InMemoryStateStore
from tests.helpers.http_server import MockResponse, start_test_server

CSV_BYTES = b"id,name\n1,A\n2,B\n"


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
