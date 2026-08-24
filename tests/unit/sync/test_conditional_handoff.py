"""Conditional response handoff behavior for resource readers."""

from __future__ import annotations

import importlib
import os
from typing import Any

import pytest

from datasluice.data import DataPlaneResourceReader
from datasluice.runtime.transport.base import RuntimeRequest
from datasluice.runtime.transport.httpx_transport import HttpxCatalogTransport
from datasluice.sync import sync_resources
from tests.helpers.http_server import MockResponse

sync_module = importlib.import_module("datasluice.sync.sync")
if not hasattr(sync_module, "_RESPONSE_AWARE_READER_READY") and os.environ.get("DATASLUICE_TDD_RED") != "1":
    pytest.skip("response-aware reader handoff implementation pending GREEN phase", allow_module_level=True)


class _ResponseAwareReader:
    def __init__(self, transport: HttpxCatalogTransport) -> None:
        self._delegate = DataPlaneResourceReader(transport=transport)
        self.open_calls = 0
        self.open_response_calls = 0
        self.response_streams = []

    def open(self, resource, *, batch_size: int = 65536):
        self.open_calls += 1
        return self._delegate.open(resource, batch_size=batch_size)

    def open_response(self, resource, stream_cm, *, headers, batch_size: int | None = None):
        self.open_response_calls += 1
        self.response_streams.append(stream_cm)
        return self._delegate.open_response(resource, stream_cm, headers=headers, batch_size=batch_size)


class _BaseReader:
    def __init__(self, transport: HttpxCatalogTransport) -> None:
        self._delegate = DataPlaneResourceReader(transport=transport)
        self.open_calls = 0

    def open(self, resource, *, batch_size: int = 65536):
        self.open_calls += 1
        return self._delegate.open(resource, batch_size=batch_size)


def test_response_aware_reader_uses_one_runtime_response(tmp_path, csv_server, make_resource) -> None:
    body_a = b"id,name\n1,A\n"
    server, url = csv_server(body=body_a, headers={"ETag": '"handoff-a"'})
    server.responses["/data.csv"] = [MockResponse(body=body_a, headers={"ETag": '"handoff-a"'})]
    resource = make_resource(url)
    transport = HttpxCatalogTransport()
    reader = _ResponseAwareReader(transport)
    response_aware_reader = getattr(importlib.import_module("datasluice.ports"), "ResponseAwareReader", None)
    state_store = _inmemory_state_store()

    assert response_aware_reader is not None
    assert isinstance(reader, response_aware_reader)

    outcomes = list(
        sync_resources(
            [resource],
            state_store=state_store,
            reader=reader,
            destination_uri=f"file://{tmp_path}/dest",
            transport=transport,
        )
    )

    assert outcomes[0].action == "materialized"
    assert reader.open_response_calls == 1
    assert reader.open_calls == 0
    assert server.captured_paths == ["/data.csv"]
    record = outcomes[0].record
    assert record is not None
    import pyarrow.parquet as pq

    with open(record.uri.removeprefix("file://"), "rb") as published:
        assert pq.read_table(published).to_pylist() == [{"id": 1, "name": "A"}]
    state = state_store.get(sync_module.canonical_identity(resource))
    assert state is not None
    assert state.cursor[sync_module.canonical_identity(resource)] == '"handoff-a"'
    stream_cm = reader.response_streams[0]
    assert stream_cm.enter_count == 1
    assert stream_cm.exit_count == 1


class _FailingResponseAwareReader(_ResponseAwareReader):
    def open_response(self, resource, stream_cm, *, headers, batch_size: int | None = None):
        self.open_response_calls += 1
        self.response_streams.append(stream_cm)
        raise RuntimeError("materialization handoff failed")


def test_response_aware_reader_closes_untransferred_response_on_failure(tmp_path, csv_server, make_resource) -> None:
    server, url = csv_server(headers={"ETag": '"handoff-failure"'})
    resource = make_resource(url)
    transport = HttpxCatalogTransport()
    reader = _FailingResponseAwareReader(transport)

    with pytest.raises(RuntimeError, match="materialization handoff failed"):
        list(
            sync_resources(
                [resource],
                state_store=_inmemory_state_store(),
                reader=reader,
                destination_uri=f"file://{tmp_path}/dest",
                transport=transport,
            )
        )

    assert server.captured_paths == ["/data.csv"]
    stream_cm = reader.response_streams[0]
    assert stream_cm.enter_count == 0
    assert stream_cm.exit_count == 1


def test_base_reader_falls_back_to_open(tmp_path, csv_server, make_resource) -> None:
    server, url = csv_server(headers={"ETag": '"fallback"'})
    resource = make_resource(url)
    transport = HttpxCatalogTransport()
    reader = _BaseReader(transport)

    outcomes = list(
        sync_resources(
            [resource],
            state_store=_inmemory_state_store(),
            reader=reader,
            destination_uri=f"file://{tmp_path}/dest",
            transport=transport,
        )
    )

    assert outcomes[0].action == "materialized"
    assert reader.open_calls == 1
    assert server.captured_paths == ["/data.csv"]


def test_base_reader_checkpoints_materialized_representation(tmp_path, csv_server, make_resource) -> None:
    body_a = b"id,name\n1,A\n"
    body_b = b"id,name\n1,B\n"
    server, url = csv_server(body=body_a, headers={"ETag": '"fallback-a"'})
    server.responses["/data.csv"] = [
        MockResponse(body=body_a, headers={"ETag": '"fallback-a"'}),
        MockResponse(body=body_b, headers={"ETag": '"fallback-b"'}),
    ]
    resource = make_resource(url)
    transport = HttpxCatalogTransport()
    reader = _BaseReader(transport)
    state_store = _inmemory_state_store()
    destination_uri = f"file://{tmp_path}/dest"

    first = list(
        sync_resources(
            [resource],
            state_store=state_store,
            reader=reader,
            destination_uri=destination_uri,
            transport=transport,
        )
    )

    server.responses["/data.csv"] = MockResponse(body=body_b, headers={"ETag": '"fallback-b"'})
    second = list(
        sync_resources(
            [resource],
            state_store=state_store,
            reader=reader,
            destination_uri=destination_uri,
            transport=transport,
        )
    )

    first_record = first[0].record
    second_record = second[0].record
    assert first_record is not None
    assert second_record is not None
    import pyarrow.parquet as pq

    with open(second_record.uri.removeprefix("file://"), "rb") as published:
        assert pq.read_table(published).to_pylist() == [{"id": 1, "name": "B"}]
    state = state_store.get(sync_module.canonical_identity(resource))
    assert state is not None
    assert state.cursor[sync_module.canonical_identity(resource)] == second_record.content_digest.value
    assert state.cursor[sync_module.canonical_identity(resource)] != '"fallback-a"'
    assert len(state.cursor[sync_module.canonical_identity(resource)]) == 64
    assert second[0].action == "materialized"
    assert reader.open_calls == 2
    assert server.captured_paths == ["/data.csv", "/data.csv"]
    assert "if-none-match" not in server.captured[1]
    assert "if-modified-since" not in server.captured[1]


def test_conditional_request_has_no_implicit_credentials(tmp_path, csv_server, make_resource, monkeypatch) -> None:
    for name in ("DATASLUICE_CKAN_API_TOKEN", "DATASLUICE_UDATA_API_KEY", "DATASLUICE_SOCRATA_APP_TOKEN"):
        monkeypatch.setenv(name, "env-secret")
    server, url = csv_server(headers={"ETag": '"query-auth"'})
    resource = make_resource(url)
    transport = HttpxCatalogTransport()

    outcomes = list(
        sync_resources(
            [resource],
            state_store=_inmemory_state_store(),
            reader=DataPlaneResourceReader(transport=transport),
            destination_uri=f"file://{tmp_path}/dest",
            transport=transport,
        )
    )

    assert outcomes[0].action == "materialized"
    assert len(server.captured) >= 1
    for request_headers in server.captured:
        assert "authorization" not in request_headers
        assert "x-api-key" not in request_headers
        assert "x-app-token" not in request_headers


class _AuthInjectingTransport:
    def __init__(self, inner: HttpxCatalogTransport) -> None:
        self._inner = inner

    def send(self, request: Any) -> Any:
        headers = {**dict(request.headers), "Authorization": "Bearer injected-secret"}
        return self._inner.send(RuntimeRequest(request.method, request.url, headers, request.body))

    def close(self) -> None:
        self._inner.close()


def test_sync_preserves_query_auth_and_transport_injected_headers(tmp_path, csv_server, make_resource) -> None:
    server, url = csv_server(headers={"ETag": '"query-auth-preserved"'})
    resource = make_resource(f"{url}?api_key=query-secret&next_token=abc")
    transport = HttpxCatalogTransport()
    injecting = _AuthInjectingTransport(transport)

    outcomes = list(
        sync_resources(
            [resource],
            state_store=_inmemory_state_store(),
            reader=DataPlaneResourceReader(transport=injecting),
            destination_uri=f"file://{tmp_path}/dest",
            transport=injecting,
        )
    )

    assert outcomes[0].action == "materialized"
    assert len(server.captured) >= 1, "the sync must have issued at least one request"
    assert len(server.captured_paths) >= 1, "the sync must have hit at least one request target"
    assert all("api_key=query-secret" in path for path in server.captured_paths)
    assert all("next_token=abc" in path for path in server.captured_paths)
    assert all(request["authorization"] == "Bearer injected-secret" for request in server.captured)


def _inmemory_state_store():
    from datasluice.sync.state_store import InMemoryStateStore

    return InMemoryStateStore()
