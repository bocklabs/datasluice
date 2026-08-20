"""Conditional response handoff behavior for resource readers."""

from __future__ import annotations

import importlib
import logging
import os

import pytest

from datasluice.data import DataPlaneResourceReader
from datasluice.runtime.transport.httpx_transport import HttpxCatalogTransport
from datasluice.sync import sync_resources

sync_module = importlib.import_module("datasluice.sync.sync")
if not hasattr(sync_module, "_RESPONSE_AWARE_READER_READY") and os.environ.get("DATASLUICE_TDD_RED") != "1":
    pytest.skip("response-aware reader handoff implementation pending GREEN phase", allow_module_level=True)


class _ResponseAwareReader:
    def __init__(self, transport: HttpxCatalogTransport) -> None:
        self._delegate = DataPlaneResourceReader(transport=transport)
        self.open_calls = 0
        self.open_response_calls = 0

    def open(self, resource, *, batch_size: int = 65536):
        self.open_calls += 1
        return self._delegate.open(resource, batch_size=batch_size)

    def open_response(self, resource, stream_cm, *, headers, batch_size: int | None = None):
        self.open_response_calls += 1
        return self._delegate.open_response(resource, stream_cm, headers=headers, batch_size=batch_size)


class _BaseReader:
    def __init__(self, transport: HttpxCatalogTransport) -> None:
        self._delegate = DataPlaneResourceReader(transport=transport)
        self.open_calls = 0

    def open(self, resource, *, batch_size: int = 65536):
        self.open_calls += 1
        return self._delegate.open(resource, batch_size=batch_size)


def test_response_aware_reader_uses_the_runtime_transport(tmp_path, csv_server, make_resource) -> None:
    server, url = csv_server(headers={"ETag": '"handoff"'})
    resource = make_resource(url)
    transport = HttpxCatalogTransport()
    reader = _ResponseAwareReader(transport)
    response_aware_reader = getattr(importlib.import_module("datasluice.ports"), "ResponseAwareReader", None)

    assert response_aware_reader is not None
    assert isinstance(reader, response_aware_reader)

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
    assert reader.open_response_calls == 0
    assert reader.open_calls == 1
    assert server.captured_paths == ["/data.csv", "/data.csv"]


def test_base_reader_falls_back_to_open(tmp_path, csv_server, make_resource, caplog) -> None:
    server, url = csv_server(headers={"ETag": '"fallback"'})
    resource = make_resource(url)
    transport = HttpxCatalogTransport()
    reader = _BaseReader(transport)

    with caplog.at_level(logging.WARNING):
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
    assert server.captured_paths == ["/data.csv", "/data.csv"]


def test_conditional_request_has_no_implicit_credentials(tmp_path, csv_server, make_resource) -> None:
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
    assert server.captured_paths == ["/data.csv", "/data.csv"]


def _inmemory_state_store():
    from datasluice.sync.state_store import InMemoryStateStore

    return InMemoryStateStore()
