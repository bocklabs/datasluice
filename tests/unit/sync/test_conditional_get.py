"""Conditional validation and headerless fallback behavior."""

from __future__ import annotations

import importlib
import os
from unittest.mock import patch

import pytest

from datasluice.data import DataPlaneResourceReader
from datasluice.sync import sync_resources
from datasluice.transport.httpx_transport import HttpxTransport

sync_module = importlib.import_module("datasluice.sync.sync")
if not hasattr(sync_module, "_CONDITIONAL_SYNC_READY") and os.environ.get("DATASLUICE_TDD_RED") != "1":
    pytest.skip("conditional sync implementation pending GREEN phase", allow_module_level=True)


def _sync(tmp_path, resource, state_store, transport, *, cache=None):
    return list(
        sync_resources(
            [resource],
            state_store=state_store,
            reader=DataPlaneResourceReader(transport=transport),
            destination_uri=f"file://{tmp_path}/dest",
            transport=transport,
            cache=cache,
        )
    )


def test_304_skips_materialize(tmp_path, csv_server, make_resource, inmemory_state) -> None:
    server, url = csv_server(headers={"ETag": '"e1"'})
    resource = make_resource(url)
    transport = HttpxTransport()

    first = _sync(tmp_path, resource, inmemory_state, transport)

    assert first[0].action == "materialized"
    state = inmemory_state.get(resource.id)
    assert state is not None
    assert state.cursor[resource.id] == '"e1"'
    assert server.captured_paths == ["/data.csv"]
    first_synced_at = state.last_synced_at
    server.captured.clear()
    server.captured_paths.clear()

    with patch("datasluice.sync.materialize.materialize") as materialize_spy:
        second = _sync(tmp_path, resource, inmemory_state, transport)

    assert second[0].action == "skipped-unchanged"
    materialize_spy.assert_not_called()
    assert server.captured_paths == ["/data.csv"]
    assert server.captured[0]["if-none-match"] == '"e1"'
    current = inmemory_state.get(resource.id)
    assert current is not None
    assert current.last_synced_at == first_synced_at


def test_304_survives_no_cache(tmp_path, csv_server, make_resource, inmemory_state) -> None:
    _server, url = csv_server(headers={"ETag": '"e1"'})
    resource = make_resource(url)
    transport = HttpxTransport()

    _sync(tmp_path, resource, inmemory_state, transport, cache=None)
    second = _sync(tmp_path, resource, inmemory_state, transport, cache=None)

    assert second[0].action == "skipped-unchanged"


def test_headerless_sha_path(tmp_path, csv_server, make_resource, inmemory_state) -> None:
    server, url = csv_server()
    resource = make_resource(url)
    transport = HttpxTransport()

    first = _sync(tmp_path, resource, inmemory_state, transport)
    first_state = inmemory_state.get(resource.id)
    assert first_state is not None
    assert len(first_state.cursor[resource.id]) == 64
    server.captured_paths.clear()

    second = _sync(tmp_path, resource, inmemory_state, transport)

    assert first[0].action == "materialized"
    assert second[0].action == "skipped-unchanged"
    assert server.captured_paths == ["/data.csv"]


def test_conditional_headers_not_stripped(tmp_path, csv_server, make_resource, inmemory_state) -> None:
    server, url = csv_server(headers={"ETag": '"e1"'})
    resource = make_resource(url)
    transport = HttpxTransport()

    _sync(tmp_path, resource, inmemory_state, transport)
    server.captured.clear()
    _sync(tmp_path, resource, inmemory_state, transport)

    assert server.captured[0]["if-none-match"] == '"e1"'


def test_last_modified_roundtrip(tmp_path, csv_server, make_resource, inmemory_state) -> None:
    last_modified = "Wed, 30 Jul 2025 00:00:00 GMT"
    server, url = csv_server(headers={"Last-Modified": last_modified})
    resource = make_resource(url)
    transport = HttpxTransport()

    _sync(tmp_path, resource, inmemory_state, transport)
    state = inmemory_state.get(resource.id)
    assert state is not None
    assert state.cursor[resource.id] == last_modified
    server.captured.clear()
    second = _sync(tmp_path, resource, inmemory_state, transport)

    assert second[0].action == "skipped-unchanged"
    assert server.captured[0]["if-modified-since"] == last_modified
