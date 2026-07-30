"""End-to-end tracer coverage for the checkpointed sync loop."""

from __future__ import annotations

from datasluice.data import DataPlaneResourceReader
from datasluice.domain import QueryAccess, Resource
from datasluice.sync import sync_resources
from datasluice.transport.httpx_transport import HttpxTransport


def test_sync_one_resource(tmp_path, csv_server, make_resource, inmemory_state) -> None:
    _server, url = csv_server()
    resource = make_resource(url)
    outcomes = list(
        sync_resources(
            [resource],
            state_store=inmemory_state,
            reader=DataPlaneResourceReader(transport=HttpxTransport()),
            destination_uri=f"file://{tmp_path}/dest",
        )
    )

    assert len(outcomes) == 1
    assert outcomes[0].action == "materialized"
    record = outcomes[0].record
    assert record is not None
    uri, media_type, size, checksum = record
    assert uri.startswith("file://")
    assert media_type == "application/x-parquet"
    assert size > 0
    assert checksum
    state = inmemory_state.get(resource.id)
    assert state is not None
    assert state.cursor == {resource.id: checksum}


def test_skip_unsupported(tmp_path, inmemory_state) -> None:
    resource = Resource(
        id="query-resource",
        format="CSV",
        access=QueryAccess(endpoint="https://example.com/query"),
    )

    outcomes = list(
        sync_resources(
            [resource],
            state_store=inmemory_state,
            reader=DataPlaneResourceReader(),
            destination_uri=f"file://{tmp_path}/dest",
        )
    )

    assert outcomes[0].action == "skipped-unsupported"
    assert outcomes[0].state_key is None
    assert inmemory_state.get(resource.id) is None


def test_outcome_stream_is_generator(tmp_path, csv_server, make_resource, inmemory_state) -> None:
    server, url = csv_server()
    resources = [
        make_resource(url, resource_id="resource-1"),
        make_resource(url, resource_id="resource-2"),
    ]

    result = sync_resources(
        resources,
        state_store=inmemory_state,
        reader=DataPlaneResourceReader(transport=HttpxTransport()),
        destination_uri=f"file://{tmp_path}/dest",
    )

    assert hasattr(result, "__next__")
    first = next(result)
    assert first.resource.id == "resource-1"
    assert inmemory_state.get("resource-1") is not None
    assert inmemory_state.get("resource-2") is None
    assert server.captured_paths == ["/data.csv"]
