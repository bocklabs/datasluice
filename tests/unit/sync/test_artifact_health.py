"""Destination health verification for idempotent sync outcomes."""

from __future__ import annotations

import importlib
import os
from unittest.mock import patch

import pytest

from datasluice.data import DataPlaneResourceReader
from datasluice.domain import Artifact
from datasluice.io.filesystem import open_filesystem
from datasluice.sync import sync_resources
from datasluice.sync._identity import canonical_identity
from datasluice.sync.state_store import InMemoryStateStore
from datasluice.transport.httpx_transport import HttpxTransport
from tests.unit.sync.conftest import write_counting_fs

materialize_module = importlib.import_module("datasluice.sync.materialize")
_ARTIFACT_HEALTH_READY = hasattr(materialize_module, "_ARTIFACT_HEALTH_READY")
_SKIP_ARTIFACT_HEALTH = not _ARTIFACT_HEALTH_READY and os.environ.get("DATASLUICE_TDD_RED") != "1"


def _sync(tmp_path, resource, store, transport, *, resume: bool = False):
    return list(
        sync_resources(
            [resource],
            state_store=store,
            reader=DataPlaneResourceReader(transport=transport),
            destination_uri=f"file://{tmp_path}/dest",
            transport=transport,
            resume=resume,
        )
    )


@pytest.mark.skipif(
    os.environ.get("DATASLUICE_TDD_RED") != "1",
    reason="Artifact boundary implementation pending GREEN phase",
)
def test_sync_outcome_record_is_not_tuple_compatible(tmp_path, csv_server, make_resource) -> None:
    _server, url = csv_server()
    outcome = _sync(tmp_path, make_resource(url), InMemoryStateStore(), HttpxTransport())[0]

    assert isinstance(outcome.record, Artifact)
    with pytest.raises(TypeError):
        _ = outcome.record[0]


@pytest.mark.skipif(_SKIP_ARTIFACT_HEALTH, reason="destination health implementation pending GREEN phase")
def test_corrupt_destination_rematerializes(tmp_path, csv_server, make_resource) -> None:
    _server, url = csv_server()
    resource = make_resource(url)
    transport = HttpxTransport()
    store = InMemoryStateStore()
    destination = f"file://{tmp_path}/dest"

    with patch("datasluice.io.filesystem.open_filesystem") as open_fs:
        fs = open_filesystem(destination)
        open_fs.return_value = fs
        first = _sync(tmp_path, resource, store, transport)
        record = first[0].record
        assert record is not None
        expected_bytes = fs.cat_file(record[0])
        fs.pipe_file(record[0], b"foreign destination bytes")
        second = _sync(tmp_path, resource, store, transport)

    assert second[0].action == "materialized"
    assert second[0].record is not None
    assert fs.cat_file(second[0].record[0]) == expected_bytes
    state = store.get(canonical_identity(resource))
    assert state is not None
    assert state.cursor[canonical_identity(resource)] == second[0].record[3]


@pytest.mark.skipif(_SKIP_ARTIFACT_HEALTH, reason="destination health implementation pending GREEN phase")
def test_missing_destination_rematerializes(tmp_path, csv_server, make_resource) -> None:
    _server, url = csv_server()
    resource = make_resource(url)
    transport = HttpxTransport()
    store = InMemoryStateStore()
    destination = f"file://{tmp_path}/dest"

    with patch("datasluice.io.filesystem.open_filesystem") as open_fs:
        fs = open_filesystem(destination)
        open_fs.return_value = fs
        first = _sync(tmp_path, resource, store, transport)
        record = first[0].record
        assert record is not None
        fs.rm(record[0])
        second = _sync(tmp_path, resource, store, transport)

    assert second[0].action == "materialized"
    assert second[0].record is not None
    assert fs.exists(second[0].record[0])


@pytest.mark.skipif(_SKIP_ARTIFACT_HEALTH, reason="destination health implementation pending GREEN phase")
def test_healthy_destination_zero_write_remains(tmp_path, csv_server, make_resource) -> None:
    _server, url = csv_server()
    resource = make_resource(url)
    transport = HttpxTransport()
    store = InMemoryStateStore()
    destination = f"file://{tmp_path}/dest"
    counting_fs = write_counting_fs(open_filesystem(destination))

    with patch("datasluice.io.filesystem.open_filesystem", return_value=counting_fs):
        first = _sync(tmp_path, resource, store, transport)
        writes_after_first = counting_fs.pipe_file_count
        second = _sync(tmp_path, resource, store, transport)

    assert first[0].action == "materialized"
    assert second[0].action == "skipped-unchanged"
    assert counting_fs.pipe_file_count == writes_after_first


@pytest.mark.skipif(_SKIP_ARTIFACT_HEALTH, reason="destination health implementation pending GREEN phase")
def test_completed_resume_rematerializes_unhealthy_destination(tmp_path, csv_server, make_resource) -> None:
    _server, url = csv_server()
    resource = make_resource(url)
    transport = HttpxTransport()
    store = InMemoryStateStore()
    destination = f"file://{tmp_path}/dest"

    first = _sync(tmp_path, resource, store, transport)
    record = first[0].record
    assert record is not None
    fs = open_filesystem(destination)
    fs.rm(record[0])
    resumed = _sync(tmp_path, resource, store, transport, resume=True)

    assert resumed[0].action == "materialized"
    assert resumed[0].record is not None
    assert fs.exists(resumed[0].record[0])


@pytest.mark.skipif(_SKIP_ARTIFACT_HEALTH, reason="destination health implementation pending GREEN phase")
def test_304_rematerializes_unhealthy_destination(tmp_path, csv_server, make_resource) -> None:
    _server, url = csv_server(headers={"ETag": '"stable"'})
    resource = make_resource(url)
    transport = HttpxTransport()
    store = InMemoryStateStore()
    destination = f"file://{tmp_path}/dest"

    first = _sync(tmp_path, resource, store, transport)
    record = first[0].record
    assert record is not None
    fs = open_filesystem(destination)
    fs.pipe_file(record[0], b"foreign destination bytes")
    second = _sync(tmp_path, resource, store, transport)

    assert second[0].action == "materialized"
    assert second[0].record is not None
    assert fs.exists(second[0].record[0])
