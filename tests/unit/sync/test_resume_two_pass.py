"""Two-pass checkpoint and resume behavior under a deterministic crash."""

from __future__ import annotations

import importlib
import os
from typing import Any

import pytest

from datasluice.data import DataPlaneResourceReader
from datasluice.runtime.transport.httpx_transport import HttpxCatalogTransport
from datasluice.sync import sync_resources
from datasluice.sync._identity import canonical_identity
from datasluice.sync.state_store import InMemoryStateStore
from tests.unit.sync.conftest import CSV_BYTES, FaultInjectingStateStore

sync_module = importlib.import_module("datasluice.sync.sync")
if not hasattr(sync_module, "_CONDITIONAL_SYNC_READY") and os.environ.get("DATASLUICE_TDD_RED") != "1":
    pytest.skip("resume implementation pending GREEN phase", allow_module_level=True)


def test_crash_then_resume_skips_completed_resource(
    tmp_path,
    csv_server_multi,
    make_resource,
) -> None:
    server, base_url = csv_server_multi(
        {
            "/r1.csv": CSV_BYTES,
            "/r2.csv": b"id,name\n2,B\n",
            "/r3.csv": b"id,name\n3,C\n",
        }
    )
    resources = [
        make_resource(f"{base_url}/r1.csv", resource_id="r1"),
        make_resource(f"{base_url}/r2.csv", resource_id="r2"),
        make_resource(f"{base_url}/r3.csv", resource_id="r3"),
    ]
    identities = {resource.id: canonical_identity(resource) for resource in resources}
    first_store = InMemoryStateStore()
    crashing_store = FaultInjectingStateStore(first_store, raise_on_put=2)
    first_transport = HttpxCatalogTransport()

    with pytest.raises(RuntimeError, match="injected crash"):
        list(
            sync_resources(
                resources,
                state_store=crashing_store,
                reader=DataPlaneResourceReader(transport=first_transport),
                destination_uri=f"file://{tmp_path}/dest",
                transport=first_transport,
            )
        )

    assert first_store.get(identities["r1"]) is not None
    assert first_store.get(identities["r2"]) is None
    assert first_store.get(identities["r3"]) is None
    assert server.captured_paths == ["/r1.csv", "/r1.csv", "/r2.csv", "/r2.csv"]

    resumed_store = InMemoryStateStore()
    completed_r1 = first_store.get(identities["r1"])
    assert completed_r1 is not None
    resumed_store.put(identities["r1"], completed_r1)
    server.captured.clear()
    server.captured_paths.clear()
    second_transport = HttpxCatalogTransport()

    resume_sync: Any = sync_module.sync_resources
    outcomes = list(
        resume_sync(
            resources,
            state_store=resumed_store,
            reader=DataPlaneResourceReader(transport=second_transport),
            destination_uri=f"file://{tmp_path}/dest",
            transport=second_transport,
            resume=True,
        )
    )

    assert [outcome.action for outcome in outcomes] == ["resumed", "materialized", "materialized"]
    assert "/r1.csv" not in server.captured_paths
    assert server.captured_paths == ["/r2.csv", "/r2.csv", "/r3.csv", "/r3.csv"]
    assert resumed_store.get(identities["r1"]) == completed_r1
    assert resumed_store.get(identities["r2"]) is not None
    assert resumed_store.get(identities["r3"]) is not None
