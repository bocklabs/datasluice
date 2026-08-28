"""Installed-artifact proof for the strict uData tracer slice."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path

from datasluice.connectors.catalog.udata.clients import declared_udata_profile

_ROOT = Path(__file__).resolve().parents[2]

_DATASET_OPERATION_ID = next(op_id for op_id in declared_udata_profile().operations if "dataset" in op_id.method)


def _build_wheel(tmp_path: Path) -> Path:
    dist = tmp_path / "dist"
    environment = dict(os.environ)
    environment.setdefault("UV_CACHE_DIR", str(tmp_path / "uv-cache"))
    completed = subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(dist)],
        cwd=_ROOT,
        capture_output=True,
        text=True,
        timeout=300,
        env=environment,
    )
    assert completed.returncode == 0, completed.stderr
    wheels = list(dist.glob("datasluice-*.whl"))
    assert len(wheels) == 1
    return wheels[0]


def _unpacked_wheel_source(tmp_path: Path) -> Path:
    wheel = _build_wheel(tmp_path)
    unpacked = tmp_path / "wheel"
    with zipfile.ZipFile(wheel) as archive:
        archive.extractall(unpacked)
    return unpacked


def test_wheel_ships_udata_176_contract_files_and_no_legacy_profile(tmp_path: Path) -> None:
    unpacked = _unpacked_wheel_source(tmp_path)
    package = unpacked / "datasluice" / "connectors" / "catalog" / "udata"
    profiles = unpacked / "datasluice" / "contracts" / "catalog" / "profiles"

    for module in ("settings.py", "clients.py", "probes.py", "mapping.py", "live.py", "factory.py", "connector.py"):
        assert (package / module).is_file(), module
    assert (profiles / "udata-17.6.json").is_file()
    assert not (profiles / "udata-17.3.json").exists()

    profile = json.loads((profiles / "udata-17.6.json").read_text(encoding="utf-8"))
    assert profile["profile_version"] == "17.6.0"
    assert profile["platform"] == "udata"


def test_wheel_import_proves_the_tracer_path_from_installed_content(tmp_path: Path) -> None:
    """A fresh interpreter importing only the unpacked wheel runs the full tracer."""
    unpacked = _unpacked_wheel_source(tmp_path)
    script = """
import json, sys
sys.path.insert(0, sys.argv[1])
import datasluice
assert datasluice.__file__ and datasluice.__file__.startswith(sys.argv[1])
from datasluice.connectors.catalog.udata.clients import create_sync_client, declared_udata_profile
from datasluice.connectors.catalog.udata.probes import UDataVersionError
from datasluice.connectors.catalog.udata.settings import UDataClientSettings
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.runtime.transport.base import RuntimeResponse

op_id = next(op for op in declared_udata_profile().operations if "dataset" in op.method)

class Transport:
    def __init__(self, version="17.6.0"):
        self.requests = []
        self.close_count = 0
        self.version = version

    def send(self, request):
        url = request.url
        self.requests.append(url)
        if url.endswith("/api/1/site/"):
            body = json.dumps({"feed_size": 0, "id": "s", "keywords": [], "metrics": {},
                               "title": "uData", "version": self.version}).encode()
        else:
            body = json.dumps({"data": [{"id": "abc", "title": "T"}], "next_page": None, "page": 1,
                               "page_size": 20, "previous_page": None, "total": 1}).encode()
        return RuntimeResponse(status_code=200, headers={}, body=body)

    def close(self):
        self.close_count += 1

transport = Transport()
client = create_sync_client(UDataClientSettings(base_url="http://127.0.0.1:5640", sync_transport=transport))
assert client.site_version().version == "17.6.0"
envelope = client.datasets_list(
    CatalogOperationRequest(operation_id=op_id, payload={}),
    CatalogOperationGuard(operation_id=op_id),
)
client.close()
recorded = [getattr(r, "url", r) for r in transport.requests]
assert recorded == [
    "http://127.0.0.1:5640/api/1/site/",
    "http://127.0.0.1:5640/api/1/datasets/",
], recorded
assert transport.close_count == 0
assert envelope.items[0].id.value == "abc"



blocked = create_sync_client(
    UDataClientSettings(base_url="http://127.0.0.1:5640", sync_transport=Transport(version="17.7"))
)
try:
    blocked.datasets_list(
        CatalogOperationRequest(operation_id=op_id, payload={}),
        CatalogOperationGuard(operation_id=op_id),
    )
except UDataVersionError as error:
    assert len(blocked.transport.requests) == 1
else:
    raise AssertionError("version mismatch did not block dispatch")
blocked.close()
print("TRACER_OK")
"""
    completed = subprocess.run(
        [sys.executable, "-c", script, str(unpacked)],
        capture_output=True,
        text=True,
        timeout=120,
        cwd=tmp_path,
    )
    assert completed.returncode == 0, completed.stderr
    assert "TRACER_OK" in completed.stdout
