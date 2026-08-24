"""Seam contracts: dual-surface CKAN live clients through the dlt extraction integration."""

from __future__ import annotations

import importlib
import json
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast

import pytest

from datasluice.connectors.catalog.ckan import CKANClientSettings, create_sync_client
from datasluice.contracts.catalog.fakes import SyncReferenceConnector
from datasluice.contracts.catalog.protocols import CatalogOperationRequest, SyncCatalogClient
from datasluice.domain.catalog.operations import OperationId
from datasluice.integrations.dlt import _sanitize, datasluice_source
from datasluice.runtime.transport.urllib_transport import UrllibCatalogTransport

pytest.importorskip("dlt")
duckdb = pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")

LOOPBACK_ORIGIN = "http://127.0.0.1:9001"
CSV_BODY = b"id,name\n1,Alice\n2,Bob\n"


class _TransportlessCkanLikeConnector(SyncReferenceConnector):
    """A protocol-compatible client that never exposes the public transport accessor."""

    @property
    def resources(self) -> _TransportlessCkanLikeConnector:
        """Return the normalized resource service."""
        return self

    @property
    def organizations(self) -> _TransportlessCkanLikeConnector:
        """Return the normalized organization service."""
        return self


class _FixtureServer(ThreadingHTTPServer):
    captured: list[dict[str, str]]
    captured_paths: list[str]
    responses: dict[str, tuple[int, bytes, str]]


class _ActionAndDataHandler(BaseHTTPRequestHandler):
    def _respond(self) -> None:
        server = cast(_FixtureServer, self.server)
        path = urllib.parse.urlparse(self.path).path
        length = self.headers.get("Content-Length")
        if length:
            self.rfile.read(int(length))
        server.captured.append({key.lower(): value for key, value in self.headers.items()})
        server.captured_paths.append(self.path)
        status, body, content_type = server.responses.get(path, (404, b"not found", "text/plain"))
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        self._respond()

    def do_POST(self) -> None:
        self._respond()

    def log_message(self, format: str, *args: Any) -> None:
        return


def _start_ckan_fixture() -> tuple[_FixtureServer, str]:
    """Start one loopback CKAN Action API plus data-file fixture server."""
    server = _FixtureServer(("127.0.0.1", 0), _ActionAndDataHandler)
    server.captured = []
    server.captured_paths = []
    server.responses = {}
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_address[1]}"
    envelope = {
        "success": True,
        "result": {
            "count": 1,
            "results": [
                {
                    "id": "res-0001",
                    "package_id": "dataset-0001",
                    "name": "People",
                    "url": f"{base_url}/people.csv",
                }
            ],
        },
    }
    server.responses = {
        "/api/3/action/resource_search": (
            200,
            json.dumps(envelope).encode("utf-8"),
            "application/json",
        ),
        "/people.csv": (200, CSV_BODY, "text/csv"),
    }
    return server, base_url


def _ckan_query() -> CatalogOperationRequest:
    return CatalogOperationRequest(operation_id=OperationId(platform="ckan", service="resources", method="list"))


def _loopback_client(base_url: str) -> tuple[Any, UrllibCatalogTransport]:
    transport = UrllibCatalogTransport()
    settings = CKANClientSettings(
        base_url=base_url,
        sync_transport=transport,
        probe_policy="declared-baseline",
    )
    return create_sync_client(settings), transport


def _make_pipeline(tmp_path: Any, name: str) -> tuple[Any, Any, str]:
    dlt = importlib.import_module("dlt")
    db_path = tmp_path / f"{name}.duckdb"
    dataset_name = f"{name}_data"
    pipeline = dlt.pipeline(
        pipeline_name=name,
        pipelines_dir=str(tmp_path / "pipelines"),
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name=dataset_name,
    )
    return pipeline, db_path, dataset_name


def test_dual_surface_client_satisfies_sync_catalog_client() -> None:
    """The real dual-surface CKAN client structurally satisfies the normalized contract."""
    client, _ = _loopback_client(LOOPBACK_ORIGIN)

    assert isinstance(client, SyncCatalogClient)


def test_ckan_live_client_flows_through_dlt_source_end_to_end(tmp_path: Any) -> None:
    """create_sync_client produces canned normalized records extracted through datasluice_source."""
    server, base_url = _start_ckan_fixture()
    transport: UrllibCatalogTransport | None = None
    try:
        client, transport = _loopback_client(base_url)
        assert client.transport is transport
        pipeline, db_path, dataset_name = _make_pipeline(tmp_path, "ckan_seam")

        pipeline.extract(datasluice_source(cast(SyncCatalogClient, client), _ckan_query()))
        pipeline.normalize()
        pipeline.load()

        with duckdb.connect(str(db_path)) as connection:
            rows = connection.execute(f'SELECT id, name FROM "{dataset_name}"."res_0001" ORDER BY id').fetchall()
        assert rows == [(1, "Alice"), (2, "Bob")]
        assert "/api/3/action/resource_search" in server.captured_paths
        action_calls = [p for p in server.captured_paths if p.endswith("/api/3/action/resource_search")]
        assert len(action_calls) == 1
        action_index = server.captured_paths.index(action_calls[0])
        assert server.captured[action_index]["content-type"] == "application/json"
    finally:
        if transport is not None:
            transport.close()
        server.shutdown()
        server.server_close()


def test_client_without_transport_accessor_raises_the_existing_type_error() -> None:
    """A protocol-compatible client lacking the public accessor is rejected with today's TypeError."""
    with pytest.raises(TypeError, match="exposing the public transport accessor"):
        datasluice_source(cast(SyncCatalogClient, _TransportlessCkanLikeConnector()), _ckan_query())


def test_non_resources_list_operation_raises_the_existing_value_error() -> None:
    """Any operation other than resources.list is rejected with today's ValueError."""
    query = CatalogOperationRequest(operation_id=OperationId(platform="ckan", service="datasets", method="list"))
    with pytest.raises(ValueError, match="requires a resources.list catalog operation"):
        datasluice_source(cast(SyncCatalogClient, _TransportlessCkanLikeConnector()), query)


def test_seam_table_naming_matches_the_resource_identifier() -> None:
    """The dlt table name derives deterministically from the CKAN resource id."""
    assert _sanitize("res-0001") == "res_0001"
