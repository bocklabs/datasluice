"""Tests for explicit catalog-backed dlt extraction and state mirroring."""

from __future__ import annotations

import importlib
import inspect
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import pytest

from datasluice.contracts.catalog.fakes import SyncReferenceConnector
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest, SyncCatalogClient
from datasluice.domain import HttpDownload, Resource
from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.models import ResourceRecord, ResultEnvelope
from datasluice.domain.catalog.operations import OperationId
from datasluice.integrations.dlt import _sanitize, datasluice_source, mirror_dlt_state
from datasluice.runtime.transport.base import RuntimeRequest
from datasluice.runtime.transport.urllib_transport import UrllibCatalogTransport
from datasluice.sync._identity import canonical_identity
from datasluice.sync.state_store import InMemoryStateStore
from tests.helpers.http_server import MockResponse, start_test_server

pytest.importorskip("dlt")
duckdb = pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")


class _ReferenceDltConnector(SyncReferenceConnector):
    """Reference connector extended with normalized resource-list behavior."""

    def __init__(self, resources: tuple[ResourceRecord, ...]) -> None:
        super().__init__()
        self._resources = resources
        self._transport = UrllibCatalogTransport()

    @property
    def transport(self) -> UrllibCatalogTransport:
        """Expose the caller-owned shared transport as the public accessor."""
        return self._transport

    @property
    def resources(self) -> _ReferenceDltConnector:
        """Return the normalized resource service."""
        return self

    @property
    def organizations(self) -> _ReferenceDltConnector:
        """Return the unused normalized organization service."""
        return self

    def list(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> ResultEnvelope[ResourceRecord]:
        """Return fixture resources for the supplied typed query."""
        guard.require_allowed()
        self.dispatches.append(str(operation.operation_id))
        return ResultEnvelope(items=self._resources)


class _TransportlessDltConnector(SyncReferenceConnector):
    """Protocol-compatible connector that never exposes a transport accessor."""

    @property
    def resources(self) -> _TransportlessDltConnector:
        """Return the normalized resource service."""
        return self

    @property
    def organizations(self) -> _TransportlessDltConnector:
        """Return the normalized organization service."""
        return self


def _query() -> CatalogOperationRequest:
    return CatalogOperationRequest(operation_id=OperationId(platform="reference", service="resources", method="list"))


def _make_resource(url: str, *, resource_id: str = "my-resource.csv") -> ResourceRecord:
    dataset_id = CatalogId(CatalogPlatform("reference"), ResourceKind.DATASET, "fixture-dataset")
    return ResourceRecord(
        id=CatalogId(CatalogPlatform("reference"), ResourceKind.RESOURCE, resource_id),
        dataset_id=dataset_id,
        name="People",
        url=url,
    )


def _make_pipeline(tmp_path: Path, name: str) -> tuple[Any, Path, str]:
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


def _extract_and_load(pipeline: Any, source: Any) -> None:
    pipeline.extract(source)
    pipeline.normalize()
    pipeline.load()


def test_source_requires_explicit_typed_client_and_query() -> None:
    """The public source accepts a normalized client and typed catalog operation."""
    signature = inspect.signature(datasluice_source)

    assert tuple(signature.parameters)[:2] == ("client", "query")
    assert "portal" not in signature.parameters
    with pytest.raises(TypeError, match="SyncCatalogClient"):
        datasluice_source(cast(Any, "https://portal.example.test"), _query())
    with pytest.raises(TypeError, match="CatalogOperationRequest"):
        datasluice_source(cast(SyncCatalogClient, _ReferenceDltConnector(())), cast(Any, "search"))


def test_source_requires_a_client_exposing_the_public_transport_accessor() -> None:
    """A protocol-compatible client without a public transport accessor is rejected early."""
    with pytest.raises(TypeError, match="transport"):
        datasluice_source(cast(SyncCatalogClient, _TransportlessDltConnector()), _query())


def test_source_uses_reference_connector_resources(tmp_path: Path) -> None:
    """The caller-owned reference connector supplies normalized extraction resources."""
    server, base_url = start_test_server(
        {"/data.csv": MockResponse(body=b"id,name\n1,Alice\n2,Bob\n", headers={"Content-Type": "text/csv"})}
    )
    try:
        connector = _ReferenceDltConnector((_make_resource(f"{base_url}/data.csv"),))
        assert isinstance(connector, SyncCatalogClient)
        pipeline, db_path, dataset_name = _make_pipeline(tmp_path, "reference_source")

        _extract_and_load(pipeline, datasluice_source(cast(SyncCatalogClient, connector), _query()))

        with duckdb.connect(str(db_path)) as connection:
            rows = connection.execute(f'SELECT id, name FROM "{dataset_name}"."my_resource_csv" ORDER BY id').fetchall()
        assert rows == [(1, "Alice"), (2, "Bob")]
        assert connector.dispatches == ["reference/resources.list"]
    finally:
        server.shutdown()
        server.server_close()


def test_two_resources_share_one_transport_that_stays_usable_after_extraction(tmp_path: Path) -> None:
    """Repeated extraction through one connector never closes the shared transport."""
    server, base_url = start_test_server(
        {
            "/alpha.csv": MockResponse(body=b"id,name\n1,Alice\n", headers={"Content-Type": "text/csv"}),
            "/beta.csv": MockResponse(body=b"id,name\n2,Bob\n", headers={"Content-Type": "text/csv"}),
        }
    )
    try:
        connector = _ReferenceDltConnector(
            (
                _make_resource(f"{base_url}/alpha.csv", resource_id="alpha.csv"),
                _make_resource(f"{base_url}/beta.csv", resource_id="beta.csv"),
            )
        )
        pipeline, db_path, dataset_name = _make_pipeline(tmp_path, "shared_transport")

        _extract_and_load(pipeline, datasluice_source(cast(SyncCatalogClient, connector), _query()))

        with duckdb.connect(str(db_path)) as connection:
            rows = connection.execute(
                f'SELECT id FROM "{dataset_name}"."alpha_csv" UNION ALL SELECT id FROM "{dataset_name}"."beta_csv"'
            ).fetchall()
        assert sorted(rows) == [(1,), (2,)]

        response = connector.transport.send(RuntimeRequest(method="GET", url=f"{base_url}/alpha.csv"))
        assert response.status_code == 200
        assert b"Alice" in response.body
    finally:
        connector.transport.close()
        server.shutdown()
        server.server_close()


def test_source_seeds_and_mirrors_load_committed_state(tmp_path: Path) -> None:
    """Extraction preserves the prior-watermark and post-load mirror behavior."""
    server, base_url = start_test_server(
        {"/data.csv": MockResponse(body=b"id,name\n1,Alice\n", headers={"Content-Type": "text/csv"})}
    )
    try:
        resource = _make_resource(f"{base_url}/data.csv")
        connector = _ReferenceDltConnector((resource,))
        store = InMemoryStateStore()
        pipeline, _, _ = _make_pipeline(tmp_path, "state_roundtrip")

        _extract_and_load(pipeline, datasluice_source(cast(SyncCatalogClient, connector), _query(), state_store=store))
        mirror_dlt_state(pipeline, store)

        identity = canonical_identity(
            Resource(
                id=resource.id.value,
                name=resource.name,
                url=resource.url,
                access=HttpDownload(url=resource.url or ""),
            )
        )
        state = store.get(identity)
        assert state is not None
        assert len(state.cursor[identity]) == 64
    finally:
        server.shutdown()
        server.server_close()


def test_source_rejects_missing_resource_url() -> None:
    """Normalized resources without a direct URL cannot be materialized by dlt."""
    resource = _make_resource("https://data.example.test/records.csv")
    missing_url = ResourceRecord(
        id=resource.id,
        dataset_id=resource.dataset_id,
        name=resource.name,
        url=None,
    )

    with pytest.raises(ValueError, match="direct URL"):
        datasluice_source(cast(SyncCatalogClient, _ReferenceDltConnector((missing_url,))), _query())


@pytest.mark.parametrize(
    ("resource_id", "expected"),
    [
        ("my-resource.csv", "my_resource_csv"),
        ("9a3f12ab-cd34", "_9a3f12ab_cd34"),
        ("9" * 64, "_" + "9" * 63),
        ("", "_"),
    ],
)
def test_sanitize_naming(resource_id: str, expected: str) -> None:
    assert _sanitize(resource_id) == expected


def test_dlt_and_catalog_imports_remain_lazy() -> None:
    """Importing the optional integration loads neither dlt nor catalog contracts."""
    code = """
import sys
sys.path.insert(0, "src")
assert "dlt" not in sys.modules
module = __import__("datasluice.integrations.dlt", fromlist=["datasluice_source"])
assert "dlt" not in sys.modules
assert "SyncCatalogClient" not in module.__dict__
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stderr
