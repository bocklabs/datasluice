"""Tests for the Arrow-backed dlt source and StateStore round-trip."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("dlt")
duckdb = pytest.importorskip("duckdb")
pytest.importorskip("pyarrow")

datasluice_source: Any
try:
    _dlt_module = importlib.import_module("datasluice.integrations.dlt")
    datasluice_source = getattr(_dlt_module, "datasluice_source", None)
except ImportError:
    datasluice_source = None

if datasluice_source is None:
    pytest.skip("datasluice_source rebuild not yet implemented (RED -> GREEN)", allow_module_level=True)

from datasluice.domain import Dataset, HttpDownload, QueryAccess, Resource, SearchResult  # noqa: E402
from datasluice.integrations.dlt import _sanitize  # noqa: E402
from datasluice.sync.state_store import InMemoryStateStore  # noqa: E402
from tests.helpers.http_server import MockResponse, start_test_server  # noqa: E402


class _Session:
    def __init__(self, result: SearchResult) -> None:
        self._result = result

    def search(self, portal: str, query: Any) -> SearchResult:
        return self._result


def _install_portal(monkeypatch: pytest.MonkeyPatch, resources: list[Resource]) -> None:
    import datasluice

    result = SearchResult(
        datasets=[Dataset(id="dataset-1", title="Dataset 1", name="dataset-1", resources=resources)],
        total=1,
    )
    monkeypatch.setattr(datasluice, "DataSluiceSession", lambda: _Session(result))


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


def _table_names(db_path: Path, dataset_name: str) -> set[str]:
    with duckdb.connect(str(db_path)) as connection:
        rows = connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
            [dataset_name],
        ).fetchall()
    return {row[0] for row in rows}


@pytest.fixture
def csv_portal(monkeypatch: pytest.MonkeyPatch) -> Any:
    server, base_url = start_test_server(
        {"/data.csv": MockResponse(body=b"id,name\n1,Alice\n2,Bob\n", headers={"Content-Type": "text/csv"})}
    )
    resource = Resource(
        id="my-resource.csv",
        name="People",
        url=f"{base_url}/data.csv",
        format="CSV",
        access=HttpDownload(url=f"{base_url}/data.csv"),
    )
    _install_portal(monkeypatch, [resource])
    try:
        yield resource
    finally:
        server.shutdown()
        server.server_close()


def test_arrow_yield_into_destination(tmp_path: Path, csv_portal: Resource) -> None:
    pipeline, db_path, dataset_name = _make_pipeline(tmp_path, "arrow_yield")

    _extract_and_load(pipeline, datasluice_source("https://portal.test"))

    with duckdb.connect(str(db_path)) as connection:
        rows = connection.execute(f'SELECT id, name FROM "{dataset_name}"."my_resource_csv" ORDER BY id').fetchall()
    assert rows == [(1, "Alice"), (2, "Bob")]


def test_state_roundtrip_run1_seeds_state(tmp_path: Path, csv_portal: Resource) -> None:
    store = InMemoryStateStore()
    pipeline, _, _ = _make_pipeline(tmp_path, "state_run1")

    _extract_and_load(pipeline, datasluice_source("https://portal.test", state_store=store))

    state = store.get(csv_portal.id)
    assert state is not None
    assert len(state.cursor[csv_portal.id]) == 64


def test_state_store_none_light_path(tmp_path: Path, csv_portal: Resource) -> None:
    pipeline, db_path, dataset_name = _make_pipeline(tmp_path, "light_path")

    _extract_and_load(pipeline, datasluice_source("https://portal.test", state_store=None))

    assert "my_resource_csv" in _table_names(db_path, dataset_name)


def test_skip_unsupported_in_dlt(monkeypatch: pytest.MonkeyPatch) -> None:
    query_resource = Resource(
        id="query-only",
        name="Query view",
        format="JSON",
        access=QueryAccess(endpoint="https://portal.test/query", query_language="sql"),
    )
    supported = Resource(
        id="download",
        name="Download",
        url="https://portal.test/data.csv",
        format="CSV",
        access=HttpDownload(url="https://portal.test/data.csv"),
    )
    _install_portal(monkeypatch, [query_resource, supported])

    source = datasluice_source("https://portal.test")

    assert set(source.resources.keys()) == {"download"}


@pytest.mark.parametrize(
    ("resource_id", "expected"),
    [
        ("my-resource.csv", "my_resource_csv"),
        ("9a3f12ab-cd34", "_9a3f12ab_cd34"),
        ("", "_"),
    ],
)
def test_sanitize_naming(resource_id: str, expected: str) -> None:
    assert _sanitize(resource_id) == expected


def test_sanitize_collision_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    resources = [
        Resource(id="a-b", url="https://portal.test/a.csv", format="CSV"),
        Resource(id="a_b", url="https://portal.test/b.csv", format="CSV"),
    ]
    _install_portal(monkeypatch, resources)

    with pytest.raises(ValueError, match="collide after sanitization"):
        datasluice_source("https://portal.test")
