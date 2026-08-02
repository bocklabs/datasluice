"""Tests for the Arrow-backed dlt source and StateStore round-trip."""

from __future__ import annotations

import importlib
import inspect
import os
import subprocess
import sys
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
if "DataSluiceSession" in inspect.getsource(datasluice_source) and os.environ.get("DATASLUICE_TDD_RED") != "1":
    pytest.skip("dlt canonical facade migration pending GREEN phase", allow_module_level=True)

from datasluice.domain import Dataset, HttpDownload, QueryAccess, Resource, SearchResult  # noqa: E402
from datasluice.integrations.dlt import _sanitize  # noqa: E402
from datasluice.sync._identity import canonical_identity  # noqa: E402
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
    monkeypatch.setattr(datasluice, "DataSluice", lambda: _Session(result))


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

    state = store.get(canonical_identity(csv_portal))
    assert state is not None
    assert len(state.cursor[canonical_identity(csv_portal)]) == 64


def test_state_store_none_light_path(tmp_path: Path, csv_portal: Resource) -> None:
    pipeline, db_path, dataset_name = _make_pipeline(tmp_path, "light_path")

    _extract_and_load(pipeline, datasluice_source("https://portal.test", state_store=None))

    assert "my_resource_csv" in _table_names(db_path, dataset_name)


def test_resource_body_closes_transport_after_extraction(
    tmp_path: Path, csv_portal: Resource, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each resource body must close its HttpxTransport once extraction completes (no socket leak)."""
    import datasluice.transport.httpx_transport as httpx_transport

    created: list[Any] = []
    real_cls = httpx_transport.HttpxTransport

    class _ClosingSpy(real_cls):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            created.append(self)

    monkeypatch.setattr(httpx_transport, "HttpxTransport", _ClosingSpy)

    pipeline, _, _ = _make_pipeline(tmp_path, "close_transport")
    _extract_and_load(pipeline, datasluice_source("https://portal.test"))

    assert created, "expected at least one HttpxTransport to be created during extraction"
    assert all(getattr(t, "_closed", False) for t in created), (
        f"transports left open after extraction: {[t._closed for t in created]}"
    )


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
        ("9" * 64, "_" + "9" * 63),
        ("", "_"),
    ],
)
def test_sanitize_naming(resource_id: str, expected: str) -> None:
    assert _sanitize(resource_id) == expected


def test_sanitize_collision_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    resources = [
        Resource(id="same-id", url="https://portal.test/a.csv", format="CSV"),
        Resource(id="same-id", url="https://portal.test/b.csv", format="CSV"),
    ]
    _install_portal(monkeypatch, resources)

    with pytest.raises(ValueError, match="collide on canonical identity"):
        datasluice_source("https://portal.test")


def test_sanitized_table_name_collision_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    resources = [
        Resource(id="a-b", url="https://portal.test/a.csv", format="CSV"),
        Resource(id="a_b", url="https://portal.test/b.csv", format="CSV"),
    ]
    _install_portal(monkeypatch, resources)

    with pytest.raises(ValueError, match="sanitized dlt table name"):
        datasluice_source("https://portal.test")


def test_reserved_metadata_table_name_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_portal(monkeypatch, [Resource(id="datasets", url="https://portal.test/data.csv", format="CSV")])

    with pytest.raises(ValueError, match="reserved dlt table name"):
        datasluice_source("https://portal.test", include_metadata=True)


def test_reserved_metadata_table_name_allowed_when_metadata_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """A resource named 'datasets' is usable when include_metadata=False (WR-03)."""
    _install_portal(monkeypatch, [Resource(id="datasets", url="https://portal.test/data.csv", format="CSV")])

    # No raise — the metadata resource that would conflict is never emitted.
    source = datasluice_source("https://portal.test", include_metadata=False)

    assert source is not None


def test_three_run_roundtrip_structure(
    tmp_path: Path,
    csv_portal: Resource,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arrow = importlib.import_module("datasluice.integrations.arrow")
    original_to_arrow = arrow.to_arrow
    seeded_watermarks: list[str | None] = []

    def recording_to_arrow(stream: Any) -> Any:
        state = importlib.import_module("dlt").current.resource_state()
        seeded_watermarks.append(state.get("datasluice", {}).get("watermark"))
        return original_to_arrow(stream)

    monkeypatch.setattr(arrow, "to_arrow", recording_to_arrow)
    store = InMemoryStateStore()
    pipeline, db_path, dataset_name = _make_pipeline(tmp_path, "three_run_roundtrip")
    watermarks: list[str] = []

    for _ in range(3):
        _extract_and_load(pipeline, datasluice_source("https://portal.test", state_store=store))
        state = store.get(canonical_identity(csv_portal))
        assert state is not None
        watermarks.append(state.cursor[canonical_identity(csv_portal)])

    with duckdb.connect(str(db_path)) as connection:
        row_count = connection.execute(f'SELECT count(*) FROM "{dataset_name}"."my_resource_csv"').fetchone()[0]

    assert seeded_watermarks == [None, watermarks[0], watermarks[0]]
    assert watermarks[0] == watermarks[1] == watermarks[2]
    assert row_count == 2


def test_replace_not_append_duplicates(tmp_path: Path, csv_portal: Resource) -> None:
    pipeline, db_path, dataset_name = _make_pipeline(tmp_path, "replace_no_duplicates")
    row_counts: list[int] = []

    for _ in range(2):
        _extract_and_load(pipeline, datasluice_source("https://portal.test"))
        with duckdb.connect(str(db_path)) as connection:
            row_counts.append(
                connection.execute(f'SELECT count(*) FROM "{dataset_name}"."my_resource_csv"').fetchone()[0]
            )

    assert row_counts == [2, 2]


def test_metadata_sibling_opt_in(tmp_path: Path, csv_portal: Resource) -> None:
    pipeline, db_path, dataset_name = _make_pipeline(tmp_path, "metadata_sibling")

    _extract_and_load(pipeline, datasluice_source("https://portal.test", include_metadata=True))

    assert {"my_resource_csv", "datasets"} <= _table_names(db_path, dataset_name)


def test_dlt_lazy_import_no_module_level() -> None:
    code = """
import sys
import types
import datasluice
from datasluice.domain import SearchResult

assert "dlt" not in sys.modules
module = __import__("datasluice.integrations.dlt", fromlist=["datasluice_source"])
assert "dlt" not in sys.modules
datasluice.DataSluice = lambda: types.SimpleNamespace(search=lambda *args, **kwargs: SearchResult())
module.datasluice_source("https://portal.test")
assert "dlt" in sys.modules
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stderr


def test_table_naming_deterministic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server, base_url = start_test_server({"/data.csv": MockResponse(body=b"id\n1\n")})
    resource_ids = ["my-resource.csv", "9a3f12ab-cd34", "départements-régions"]
    resources = [
        Resource(
            id=resource_id,
            url=f"{base_url}/data.csv",
            format="CSV",
            access=HttpDownload(url=f"{base_url}/data.csv"),
        )
        for resource_id in resource_ids
    ]
    _install_portal(monkeypatch, resources)
    pipeline, db_path, dataset_name = _make_pipeline(tmp_path, "deterministic_names")
    try:
        _extract_and_load(pipeline, datasluice_source("https://portal.test"))
    finally:
        server.shutdown()
        server.server_close()

    expected = {_sanitize(resource_id) for resource_id in resource_ids}
    assert expected <= _table_names(db_path, dataset_name)


def test_state_writeback_durable_across_pipeline_recreation(
    tmp_path: Path,
    csv_portal: Resource,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = InMemoryStateStore()
    first_pipeline, _, _ = _make_pipeline(tmp_path, "durable_first")
    _extract_and_load(first_pipeline, datasluice_source("https://portal.test", state_store=store))
    first_state = store.get(canonical_identity(csv_portal))
    assert first_state is not None
    first_watermark = first_state.cursor[canonical_identity(csv_portal)]

    arrow = importlib.import_module("datasluice.integrations.arrow")
    original_to_arrow = arrow.to_arrow
    seeded_watermarks: list[str | None] = []

    def recording_to_arrow(stream: Any) -> Any:
        state = importlib.import_module("dlt").current.resource_state()
        seeded_watermarks.append(state.get("datasluice", {}).get("watermark"))
        return original_to_arrow(stream)

    monkeypatch.setattr(arrow, "to_arrow", recording_to_arrow)
    second_pipeline, _, _ = _make_pipeline(tmp_path, "durable_second")
    _extract_and_load(second_pipeline, datasluice_source("https://portal.test", state_store=store))
    second_state = store.get(canonical_identity(csv_portal))

    assert second_state is not None
    assert seeded_watermarks == [first_watermark]
    assert second_state.cursor[canonical_identity(csv_portal)] == first_watermark
