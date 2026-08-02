"""One-resource DataSluice materialize operator contract tests."""

from __future__ import annotations

import copy
import json
import runpy
from datetime import datetime
from pathlib import Path

import airflow.providers.datasluice.operators.materialize as materialize_module
import pytest
from airflow.providers.datasluice.operators.materialize import DataSluiceMaterializeOperator
from airflow.providers.datasluice.operators.search import DataSluiceSearchOperator
from airflow.sdk import DAG

from datasluice import Artifact, CatalogResourceLocator, DataSluiceError, DirectResourceLocator

_ROOT = Path(__file__).resolve().parents[3]
_FIXTURES = _ROOT / "tests" / "fixtures" / "contracts"


class _FakeHook:
    instances: list[_FakeHook] = []
    result: Artifact | None = None

    def __init__(self, airflow_conn_id: str = "datasluice_default") -> None:
        self.airflow_conn_id = airflow_conn_id
        self.calls: list[tuple[object, str, str]] = []
        type(self).instances.append(self)

    def materialize(self, locator: object, destination_uri: str, *, mode: str = "parquet") -> Artifact:
        self.calls.append((locator, destination_uri, mode))
        if type(self).result is None:
            raise AssertionError("fake materialize result was not configured")
        return type(self).result


@pytest.fixture
def fake_hook(monkeypatch: pytest.MonkeyPatch) -> type[_FakeHook]:
    _FakeHook.instances = []
    _FakeHook.result = None
    monkeypatch.setattr(materialize_module, "DataSluiceHook", _FakeHook)
    return _FakeHook


def _fixture() -> dict[str, object]:
    return json.loads((_FIXTURES / "artifact-v1.json").read_text(encoding="utf-8"))


def _artifact(payload: dict[str, object] | None = None) -> Artifact:
    return Artifact.from_dict(payload or _fixture())


def _locator() -> dict[str, object]:
    return {
        "schema_version": 1,
        "kind": "direct",
        "uri": "https://data.example.test/files/observations.csv?api_key=***&page=1",
        "format": None,
        "media_type": None,
        "extensions": {},
    }


def _encoded_size(value: object) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8"))


def _sized_artifact(target_size: int) -> Artifact:
    base = _fixture()
    base["extensions"] = {"org.datasluice.boundary": {"payload": ""}}
    base_size = _encoded_size(base)
    payload = copy.deepcopy(base)
    payload["extensions"] = {"org.datasluice.boundary": {"payload": "x" * (target_size - base_size)}}
    result = _artifact(payload)
    assert _encoded_size(result.to_dict()) == target_size
    return result


def _operator(**kwargs: object) -> DataSluiceMaterializeOperator:
    values: dict[str, object] = {
        "task_id": "materialize",
        "locator": _locator(),
        "destination_uri": "memory://output/result.parquet",
    }
    values.update(kwargs)
    return DataSluiceMaterializeOperator(**values)


def test_constructor_is_pure_and_accepts_one_locator(fake_hook: type[_FakeHook]) -> None:
    operator = _operator()

    assert operator.mode == "parquet"
    assert operator.airflow_conn_id == "datasluice_default"
    assert fake_hook.instances == []


def test_execute_materializes_once_and_returns_exact_artifact_fixture(fake_hook: type[_FakeHook]) -> None:
    fixture = _fixture()
    fake_hook.result = _artifact(fixture)

    result = _operator(airflow_conn_id="configured", mode="raw").execute({})

    assert result == fixture
    assert set(result) == {
        "schema_version",
        "kind",
        "uri",
        "media_type",
        "size",
        "content_digest",
        "blob_digest",
        "provenance",
        "metadata",
        "extensions",
    }
    assert fake_hook.instances[0].airflow_conn_id == "configured"
    locator, destination, mode = fake_hook.instances[0].calls[0]
    assert isinstance(locator, DirectResourceLocator)
    assert destination == "memory://output/result.parquet"
    assert mode == "raw"
    assert len(fake_hook.instances[0].calls) == 1
    assert json.loads(json.dumps(result)) == fixture


def test_execute_rejects_list_before_hook_construction(fake_hook: type[_FakeHook]) -> None:
    operator = _operator(locator=[_locator()])

    with pytest.raises(DataSluiceError, match="exactly one"):
        operator.execute({})

    assert fake_hook.instances == []


def test_execute_rejects_invalid_mode_before_hook_construction(fake_hook: type[_FakeHook]) -> None:
    with pytest.raises(DataSluiceError, match="mode"):
        _operator(mode="csv").execute({})

    assert fake_hook.instances == []


@pytest.mark.parametrize(
    "locator",
    [[], [_locator(), _locator()], CatalogResourceLocator("https://portal.test", "d", "r")],
)
def test_operator_accepts_only_one_locator_dictionary(fake_hook: type[_FakeHook], locator: object) -> None:
    with pytest.raises(DataSluiceError):
        _operator(locator=locator).execute({})

    assert fake_hook.instances == []


def test_materialize_mapping_owns_fanout() -> None:
    with DAG(dag_id="operator-mapping", start_date=datetime(2026, 1, 1), schedule=None, catchup=False):
        search = DataSluiceSearchOperator(task_id="search", portal_url="https://portal.test", query="data")
        mapped = DataSluiceMaterializeOperator.partial(
            task_id="materialize",
            destination_uri="memory://output/{{ ti.map_index }}/result.parquet",
        ).expand(locator=search.output)

    assert mapped.__class__.__name__ == "MappedOperator"


def test_sample_dag_imports_and_maps_search_output() -> None:
    dag_path = _ROOT / "providers" / "apache-airflow" / "tests" / "dags" / "example_datasluice.py"

    namespace = runpy.run_path(str(dag_path))
    dag = namespace["dag"]
    materialize = dag.get_task("materialize")
    source = dag_path.read_text(encoding="utf-8")

    assert dag.dag_id == "datasluice_mapped_materialize"
    assert materialize.__class__.__name__ == "MappedOperator"
    assert ".expand(locator=search.output)" in source


@pytest.mark.parametrize("target_size", [49152, 49153])
def test_artifact_xcom_size_ceiling_is_exact(fake_hook: type[_FakeHook], target_size: int) -> None:
    fake_hook.result = _sized_artifact(target_size)

    if target_size == 49152:
        result = _operator().execute({})
        assert _encoded_size(result) == 49152
    else:
        with pytest.raises(DataSluiceError, match="49152"):
            _operator().execute({})


def test_materialize_output_contains_only_json_native_values(fake_hook: type[_FakeHook]) -> None:
    fake_hook.result = _artifact()
    result = _operator().execute({})

    def assert_json_native(value: object) -> None:
        if isinstance(value, dict):
            for key, nested in value.items():
                assert isinstance(key, str)
                assert_json_native(nested)
        elif isinstance(value, list):
            for nested in value:
                assert_json_native(nested)
        else:
            assert value is None or isinstance(value, (str, bool, int, float))

    assert_json_native(result)
