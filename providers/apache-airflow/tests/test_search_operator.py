"""Bounded DataSluice search operator contract tests."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import airflow.providers.datasluice.operators.search as search_module
import pytest
from airflow.providers.datasluice.operators.search import DataSluiceSearchOperator

from datasluice import CatalogResourceLocator, DataSluiceError, Resource, SearchResult

_FIXTURES = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "contracts"
_PORTAL_URL = "https://catalog.example.test/api"


class _FakeHook:
    instances: list[_FakeHook] = []
    result: object = []

    def __init__(self, airflow_conn_id: str = "datasluice_default") -> None:
        self.airflow_conn_id = airflow_conn_id
        self.calls: list[tuple[object, object, int]] = []
        type(self).instances.append(self)

    def search(self, portal_url: object, query: object, *, max_results: int = 50) -> object:
        self.calls.append((portal_url, query, max_results))
        return type(self).result


@pytest.fixture
def fake_hook(monkeypatch: pytest.MonkeyPatch) -> type[_FakeHook]:
    _FakeHook.instances = []
    _FakeHook.result = []
    monkeypatch.setattr(search_module, "DataSluiceHook", _FakeHook)
    return _FakeHook


def _operator(**kwargs: object) -> DataSluiceSearchOperator:
    values: dict[str, object] = {
        "task_id": "search",
        "portal_url": _PORTAL_URL,
        "query": "open data",
    }
    values.update(kwargs)
    return DataSluiceSearchOperator(**values)


def _catalog_locator(*, extension_value: str = "") -> dict[str, object]:
    return CatalogResourceLocator(
        portal_url=_PORTAL_URL,
        dataset_id="dataset-42",
        resource_id="resource-7",
        extensions={"org.datasluice.boundary": {"payload": extension_value}},
    ).to_dict()


def _encoded_size(value: object) -> int:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return len(encoded)


def _sized_result(target_size: int) -> list[dict[str, object]]:
    base = [_catalog_locator()]
    padding = "x" * (target_size - _encoded_size(base))
    result = [_catalog_locator(extension_value=padding)]
    assert _encoded_size(result) == target_size
    return result


def test_constructor_is_pure_and_uses_default_limit(fake_hook: type[_FakeHook]) -> None:
    operator = _operator()

    assert operator.max_results == 50
    assert fake_hook.instances == []


def test_execute_propagates_query_and_limit_and_returns_fixture_locators(
    fake_hook: type[_FakeHook],
) -> None:
    fixture = json.loads((_FIXTURES / "locator-v1.json").read_text(encoding="utf-8"))
    fake_hook.result = [fixture["catalog"]]
    operator = _operator(airflow_conn_id="configured", max_results=7)

    result = operator.execute({})

    assert result == [fixture["catalog"]]
    assert fake_hook.instances[0].airflow_conn_id == "configured"
    assert fake_hook.instances[0].calls == [(_PORTAL_URL, "open data", 7)]


@pytest.mark.parametrize("limit", [1, 1000])
def test_execute_accepts_inclusive_result_limit_boundaries(fake_hook: type[_FakeHook], limit: int) -> None:
    operator = _operator(max_results=limit)

    assert operator.execute({}) == []
    assert fake_hook.instances[0].calls[0][2] == limit


@pytest.mark.parametrize("limit", [0, 1001])
def test_invalid_limit_fails_before_hook_construction(fake_hook: type[_FakeHook], limit: int) -> None:
    operator = _operator(max_results=limit)

    with pytest.raises(DataSluiceError, match="between 1 and 1000"):
        operator.execute({})

    assert fake_hook.instances == []


def test_result_count_fails_before_xcom_return(fake_hook: type[_FakeHook]) -> None:
    fake_hook.result = [_catalog_locator(), _catalog_locator(extension_value="second")]

    with pytest.raises(DataSluiceError, match="max_results"):
        _operator(max_results=1).execute({})


def test_empty_result_is_json_serializable_and_contains_no_core_objects(fake_hook: type[_FakeHook]) -> None:
    result = _operator().execute({})

    assert result == []
    assert json.loads(json.dumps(result)) == []
    assert not any(isinstance(value, (bytes, Resource, SearchResult)) for value in result)


@pytest.mark.parametrize("invalid", [SearchResult(), [b"payload"], [Resource(id="resource-1")]])
def test_non_locator_results_cannot_escape_through_xcom(fake_hook: type[_FakeHook], invalid: object) -> None:
    fake_hook.result = invalid

    with pytest.raises(DataSluiceError):
        _operator().execute({})


def test_xcom_validation_is_pure_and_uses_utf8_compact_json() -> None:
    from airflow.providers.datasluice.operators._xcom import validate_xcom_payload

    payload: dict[str, Any] = {"z": "é", "a": {"items": [1, True, None]}}
    original = copy.deepcopy(payload)

    result = validate_xcom_payload(payload)

    assert result == original
    assert payload == original
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    assert len(encoded) < 49152


@pytest.mark.parametrize("target_size", [49152, 49153])
def test_xcom_payload_size_ceiling_is_exact(fake_hook: type[_FakeHook], target_size: int) -> None:
    fake_hook.result = _sized_result(target_size)
    operator = _operator()

    if target_size == 49152:
        result = operator.execute({})
        assert _encoded_size(result) == 49152
    else:
        with pytest.raises(DataSluiceError, match="49152"):
            operator.execute({})
