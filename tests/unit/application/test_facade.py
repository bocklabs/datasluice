"""Application facade delegation and resource-resolution contracts."""

from __future__ import annotations

import importlib
import os
from typing import Any, cast

import pytest

from datasluice.domain import Dataset, DetectionResult, HttpDownload, Query, Resource, SearchResult

application = importlib.import_module("datasluice.application")
_REQUIRED_CONTRACTS = (
    "detect_portal",
    "materialize",
    "open_resource",
    "read_stream",
    "run_transform_pipeline",
    "search_datasets",
)
_missing = tuple(name for name in _REQUIRED_CONTRACTS if not hasattr(application, name))
if _missing:
    if os.environ.get("DATASLUICE_TDD_RED") == "1":
        pytest.skip("application service contracts pending GREEN phase", allow_module_level=True)
    pytest.fail(f"missing public application service contracts: {_missing}", pytrace=False)


application_contracts = cast(Any, application)
DataSluice = application_contracts.DataSluice
CatalogResourceLocator = application_contracts.CatalogResourceLocator
ResourceResolutionError = application_contracts.ResourceResolutionError
detect_portal = application_contracts.detect_portal
search_datasets = application_contracts.search_datasets


class _Connector:
    def __init__(self, result: SearchResult, dataset: Dataset) -> None:
        self._result = result
        self._dataset = dataset
        self.search_queries: list[Query] = []
        self.dataset_ids: list[str] = []

    def search(self, query: Query) -> SearchResult:
        self.search_queries.append(query)
        return self._result

    def get_dataset(self, dataset_id: str) -> Dataset:
        self.dataset_ids.append(dataset_id)
        return self._dataset


class _Session:
    def __init__(self, connector: _Connector) -> None:
        self._connector = connector
        self._transport = object()
        self.plugins = object()
        self.portal_calls: list[tuple[str, str | None]] = []

    def portal(self, url: str, portal_type: str | None = None) -> _Connector:
        self.portal_calls.append((url, portal_type))
        return self._connector


def _resource(resource_id: str, url: str = "https://data.example.test/observations.csv") -> Resource:
    return Resource(id=resource_id, url=url, format="CSV", access=HttpDownload(url=url))


def _facade(*resources: Resource) -> tuple[Any, _Session, _Connector, SearchResult]:
    result = SearchResult(datasets=[])
    connector = _Connector(result, Dataset(id="weather", resources=list(resources)))
    session = _Session(connector)
    return DataSluice(session=session, reader=object()), session, connector, result


def test_one_shot_and_portal_search_share_connector_agnostic_service() -> None:
    """Portal-bound and one-shot searches return the exact same domain result."""
    data_sluice, session, connector, result = _facade(_resource("observations"))
    query = Query(text="rain", limit=3)

    assert data_sluice.search("https://catalog.example.test", query) is result
    portal = data_sluice.portal("https://catalog.example.test")
    assert portal.search(query) is result

    assert connector.search_queries == [query, query]
    assert session.portal_calls == [
        ("https://catalog.example.test", None),
        ("https://catalog.example.test", None),
    ]
    assert "connector" not in portal.__dict__
    assert not hasattr(portal, "_session")


def test_search_service_preserves_explicit_portal_type_and_query_identity() -> None:
    """The dependency-explicit search service uses the supplied session unchanged."""
    data_sluice, session, connector, result = _facade(_resource("observations"))
    query = Query(text="rain")

    assert search_datasets(session, "https://catalog.example.test", query, portal_type="ckan") is result

    assert connector.search_queries == [query]
    assert session.portal_calls == [("https://catalog.example.test", "ckan")]
    data_sluice.close()


def test_detect_returns_the_injected_detector_result_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    """Detection is a dependency-explicit operation with no facade-specific result mapping."""
    data_sluice, session, _connector, _result = _facade(_resource("observations"))
    detected = DetectionResult(portal_type="ckan", confidence=1.0, evidence=[])
    calls: list[tuple[str, object, object]] = []

    def _detect(url: str, *, transport: object, plugin_manager: object) -> DetectionResult:
        calls.append((url, transport, plugin_manager))
        return detected

    monkeypatch.setattr("datasluice.discovery.detect", _detect)

    assert (
        detect_portal("https://catalog.example.test", transport=session._transport, plugin_manager=session.plugins)
        is detected
    )
    assert data_sluice.detect("https://catalog.example.test") is detected
    assert calls == [
        ("https://catalog.example.test", session._transport, session.plugins),
        ("https://catalog.example.test", session._transport, session.plugins),
    ]


def test_catalog_resolution_requires_one_exact_resource_and_sanitizes_selectors() -> None:
    """Missing and duplicate resource selectors are actionable without leaking URI secrets."""
    secret_id = "https://data.example.test/entry.csv?token=must-not-leak"
    data_sluice, _session, connector, _result = _facade(
        _resource(secret_id),
        _resource("observations", "https://data.example.test/observations.csv?api_key=secret"),
    )

    with pytest.raises(ResourceResolutionError) as missing:
        data_sluice.resolve(
            CatalogResourceLocator(
                portal_url="https://catalog.example.test",
                dataset_id="weather",
                resource_id="absent",
            )
        )

    message = str(missing.value)
    assert "absent" in message
    assert "observations" in message
    assert "must-not-leak" not in message

    connector._dataset = Dataset(id="weather", resources=[_resource("duplicate"), _resource("duplicate")])
    with pytest.raises(ResourceResolutionError, match="ambiguous"):
        data_sluice.resolve(
            CatalogResourceLocator(
                portal_url="https://catalog.example.test",
                dataset_id="weather",
                resource_id="duplicate",
            )
        )
