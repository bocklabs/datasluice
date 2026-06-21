"""Unit tests for domain models."""

from __future__ import annotations

from datasluice.domain import Dataset, License, Organization, Query, Resource, SearchResult


def test_license_defaults() -> None:
    license_ = License(id="CC-BY-4.0")
    assert license_.id == "CC-BY-4.0"
    assert license_.title is None
    assert license_.url is None


def test_resource_normalize_format() -> None:
    assert Resource.normalize_format("text/csv") == "CSV"
    assert Resource.normalize_format("application/json") == "JSON"
    assert Resource.normalize_format("csv") == "CSV"
    assert Resource.normalize_format(None) is None


def test_resource_defaults() -> None:
    resource = Resource(id="abc-123")
    assert resource.id == "abc-123"
    assert resource.url is None
    assert resource.extra == {}


def test_organization_defaults() -> None:
    org = Organization(id="org-1")
    assert org.id == "org-1"
    assert org.extra == {}


def test_dataset_defaults() -> None:
    dataset = Dataset(id="ds-1")
    assert dataset.id == "ds-1"
    assert dataset.resources == []
    assert dataset.tags == []


def test_query_defaults() -> None:
    query = Query()
    assert query.text is None
    assert query.limit == 100
    assert query.offset == 0


def test_search_result_iteration() -> None:
    result = SearchResult(datasets=[Dataset(id="a"), Dataset(id="b")], total=2)
    assert len(result) == 2
    ids = [d.id for d in result]
    assert ids == ["a", "b"]
