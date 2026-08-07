"""Unit tests for the CKAN adapter mapper."""

from __future__ import annotations

from datasluice.connectors.ckan.mapper import map_dataset, map_resource


def test_map_resource_basic() -> None:
    raw = {"id": "res-1", "name": "My CSV", "url": "https://example.com/data.csv", "format": "CSV"}
    resource = map_resource(raw)
    assert resource.id == "res-1"
    assert resource.name == "My CSV"
    assert resource.format == "CSV"


def test_map_dataset_basic() -> None:
    raw = {
        "id": "ds-1",
        "name": "test-dataset",
        "title": "Test Dataset",
        "notes": "A test dataset.",
        "resources": [{"id": "r1", "url": "https://example.com/r.csv", "format": "csv"}],
        "organization": {"id": "org-1", "name": "my-org"},
        "tags": [{"name": "climate"}, {"name": "weather"}],
    }
    dataset = map_dataset(raw)
    assert dataset.id == "ds-1"
    assert dataset.title == "Test Dataset"
    assert dataset.description == "A test dataset."
    assert len(dataset.resources) == 1
    assert dataset.resources[0].format == "CSV"
    assert dataset.organization is not None
    assert dataset.organization.name == "my-org"
    assert "climate" in dataset.tags
    assert "weather" in dataset.tags


def test_map_dataset_handles_null_resources_tags_groups() -> None:
    raw = {
        "id": "ds-null",
        "name": "nulled",
        "title": "Nulled",
        "resources": None,
        "tags": None,
        "groups": None,
        "license_id": None,
        "license_title": None,
        "license_url": None,
    }
    dataset = map_dataset(raw)
    assert dataset.resources == []
    assert dataset.tags == []
    assert dataset.themes == []


def test_map_resource_coerces_size_to_int() -> None:
    raw = {"id": "r1", "url": "https://example.com/r.csv", "size": "12345"}
    resource = map_resource(raw)
    assert resource.size == 12345


def test_map_organization_id_fallback_on_null() -> None:
    from datasluice.connectors.ckan.mapper import map_organization

    org = map_organization({"id": None, "name": "fallback-org"})
    assert org is not None
    assert org.id == "fallback-org"
