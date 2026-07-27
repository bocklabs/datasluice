"""Unit tests for CKAN/data.gouv/Socrata resource.access + resource.schema population (D-P5-02/03/04).

Covers the three access paths (HttpDownload default, QueryAccess on datastore,
None when portal silent) and the two schema paths (datastore_fields first,
schema.fields fallback, None when silent). Existing
``test_ckan_mapper.py`` assertions are kept backward-compatible via the
optional ``base_url`` kwarg.
"""

from __future__ import annotations

from datasluice.connectors.ckan.mapper import map_resource as ckan_map_resource
from datasluice.connectors.datagouv.mapper import map_resource as datagouv_map_resource
from datasluice.domain import HttpDownload, QueryAccess


def test_access_httpdownload_when_url_set() -> None:
    resource = ckan_map_resource({"id": "r1", "url": "https://example.com/data.csv", "format": "CSV"})
    assert isinstance(resource.access, HttpDownload)
    assert resource.access.url == "https://example.com/data.csv"


def test_access_queryaccess_when_no_url_and_datastore_active() -> None:
    resource = ckan_map_resource(
        {"id": "r1", "url": None, "datastore_active": True, "datastore_fields": [{"id": "col1"}]}
    )
    assert isinstance(resource.access, QueryAccess)
    assert resource.access.query_language == "ckan-datastore"
    assert resource.access.extra["resource_id"] == "r1"


def test_access_none_when_neither_url_nor_datastore() -> None:
    resource = ckan_map_resource({"id": "r1"})
    assert resource.access is None


def test_schema_from_datastore_fields() -> None:
    resource = ckan_map_resource(
        {"id": "r1", "url": None, "datastore_active": True, "datastore_fields": [{"id": "col1"}]}
    )
    assert resource.schema is not None
    assert resource.schema.columns == [{"id": "col1"}]


def test_schema_from_schema_fields_fallback() -> None:
    resource = ckan_map_resource({"id": "r1", "url": "https://x", "schema": {"fields": [{"id": "colA"}]}})
    assert resource.schema is not None
    assert resource.schema.columns == [{"id": "colA"}]


def test_schema_none_when_portal_silent() -> None:
    resource = ckan_map_resource({"id": "r1", "url": "https://x"})
    assert resource.schema is None


def test_access_queryaccess_endpoint_uses_base_url_when_supplied() -> None:
    resource = ckan_map_resource(
        {"id": "r1", "url": None, "datastore_active": True},
        base_url="https://data.gov.uk",
    )
    assert isinstance(resource.access, QueryAccess)
    assert resource.access.endpoint == "https://data.gov.uk/api/3/action/datastore_search"


def test_datagouv_access_httpdownload_when_url_set() -> None:
    resource = datagouv_map_resource({"id": "r1", "url": "https://example.com/data.csv", "format": "CSV"})
    assert isinstance(resource.access, HttpDownload)
    assert resource.access.url == "https://example.com/data.csv"


def test_datagouv_access_none_when_no_url() -> None:
    resource = datagouv_map_resource({"id": "r1"})
    assert resource.access is None


def test_datagouv_schema_from_schema_fields() -> None:
    resource = datagouv_map_resource(
        {"id": "r1", "url": "https://x", "schema": {"fields": [{"name": "col1", "type": "text"}]}}
    )
    assert resource.schema is not None
    assert resource.schema.columns == [{"name": "col1", "type": "text"}]


def test_datagouv_schema_none_when_portal_silent() -> None:
    resource = datagouv_map_resource({"id": "r1", "url": "https://x"})
    assert resource.schema is None
