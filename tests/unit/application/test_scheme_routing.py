"""Tests for DirectResourceLocator scheme routing in _resolve_direct_resource."""

from __future__ import annotations

from datasluice.application import DirectResourceLocator, _resolve_direct_resource
from datasluice.domain import HttpDownload, LocalFile, ObjectStorage


def test_file_scheme_routes_to_local_file() -> None:
    locator = DirectResourceLocator(uri="file:///tmp/data.csv")
    resource = _resolve_direct_resource(locator)
    assert isinstance(resource.access, LocalFile)


def test_https_scheme_routes_to_http_download() -> None:
    locator = DirectResourceLocator(uri="https://example.com/data.csv")
    resource = _resolve_direct_resource(locator)
    assert isinstance(resource.access, HttpDownload)


def test_s3_scheme_routes_to_object_storage() -> None:
    locator = DirectResourceLocator(uri="s3://bucket/key.csv")
    resource = _resolve_direct_resource(locator)
    assert isinstance(resource.access, ObjectStorage)


def test_gs_scheme_routes_to_object_storage() -> None:
    locator = DirectResourceLocator(uri="gs://bucket/key.csv")
    resource = _resolve_direct_resource(locator)
    assert isinstance(resource.access, ObjectStorage)


def test_az_scheme_routes_to_object_storage() -> None:
    locator = DirectResourceLocator(uri="az://container/blob.csv")
    resource = _resolve_direct_resource(locator)
    assert isinstance(resource.access, ObjectStorage)


def test_bare_path_routes_to_local_file() -> None:
    locator = DirectResourceLocator(uri="/tmp/local.csv")
    resource = _resolve_direct_resource(locator)
    assert isinstance(resource.access, LocalFile)
