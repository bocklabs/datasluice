from __future__ import annotations

import pytest

from datasluice.application import DirectResourceLocator, _resolve_direct_resource
from datasluice.domain import HttpDownload, LocalFile, ObjectStorage
from datasluice.exceptions import DataSluiceError


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


def test_unknown_scheme_is_rejected() -> None:
    with pytest.raises(DataSluiceError):
        DirectResourceLocator(uri="ftp://example.test/data.csv")


def test_windows_drive_path_routes_to_local_file() -> None:
    path = r"C:\data\source.csv"
    resource = _resolve_direct_resource(DirectResourceLocator(uri=path))
    assert isinstance(resource.access, LocalFile)
    assert resource.access.path == path
