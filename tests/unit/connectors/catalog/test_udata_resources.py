"""Exact wire and streaming resource contract tests."""

from __future__ import annotations

from io import BytesIO

import pytest

from datasluice.connectors.catalog.udata.models.resources import ResourceCreateInput, ResourceUploadInput
from datasluice.connectors.catalog.udata.wire import resources as wire


def test_remote_resource_and_upload_routes_have_distinct_exact_shapes() -> None:
    create = ResourceCreateInput(title="Remote", url="https://example.test/data.csv", filetype="remote")

    assert wire.create_resource_request("dataset", create) == (
        "POST",
        "/api/1/datasets/dataset/resources/",
        {},
        {"title": "Remote", "url": "https://example.test/data.csv", "filetype": "remote", "type": "other"},
    )
    assert wire.upload_resource_request("dataset", None) == (
        "POST",
        "/api/1/datasets/dataset/upload/",
        {"Content-Type": "multipart/form-data"},
    )
    assert wire.upload_resource_request("dataset", "resource") == (
        "POST",
        "/api/1/datasets/dataset/resources/resource/upload/",
        {"Content-Type": "multipart/form-data"},
    )


def test_upload_source_is_bounded_streamed_once_and_closed() -> None:
    source = BytesIO(b"abc")
    upload = ResourceUploadInput(source=source, file_name="data.csv", max_upload_bytes=3)

    part = upload.part()

    assert part.field_name == "file"
    assert part.file_name == "data.csv"
    assert part.data is source
    upload.close()
    assert source.closed


def test_upload_rejects_a_source_larger_than_its_byte_ceiling() -> None:
    upload = ResourceUploadInput(source=BytesIO(b"abcd"), file_name="data.csv", max_upload_bytes=3)

    with pytest.raises(ValueError, match="byte limit"):
        upload.part().data.read()
