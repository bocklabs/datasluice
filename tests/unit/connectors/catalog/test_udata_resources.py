"""Exact wire and streaming resource contract tests."""

from __future__ import annotations

import asyncio
import json
from io import BytesIO

import pytest

from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient, declared_udata_profile
from datasluice.connectors.catalog.udata.models.resources import (
    ResourceCreateInput,
    ResourceUpdateInput,
    ResourceUploadInput,
)
from datasluice.connectors.catalog.udata.wire import resources as wire
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse


class _Router:
    def __init__(self, routes: dict[tuple[str, str], object]) -> None:
        self.routes = routes
        self.requests: list[RuntimeRequest] = []

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        value = self.routes[(request.method, request.url)]
        status, payload = value if isinstance(value, tuple) else (200, value)
        body = b"" if payload is None else json.dumps(payload).encode()
        return RuntimeResponse(status_code=status, headers={"Content-Type": "application/json"}, body=body)

    def close(self) -> None:
        return None


class _AsyncRouter:
    def __init__(self, routes: dict[tuple[str, str], object]) -> None:
        self.routes = routes
        self.requests: list[RuntimeRequest] = []

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        value = self.routes[(request.method, request.url)]
        status, payload = value if isinstance(value, tuple) else (200, value)
        body = b"" if payload is None else json.dumps(payload).encode()
        return RuntimeResponse(status_code=status, headers={"Content-Type": "application/json"}, body=body)

    async def aclose(self) -> None:
        return None


def _routes(routes: dict[tuple[str, str], object]) -> dict[tuple[str, str], object]:
    return {
        ("GET", "http://127.0.0.1:5640/api/1/site/"): {
            "feed_size": 0,
            "id": "site",
            "keywords": [],
            "metrics": {},
            "title": "uData",
            "version": "17.6.0",
        },
        **routes,
    }


def _policy(target: str, *, destructive: bool = False) -> MutationPolicy:
    return MutationPolicy(
        destructive=destructive,
        confirmation=ConfirmationPolicy(confirmed=True, operation=wire.RESOURCE_OPERATION, target=target),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


_CREDENTIAL = UDataCredential(api_key="secret-key")
_PERMISSIONS = EffectivePermissions.for_credential(_CREDENTIAL, platform=CatalogPlatform.UDATA)


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


def test_every_assigned_resource_route_has_an_independent_exact_path() -> None:
    resource = ResourceUpdateInput({"title": "Updated"})

    assert wire.resource_request("GET", "dataset", "resource")[1] == "/api/1/datasets/dataset/resources/resource/"
    assert (
        wire.resource_request("PUT", "dataset", "resource", body=resource.payload())[1]
        == "/api/1/datasets/dataset/resources/resource/"
    )
    assert wire.resource_request("DELETE", "dataset", "resource")[1] == "/api/1/datasets/dataset/resources/resource/"
    assert wire.list_community_resources_request()[1] == "/api/1/datasets/community_resources/"
    assert wire.community_collection_request("POST")[1] == "/api/1/datasets/community_resources/"
    assert (
        wire.resource_request("GET", "", "resource", community=True)[1]
        == "/api/1/datasets/community_resources/resource/"
    )
    assert wire.reupload_community_request("resource")[1] == "/api/1/datasets/community_resources/resource/upload/"
    assert wire.resource_types_request()[1] == "/api/1/datasets/resource_types/"
    assert wire.v2_resource_request("dataset")[1] == "/api/2/datasets/dataset/resources/"
    assert wire.v2_resource_request("", "resource")[1] == "/api/2/datasets/resources/resource/"
    assert (
        wire.v2_extras_request("GET", "dataset", "resource")[1] == "/api/2/datasets/dataset/resources/resource/extras/"
    )


def test_upload_source_is_bounded_streamed_once_and_closed() -> None:
    source = BytesIO(b"abc")
    upload = ResourceUploadInput(source=source, file_name="data.csv", max_upload_bytes=3)

    part = upload.part()

    assert part.field_name == "file"
    assert part.file_name == "data.csv"
    assert not isinstance(part.data, bytes)
    assert part.data.read() == b"abc"
    upload.close()
    assert source.closed


def test_upload_rejects_a_source_larger_than_its_byte_ceiling() -> None:
    upload = ResourceUploadInput(source=BytesIO(b"abcd"), file_name="data.csv", max_upload_bytes=3)

    with pytest.raises(ValueError, match="byte limit"):
        part = upload.part()
        assert not isinstance(part.data, bytes)
        part.data.read()


def test_resource_service_uses_exact_json_and_multipart_routes_with_receipts() -> None:
    origin = "http://127.0.0.1:5640"
    create_url = f"{origin}/api/1/datasets/dataset/resources/"
    upload_url = f"{origin}/api/1/datasets/dataset/resources/resource/upload/"
    transport = _Router(
        _routes({("POST", create_url): (201, {"id": "resource"}), ("POST", upload_url): {"id": "resource"}})
    )
    client = SyncUDataClient(
        transport, declared_udata_profile(), origin=origin, credentials=_CREDENTIAL, owns_transport=False
    )
    source = BytesIO(b"abc")

    with client:
        created = client.resources.create(
            "dataset",
            ResourceCreateInput(title="Remote", url="https://example.test/data.csv"),
            _PERMISSIONS,
            _policy("dataset"),
        )
        uploaded = client.resources.upload(
            "dataset",
            ResourceUploadInput(source=source, file_name="data.csv", max_upload_bytes=3),
            _PERMISSIONS,
            _policy("resource"),
            resource_id="resource",
        )

    assert created.receipt.outcome == uploaded.receipt.outcome == "succeeded"
    assert json.loads(transport.requests[1].body or b"{}") == {
        "title": "Remote",
        "url": "https://example.test/data.csv",
        "filetype": "remote",
        "type": "other",
    }
    assert transport.requests[2].files[0].field_name == "file"
    assert source.closed


def test_async_resource_service_matches_sync_create_route() -> None:
    origin = "http://127.0.0.1:5640"
    transport = _AsyncRouter(
        _routes({("POST", f"{origin}/api/1/datasets/dataset/resources/"): (201, {"id": "resource"})})
    )
    client = AsyncUDataClient(
        transport, declared_udata_profile(), origin=origin, credentials=_CREDENTIAL, owns_transport=False
    )

    async def run() -> str:
        async with client:
            return (
                await client.resources.create(
                    "dataset",
                    ResourceCreateInput(title="Remote", url="https://example.test/data.csv"),
                    _PERMISSIONS,
                    _policy("dataset"),
                )
            ).receipt.outcome

    assert asyncio.run(run()) == "succeeded"
    assert transport.requests[-1].url.endswith("/api/1/datasets/dataset/resources/")
