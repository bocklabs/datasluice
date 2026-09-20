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
from datasluice.connectors.catalog.udata.services.resources import AsyncResourcesService, SyncResourcesService
from datasluice.connectors.catalog.udata.wire import resources as wire
from datasluice.contracts.catalog.native.udata import AsyncUDataResourcesService, SyncUDataResourcesService
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform, ResourceKind
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.errors.catalog import CatalogValidationError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse


class _Router:
    def __init__(self, routes: dict[tuple[str, str], object]) -> None:
        self.routes = routes
        self.requests: list[RuntimeRequest] = []

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        value = self.routes[(request.method, request.url)]
        if isinstance(value, BaseException):
            raise value
        if isinstance(value, RuntimeResponse):
            return value
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
        if isinstance(value, RuntimeResponse):
            return value
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


def _policy(target: str, *, destructive: bool = False, operation: str = wire.RESOURCE_OPERATION) -> MutationPolicy:
    return MutationPolicy(
        destructive=destructive,
        confirmation=ConfirmationPolicy(confirmed=True, operation=operation, target=target),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


_CREDENTIAL = UDataCredential(api_key="secret-key")
_PERMISSIONS = EffectivePermissions.for_credential(_CREDENTIAL, platform=CatalogPlatform.UDATA)


def test_resource_service_methods_have_sync_async_parity() -> None:
    def public_methods(service: type[SyncResourcesService] | type[AsyncResourcesService]) -> set[str]:
        return {name for name in dir(service) if not name.startswith("_") and callable(getattr(service, name))}

    expected = {
        "redirect",
        "create",
        "reorder",
        "upload",
        "upload_community",
        "reupload_community",
        "get",
        "update",
        "delete",
        "list_community",
        "create_community",
        "get_community",
        "update_community",
        "delete_community",
        "resource_types",
        "get_dataset_v2",
        "list_v2",
        "get_v2",
        "get_extras_v2",
        "update_extras_v2",
        "delete_extras_v2",
    }
    assert public_methods(SyncResourcesService) == public_methods(AsyncResourcesService) == expected


def test_clients_expose_typed_native_resource_protocols() -> None:
    sync = SyncUDataClient(
        _Router(_routes({})),
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=_CREDENTIAL,
        owns_transport=False,
    )
    async_client = AsyncUDataClient(
        _AsyncRouter(_routes({})),
        declared_udata_profile(),
        origin="http://127.0.0.1:5640",
        credentials=_CREDENTIAL,
        owns_transport=False,
    )

    assert isinstance(sync.resources, SyncUDataResourcesService)
    assert isinstance(async_client.resources, AsyncUDataResourcesService)
    sync.close()


def test_resource_page_and_type_decoders_keep_typed_data_and_pagination() -> None:
    page = wire.parse_resource_page(
        {
            "data": [{"id": "resource", "title": "Sample"}],
            "page": 1,
            "page_size": 10,
            "previous_page": None,
            "next_page": "https://example.test/page/2",
            "total": 11,
        }
    )

    assert page.items[0].id.value == "resource"
    assert page.native_page.total == 11
    assert page.page is not None and page.page.next_cursor == "2"
    assert wire.parse_resource_types([{"id": "file", "label": "File"}]) == ({"id": "file", "label": "File"},)

    with pytest.raises(CatalogValidationError, match="resource type"):
        wire.parse_resource_types([{"id": "file"}])


def test_v2_dataset_decoder_preserves_resource_link_instead_of_requiring_embedded_list() -> None:
    resources = {"href": "https://example.test/resources", "rel": "related", "type": "application/json", "total": 1}
    record = wire.parse_v2_dataset({"id": "dataset", "title": "Dataset", "slug": "dataset", "resources": resources})

    assert record.id.value == "dataset"
    assert record.payload["resources"] == resources


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
        {},
    )
    assert wire.upload_resource_request("dataset", "resource") == (
        "POST",
        "/api/1/datasets/dataset/resources/resource/upload/",
        {},
    )


def test_all_22_assigned_resource_routes_have_independent_exact_verbs_and_paths() -> None:
    create = ResourceCreateInput(title="Remote", url="https://example.test/data.csv")
    update = ResourceUpdateInput({"id": "resource", "title": "Updated"})
    actual = [
        wire.redirect_resource_request("resource")[:2],
        wire.create_resource_request("dataset", create)[:2],
        wire.update_resources_request("dataset", (update,))[:2],
        wire.upload_resource_request("dataset", None)[:2],
        wire.upload_resource_request("dataset", None, community=True)[:2],
        wire.upload_resource_request("dataset", "resource")[:2],
        wire.reupload_community_request("resource")[:2],
        wire.resource_request("GET", "dataset", "resource")[:2],
        wire.resource_request("PUT", "dataset", "resource", body=update.payload())[:2],
        wire.resource_request("DELETE", "dataset", "resource")[:2],
        wire.list_community_resources_request()[:2],
        wire.community_collection_request("POST", create.payload() | {"dataset": "dataset"})[:2],
        wire.resource_request("GET", "", "resource", community=True)[:2],
        wire.resource_request("PUT", "", "resource", community=True, body=update.payload())[:2],
        wire.resource_request("DELETE", "", "resource", community=True)[:2],
        wire.resource_types_request()[:2],
        wire.v2_dataset_request("dataset")[:2],
        wire.v2_resource_request("dataset")[:2],
        wire.v2_resource_request("", "resource")[:2],
        wire.v2_extras_request("GET", "dataset", "resource")[:2],
        wire.v2_extras_request("PUT", "dataset", "resource", {"key": "value"})[:2],
        wire.v2_extras_request("DELETE", "dataset", "resource", ["key"])[:2],
    ]
    assert actual == [
        ("GET", "/api/1/datasets/r/resource"),
        ("POST", "/api/1/datasets/dataset/resources/"),
        ("PUT", "/api/1/datasets/dataset/resources/"),
        ("POST", "/api/1/datasets/dataset/upload/"),
        ("POST", "/api/1/datasets/dataset/upload/community/"),
        ("POST", "/api/1/datasets/dataset/resources/resource/upload/"),
        ("POST", "/api/1/datasets/community_resources/resource/upload/"),
        ("GET", "/api/1/datasets/dataset/resources/resource/"),
        ("PUT", "/api/1/datasets/dataset/resources/resource/"),
        ("DELETE", "/api/1/datasets/dataset/resources/resource/"),
        ("GET", "/api/1/datasets/community_resources/"),
        ("POST", "/api/1/datasets/community_resources/"),
        ("GET", "/api/1/datasets/community_resources/resource/"),
        ("PUT", "/api/1/datasets/community_resources/resource/"),
        ("DELETE", "/api/1/datasets/community_resources/resource/"),
        ("GET", "/api/1/datasets/resource_types/"),
        ("GET", "/api/2/datasets/dataset/"),
        ("GET", "/api/2/datasets/dataset/resources/"),
        ("GET", "/api/2/datasets/resources/resource/"),
        ("GET", "/api/2/datasets/dataset/resources/resource/extras/"),
        ("PUT", "/api/2/datasets/dataset/resources/resource/extras/"),
        ("DELETE", "/api/2/datasets/dataset/resources/resource/extras/"),
    ]

    with pytest.raises(CatalogValidationError):
        wire.resource_request("GET", "..", "resource")


@pytest.mark.parametrize(
    ("name", "args", "path", "payload"),
    [
        (
            "redirect",
            ("resource",),
            "/api/1/datasets/r/resource",
            RuntimeResponse(302, {"Location": "https://example.test/resource"}, b""),
        ),
        ("get", ("dataset", "resource"), "/api/1/datasets/dataset/resources/resource/", {"id": "resource"}),
        (
            "list_community",
            (),
            "/api/1/datasets/community_resources/",
            {"data": [{"id": "resource"}], "page": 1, "page_size": 1, "total": 1},
        ),
        ("get_community", ("resource",), "/api/1/datasets/community_resources/resource/", {"id": "resource"}),
        ("resource_types", (), "/api/1/datasets/resource_types/", [{"id": "file", "label": "File"}]),
        (
            "get_dataset_v2",
            ("dataset",),
            "/api/2/datasets/dataset/",
            {"id": "dataset", "title": "Dataset", "slug": "dataset"},
        ),
        (
            "list_v2",
            ("dataset",),
            "/api/2/datasets/dataset/resources/",
            {"data": [{"id": "resource"}], "page": 1, "page_size": 1, "total": 1},
        ),
        (
            "get_v2",
            ("resource",),
            "/api/2/datasets/resources/resource/",
            {"resource": {"id": "resource"}, "dataset_id": "dataset"},
        ),
        (
            "get_extras_v2",
            ("dataset", "resource"),
            "/api/2/datasets/dataset/resources/resource/extras/",
            {"key": "value"},
        ),
    ],
)
def test_read_routes_dispatch_with_sync_async_parity(
    name: str, args: tuple[object, ...], path: str, payload: object
) -> None:
    origin = "http://127.0.0.1:5640"
    sync_transport = _Router(_routes({("GET", origin + path): payload}))
    async_transport = _AsyncRouter(_routes({("GET", origin + path): payload}))
    sync_client = SyncUDataClient(
        sync_transport, declared_udata_profile(), origin=origin, credentials=_CREDENTIAL, owns_transport=False
    )
    async_client = AsyncUDataClient(
        async_transport, declared_udata_profile(), origin=origin, credentials=_CREDENTIAL, owns_transport=False
    )

    with sync_client:
        sync_result = getattr(sync_client.resources, name)(*args)

    async def run() -> object:
        async with async_client:
            return await getattr(async_client.resources, name)(*args)

    async_result = asyncio.run(run())
    assert sync_transport.requests[-1].url == async_transport.requests[-1].url == origin + path
    assert sync_result == async_result


@pytest.mark.parametrize(
    ("call", "verb", "path", "response"),
    [
        pytest.param(
            lambda s: s.create(
                "dataset",
                ResourceCreateInput("Remote", "https://example.test/data.csv"),
                _PERMISSIONS,
                _policy("dataset", operation=wire.CREATE_OPERATION),
            ),
            "POST",
            "/api/1/datasets/dataset/resources/",
            (201, {"id": "resource"}),
            id="create_resource",
        ),
        pytest.param(
            lambda s: s.reorder(
                "dataset",
                (ResourceUpdateInput({"id": "resource"}),),
                _PERMISSIONS,
                _policy("dataset", operation=wire.REORDER_OPERATION),
            ),
            "PUT",
            "/api/1/datasets/dataset/resources/",
            [{"id": "resource"}],
            id="update_resources",
        ),
        pytest.param(
            lambda s: s.upload(
                "dataset",
                ResourceUploadInput(BytesIO(b"x"), "data.csv", 1),
                _PERMISSIONS,
                _policy("dataset", operation=wire.UPLOAD_NEW_OPERATION),
            ),
            "POST",
            "/api/1/datasets/dataset/upload/",
            (201, {"id": "resource"}),
            id="upload_new_dataset_resource",
        ),
        pytest.param(
            lambda s: s.upload_community(
                "dataset",
                ResourceUploadInput(BytesIO(b"x"), "data.csv", 1),
                _PERMISSIONS,
                _policy("dataset", operation=wire.UPLOAD_NEW_OPERATION),
            ),
            "POST",
            "/api/1/datasets/dataset/upload/community/",
            (201, {"id": "resource"}),
            id="upload_new_community_resource",
        ),
        pytest.param(
            lambda s: s.upload(
                "dataset",
                ResourceUploadInput(BytesIO(b"x"), "data.csv", 1),
                _PERMISSIONS,
                _policy("resource", destructive=True, operation=wire.UPLOAD_REPLACE_OPERATION),
                resource_id="resource",
            ),
            "POST",
            "/api/1/datasets/dataset/resources/resource/upload/",
            {"id": "resource"},
            id="upload_dataset_resource",
        ),
        pytest.param(
            lambda s: s.reupload_community(
                "resource",
                ResourceUploadInput(BytesIO(b"x"), "data.csv", 1),
                _PERMISSIONS,
                _policy("resource", destructive=True, operation=wire.UPLOAD_REPLACE_OPERATION),
            ),
            "POST",
            "/api/1/datasets/community_resources/resource/upload/",
            {"id": "resource"},
            id="upload_community_resource",
        ),
        pytest.param(
            lambda s: s.update(
                "dataset",
                "resource",
                ResourceUpdateInput({"title": "Updated"}),
                _PERMISSIONS,
                _policy("resource", operation=wire.RESOURCE_UPDATE_OPERATION),
            ),
            "PUT",
            "/api/1/datasets/dataset/resources/resource/",
            {"id": "resource"},
            id="update_resource",
        ),
        pytest.param(
            lambda s: s.delete(
                "dataset",
                "resource",
                _PERMISSIONS,
                _policy("resource", destructive=True, operation=wire.RESOURCE_DELETE_OPERATION),
            ),
            "DELETE",
            "/api/1/datasets/dataset/resources/resource/",
            (204, None),
            id="delete_resource",
        ),
        pytest.param(
            lambda s: s.create_community(
                "dataset",
                ResourceCreateInput("Remote", "https://example.test/data.csv"),
                _PERMISSIONS,
                _policy("dataset", operation=wire.COMMUNITY_CREATE_OPERATION),
            ),
            "POST",
            "/api/1/datasets/community_resources/",
            (201, {"id": "resource"}),
            id="create_community_resource",
        ),
        pytest.param(
            lambda s: s.update_community(
                "resource",
                ResourceUpdateInput({"title": "Updated"}),
                _PERMISSIONS,
                _policy("resource", operation=wire.COMMUNITY_UPDATE_OPERATION),
            ),
            "PUT",
            "/api/1/datasets/community_resources/resource/",
            {"id": "resource"},
            id="update_community_resource",
        ),
        pytest.param(
            lambda s: s.delete_community(
                "resource",
                _PERMISSIONS,
                _policy("resource", destructive=True, operation=wire.COMMUNITY_DELETE_OPERATION),
            ),
            "DELETE",
            "/api/1/datasets/community_resources/resource/",
            (204, None),
            id="delete_community_resource",
        ),
        pytest.param(
            lambda s: s.update_extras_v2(
                "dataset",
                "resource",
                {"key": "value"},
                _PERMISSIONS,
                _policy("resource", operation=wire.EXTRAS_UPDATE_OPERATION),
            ),
            "PUT",
            "/api/2/datasets/dataset/resources/resource/extras/",
            {"key": "value"},
            id="update_resource_extras",
        ),
        pytest.param(
            lambda s: s.delete_extras_v2(
                "dataset",
                "resource",
                ("key",),
                _PERMISSIONS,
                _policy("resource", destructive=True, operation=wire.EXTRAS_DELETE_OPERATION),
            ),
            "DELETE",
            "/api/2/datasets/dataset/resources/resource/extras/",
            (204, None),
            id="delete_resource_extras",
        ),
    ],
)
def test_mutation_routes_dispatch_with_sync_async_parity(call, verb: str, path: str, response: object) -> None:
    origin = "http://127.0.0.1:5640"
    sync_transport = _Router(_routes({(verb, origin + path): response}))
    async_transport = _AsyncRouter(_routes({(verb, origin + path): response}))
    sync_client = SyncUDataClient(
        sync_transport, declared_udata_profile(), origin=origin, credentials=_CREDENTIAL, owns_transport=False
    )
    async_client = AsyncUDataClient(
        async_transport, declared_udata_profile(), origin=origin, credentials=_CREDENTIAL, owns_transport=False
    )

    with sync_client:
        sync_result = call(sync_client.resources)

    async def run() -> object:
        async with async_client:
            return await call(async_client.resources)

    async_result = asyncio.run(run())
    assert sync_transport.requests[-1].url == async_transport.requests[-1].url == origin + path
    assert sync_transport.requests[-1].method == async_transport.requests[-1].method == verb
    assert sync_result == async_result


def test_upload_source_is_bounded_streamed_once_and_closed() -> None:
    source = BytesIO(b"abc")
    upload = ResourceUploadInput(source=source, file_name="data.csv", max_upload_bytes=3)

    part = upload.part()

    assert part.field_name == "file"
    assert part.file_name == "data.csv"
    assert not isinstance(part.data, bytes)
    assert part.data.read() == b"abc"
    with pytest.raises(ValueError, match="cannot be reused"):
        upload.part()
    upload.close()
    assert source.closed


def test_resource_inputs_do_not_render_upload_or_configuration_values() -> None:
    marker = "sensitive-resource-value"
    create = ResourceCreateInput(title="Remote", url=f"https://example.test/{marker}", fields={"note": marker})
    update = ResourceUpdateInput({"note": marker})
    upload = ResourceUploadInput(source=BytesIO(b"data"), file_name=marker, max_upload_bytes=4)

    assert marker not in repr(create)
    assert marker not in repr(update)
    assert marker not in repr(upload)
    upload.close()


def test_upload_rejects_a_source_larger_than_its_byte_ceiling() -> None:
    upload = ResourceUploadInput(source=BytesIO(b"abcd"), file_name="data.csv", max_upload_bytes=3)

    with pytest.raises(ValueError, match="byte limit"):
        part = upload.part()
        assert not isinstance(part.data, bytes)
        part.data.read()


def test_interrupted_upload_closes_source_and_attaches_cancelled_receipt() -> None:
    origin = "http://127.0.0.1:5640"
    upload_url = f"{origin}/api/1/datasets/dataset/upload/"
    transport = _Router(_routes({("POST", upload_url): KeyboardInterrupt()}))
    client = SyncUDataClient(
        transport, declared_udata_profile(), origin=origin, credentials=_CREDENTIAL, owns_transport=False
    )
    source = BytesIO(b"abc")

    with client, pytest.raises(KeyboardInterrupt) as raised:
        client.resources.upload(
            "dataset",
            ResourceUploadInput(source=source, file_name="data.csv", max_upload_bytes=3),
            _PERMISSIONS,
            _policy("dataset", operation=wire.UPLOAD_NEW_OPERATION),
        )

    assert source.closed
    assert raised.value.__dict__["mutation_receipt"].outcome == "cancelled"


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
            _policy("dataset", operation=wire.CREATE_OPERATION),
        )
        uploaded = client.resources.upload(
            "dataset",
            ResourceUploadInput(source=source, file_name="data.csv", max_upload_bytes=3),
            _PERMISSIONS,
            _policy("resource", destructive=True, operation=wire.UPLOAD_REPLACE_OPERATION),
            resource_id="resource",
        )

    assert created.receipt.outcome == uploaded.receipt.outcome == "succeeded"
    assert created.receipt.target.resource_kind is ResourceKind.DATASET
    assert uploaded.receipt.target.resource_kind is ResourceKind.RESOURCE
    assert "https://example.test/data.csv" not in repr(created)
    assert json.loads(transport.requests[1].body or b"{}") == {
        "title": "Remote",
        "url": "https://example.test/data.csv",
        "filetype": "remote",
        "type": "other",
    }
    assert transport.requests[2].files[0].field_name == "file"
    assert source.closed


def test_reorder_and_v2_extras_keep_typed_results_and_exact_json_bodies() -> None:
    origin = "http://127.0.0.1:5640"
    reorder_url = f"{origin}/api/1/datasets/dataset/resources/"
    extras_url = f"{origin}/api/2/datasets/dataset/resources/resource/extras/"
    transport = _Router(
        _routes(
            {
                ("PUT", reorder_url): [{"id": "resource"}],
                ("PUT", extras_url): {"key": "value"},
                ("DELETE", extras_url): (204, None),
            }
        )
    )
    client = SyncUDataClient(
        transport, declared_udata_profile(), origin=origin, credentials=_CREDENTIAL, owns_transport=False
    )

    with client:
        reordered = client.resources.reorder(
            "dataset",
            (ResourceUpdateInput({"id": "resource"}),),
            _PERMISSIONS,
            _policy("dataset", operation=wire.REORDER_OPERATION),
        )
        updated = client.resources.update_extras_v2(
            "dataset",
            "resource",
            {"key": "value"},
            _PERMISSIONS,
            _policy("resource", operation=wire.EXTRAS_UPDATE_OPERATION),
        )
        deleted = client.resources.delete_extras_v2(
            "dataset",
            "resource",
            ("key",),
            _PERMISSIONS,
            _policy("resource", destructive=True, operation=wire.EXTRAS_DELETE_OPERATION),
        )

    assert reordered.records[0].id.value == "resource"
    assert updated.extras == {"key": "value"}
    assert deleted.receipt.outcome == "succeeded"
    assert json.loads(transport.requests[1].body or b"null") == [{"id": "resource"}]
    assert json.loads(transport.requests[2].body or b"null") == {"key": "value"}
    assert json.loads(transport.requests[3].body or b"null") == ["key"]


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
                    _policy("dataset", operation=wire.CREATE_OPERATION),
                )
            ).receipt.outcome

    assert asyncio.run(run()) == "succeeded"
    assert transport.requests[-1].url.endswith("/api/1/datasets/dataset/resources/")
