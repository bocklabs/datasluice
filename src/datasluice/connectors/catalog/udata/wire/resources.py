"""Exact request builders and bounded resource decoders for uData 17.6."""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast
from urllib.parse import quote, urlencode

from datasluice.connectors.catalog.udata.models.resources import ResourceCreateInput, ResourceUpdateInput
from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.models import NativeRecord, _freeze_json
from datasluice.errors.catalog import CatalogValidationError

RESOURCE_OPERATION = "udata/api-v1.dataset-resource-create-update-reorder-upload-delete"


def _id(value: object, name: str) -> str:
    if not isinstance(value, str) or not value or any(character in value for character in "/?#"):
        raise CatalogValidationError(
            f"uData resource {name} must be one non-empty path segment.",
            operation=RESOURCE_OPERATION,
            platform="udata",
            safe_action="Pass the raw uData identifier without separators.",
        )
    return quote(value, safe="")


def _request(method: str, path: str, body: object | None = None) -> tuple[str, str, dict[str, str], object | None]:
    return method, path, {}, body


def create_resource_request(
    dataset_id: str, client_input: ResourceCreateInput
) -> tuple[str, str, dict[str, str], object]:
    return _request("POST", f"/api/1/datasets/{_id(dataset_id, 'dataset id')}/resources/", client_input.payload())


def update_resources_request(
    dataset_id: str, values: tuple[ResourceUpdateInput, ...]
) -> tuple[str, str, dict[str, str], object]:
    if not values:
        raise ValueError("uData resource reorder requires at least one resource.")
    return _request(
        "PUT", f"/api/1/datasets/{_id(dataset_id, 'dataset id')}/resources/", [item.payload() for item in values]
    )


def upload_resource_request(
    dataset_id: str, resource_id: str | None, *, community: bool = False
) -> tuple[str, str, dict[str, str]]:
    dataset = _id(dataset_id, "dataset id")
    if community:
        path = f"/api/1/datasets/{dataset}/upload/community/"
    elif resource_id is None:
        path = f"/api/1/datasets/{dataset}/upload/"
    else:
        path = f"/api/1/datasets/{dataset}/resources/{_id(resource_id, 'resource id')}/upload/"
    return "POST", path, {"Content-Type": "multipart/form-data"}


def reupload_community_request(resource_id: str) -> tuple[str, str, dict[str, str]]:
    return (
        "POST",
        f"/api/1/datasets/community_resources/{_id(resource_id, 'community resource id')}/upload/",
        {"Content-Type": "multipart/form-data"},
    )


def v2_extras_request(
    method: str, dataset_id: str, resource_id: str, body: object = None
) -> tuple[str, str, dict[str, str], object | None]:
    return _request(
        method,
        f"/api/2/datasets/{_id(dataset_id, 'dataset id')}/resources/{_id(resource_id, 'resource id')}/extras/",
        body,
    )


def resource_request(
    method: str, dataset_id: str, resource_id: str, *, community: bool = False, body: object = None
) -> tuple[str, str, dict[str, str], object | None]:
    if community:
        path = f"/api/1/datasets/community_resources/{_id(resource_id, 'community resource id')}/"
    else:
        path = f"/api/1/datasets/{_id(dataset_id, 'dataset id')}/resources/{_id(resource_id, 'resource id')}/"
    return _request(method, path, body)


def list_community_resources_request(
    params: Mapping[str, str | int] | None = None,
) -> tuple[str, str, dict[str, str], object | None]:
    pairs = [] if params is None else [(key, str(value)) for key, value in sorted(params.items())]
    query = urlencode(pairs)
    return _request("GET", "/api/1/datasets/community_resources/" + (f"?{query}" if query else ""))


def community_collection_request(method: str, body: object = None) -> tuple[str, str, dict[str, str], object | None]:
    return _request(method, "/api/1/datasets/community_resources/", body)


def resource_types_request() -> tuple[str, str, dict[str, str], object | None]:
    return _request("GET", "/api/1/datasets/resource_types/")


def v2_resource_request(
    dataset_id: str, resource_id: str | None = None, *, extras: str | None = None
) -> tuple[str, str, dict[str, str], object | None]:
    if resource_id is None:
        path = f"/api/2/datasets/{_id(dataset_id, 'dataset id')}/resources/"
    elif dataset_id == "":
        path = f"/api/2/datasets/resources/{_id(resource_id, 'resource id')}/"
    else:
        path = f"/api/2/datasets/{_id(dataset_id, 'dataset id')}/resources/{_id(resource_id, 'resource id')}"
        path += "/extras/" if extras is not None else "/"
    return _request("GET" if extras is None else extras, path)


def parse_resource(payload: object) -> NativeRecord:
    if not isinstance(payload, Mapping) or not isinstance(payload.get("id"), str) or not payload["id"]:
        raise CatalogValidationError(
            "The uData resource response must contain a non-empty id.",
            operation=RESOURCE_OPERATION,
            platform="udata",
            safe_action="Verify the response against the pinned uData resource schema.",
        )
    identifier = cast(str, payload["id"])
    return NativeRecord(
        platform=CatalogPlatform.UDATA,
        resource_kind=ResourceKind.RESOURCE,
        id=CatalogId(platform=CatalogPlatform.UDATA, resource_kind=ResourceKind.RESOURCE, value=identifier),
        payload=cast(Mapping[str, object], _freeze_json(dict(payload), "udata.resource")),
    )
