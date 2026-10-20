"""Exact request builders and bounded resource decoders for uData 17.6."""

from __future__ import annotations

import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, cast
from urllib.parse import urlencode

from datasluice.connectors.catalog.udata.mapping import UDataPageEnvelope, parse_native_page, shape_native_page
from datasluice.connectors.catalog.udata.models._segment import path_segment
from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.models import NativeRecord, _freeze_json
from datasluice.errors.catalog import CatalogValidationError

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.models.resources import ResourceCreateInput, ResourceUpdateInput

RESOURCE_OPERATION = "udata/api-v1.dataset-resource-create-update-reorder-upload-delete"
RESOURCE_MUTATION_CAPABILITY = "udata/api-v1.resource-mutations"
RESOURCE_DELETE_CAPABILITY = "udata/api-v1.resource-destructive-mutations"

CREATE_OPERATION = f"{RESOURCE_OPERATION}-create"
REORDER_OPERATION = f"{RESOURCE_OPERATION}-reorder"
UPLOAD_NEW_OPERATION = f"{RESOURCE_OPERATION}-upload-new"
UPLOAD_REPLACE_OPERATION = f"{RESOURCE_OPERATION}-upload-replace"
UPLOAD_COMMUNITY_NEW_OPERATION = f"{RESOURCE_OPERATION}-upload-community-new"
UPLOAD_COMMUNITY_REPLACE_OPERATION = f"{RESOURCE_OPERATION}-upload-community-replace"
RESOURCE_UPDATE_OPERATION = f"{RESOURCE_OPERATION}-update"
RESOURCE_DELETE_OPERATION = f"{RESOURCE_OPERATION}-delete"
COMMUNITY_CREATE_OPERATION = f"{RESOURCE_OPERATION}-community-create"
COMMUNITY_UPDATE_OPERATION = f"{RESOURCE_OPERATION}-community-update"
COMMUNITY_DELETE_OPERATION = f"{RESOURCE_OPERATION}-community-delete"
EXTRAS_UPDATE_OPERATION = f"{RESOURCE_OPERATION}-extras-update"
EXTRAS_DELETE_OPERATION = f"{RESOURCE_OPERATION}-extras-delete"
REDIRECT_OPERATION = f"{RESOURCE_OPERATION}-redirect"
RESOURCE_GET_OPERATION = f"{RESOURCE_OPERATION}-get"
RESOURCE_TYPES_OPERATION = f"{RESOURCE_OPERATION}-types"
COMMUNITY_LIST_OPERATION = f"{RESOURCE_OPERATION}-community-list"
COMMUNITY_GET_OPERATION = f"{RESOURCE_OPERATION}-community-get"
V2_DATASET_GET_OPERATION = f"{RESOURCE_OPERATION}-v2-dataset-get"
V2_RESOURCE_LIST_OPERATION = f"{RESOURCE_OPERATION}-v2-resource-list"
V2_RESOURCE_GET_OPERATION = f"{RESOURCE_OPERATION}-v2-resource-get"
V2_EXTRAS_GET_OPERATION = f"{RESOURCE_OPERATION}-v2-extras-get"


RESOURCE_SCHEMA_SAFE_ACTION = "Verify the response against the pinned uData resource schema."


def _id(value: str, name: str) -> str:
    return path_segment(value, RESOURCE_OPERATION, f"uData resource {name}")


def _request(method: str, path: str, body: object | None = None) -> tuple[str, str, dict[str, str], object | None]:
    return method, path, {}, body


def redirect_resource_request(resource_id: str) -> tuple[str, str, dict[str, str], object | None]:
    return _request("GET", f"/api/1/datasets/r/{_id(resource_id, 'resource id')}")


def redirect_location(headers: object) -> str:
    location = (
        next(
            (
                value
                for key, value in headers.items()
                if isinstance(key, str) and key.lower() == "location" and isinstance(value, str)
            ),
            None,
        )
        if isinstance(headers, Mapping)
        else None
    )
    if not location:
        raise CatalogValidationError(
            "The uData resource redirect omits its Location header.",
            operation=RESOURCE_OPERATION,
            platform="udata",
            safe_action="Verify the response against the pinned uData resource route.",
        )
    return location


def create_resource_request(
    dataset_id: str, client_input: ResourceCreateInput
) -> tuple[str, str, dict[str, str], object]:
    return _request("POST", f"/api/1/datasets/{_id(dataset_id, 'dataset id')}/resources/", client_input.payload())


def update_resources_request(
    dataset_id: str, values: tuple[ResourceUpdateInput, ...]
) -> tuple[str, str, dict[str, str], object]:
    if not values:
        raise CatalogValidationError(
            "uData resource reorder requires at least one resource.",
            operation=REORDER_OPERATION,
            platform="udata",
            safe_action="Pass at least one resource update before reordering.",
        )
    return _request(
        "PUT", f"/api/1/datasets/{_id(dataset_id, 'dataset id')}/resources/", [item.payload() for item in values]
    )


def upload_resource_request(
    dataset_id: str, resource_id: str | None, *, community: bool = False
) -> tuple[str, str, dict[str, str]]:
    if community and resource_id is not None:
        raise CatalogValidationError(
            "Community upload replacement requires reupload_community().",
            operation=UPLOAD_COMMUNITY_REPLACE_OPERATION,
            platform="udata",
            safe_action="Call reupload_community with the existing community-resource ID.",
        )
    dataset = _id(dataset_id, "dataset id")
    if community:
        path = f"/api/1/datasets/{dataset}/upload/community/"
    elif resource_id is None:
        path = f"/api/1/datasets/{dataset}/upload/"
    else:
        path = f"/api/1/datasets/{dataset}/resources/{_id(resource_id, 'resource id')}/upload/"
    return "POST", path, {}


def reupload_community_request(resource_id: str) -> tuple[str, str, dict[str, str]]:
    return (
        "POST",
        f"/api/1/datasets/community_resources/{_id(resource_id, 'community resource id')}/upload/",
        {},
    )


def v2_extras_request(
    method: str, dataset_id: str, resource_id: str, body: object = None
) -> tuple[str, str, dict[str, str], object | None]:
    if method not in {"GET", "PUT", "DELETE"}:
        raise CatalogValidationError(
            "The uData resource method is not documented.",
            operation=RESOURCE_OPERATION,
            platform="udata",
            safe_action="Use GET, PUT, or DELETE for resource routes.",
        )
    return _request(
        method,
        f"/api/2/datasets/{_id(dataset_id, 'dataset id')}/resources/{_id(resource_id, 'resource id')}/extras/",
        body,
    )


def resource_request(
    method: str, dataset_id: str, resource_id: str, *, community: bool = False, body: object = None
) -> tuple[str, str, dict[str, str], object | None]:
    if method not in {"GET", "PUT", "DELETE"}:
        raise CatalogValidationError(
            "The uData resource method is not documented.",
            operation=RESOURCE_OPERATION,
            platform="udata",
            safe_action="Use GET, PUT, or DELETE for resource routes.",
        )
    if community:
        path = f"/api/1/datasets/community_resources/{_id(resource_id, 'community resource id')}/"
    else:
        path = f"/api/1/datasets/{_id(dataset_id, 'dataset id')}/resources/{_id(resource_id, 'resource id')}/"
    return _request(method, path, body)


def list_community_resources_request(
    params: Mapping[str, str | int] | None = None,
) -> tuple[str, str, dict[str, str], object | None]:
    if params is None:
        params = {}
    if not isinstance(params, Mapping) or any(
        not isinstance(key, str)
        or key not in {"page", "page_size", "sort", "dataset", "organization", "owner"}
        or type(value) not in {str, int}
        for key, value in params.items()
    ):
        raise CatalogValidationError(
            "Community resource queries require documented keys and string or integer values.",
            operation=COMMUNITY_LIST_OPERATION,
            platform="udata",
            safe_action="Pass documented community resource filters and bounded pagination.",
        )
    for key, maximum in (("page", 100_000), ("page_size", 100)):
        if key not in params:
            continue
        value = str(params[key])
        if re.fullmatch(r"\d{1,6}", value, flags=re.ASCII) is None or not 1 <= int(value) <= maximum:
            raise CatalogValidationError(
                "Community resource query paging must be bounded positive integers.",
                operation=COMMUNITY_LIST_OPERATION,
                platform="udata",
                safe_action="Pass documented community resource filters and bounded pagination.",
            )
    pairs = [(key, str(value)) for key, value in sorted(params.items())]
    query = urlencode(pairs)
    return _request("GET", "/api/1/datasets/community_resources/" + (f"?{query}" if query else ""))


def community_collection_request(method: str, body: object = None) -> tuple[str, str, dict[str, str], object | None]:
    if method != "POST":
        raise CatalogValidationError(
            "The uData community collection method is not documented.",
            operation=COMMUNITY_CREATE_OPERATION,
            platform="udata",
            safe_action="Use POST for the community collection route.",
        )
    return _request(method, "/api/1/datasets/community_resources/", body)


def resource_types_request() -> tuple[str, str, dict[str, str], object | None]:
    return _request("GET", "/api/1/datasets/resource_types/")


def v2_dataset_request(dataset_id: str) -> tuple[str, str, dict[str, str], object | None]:
    return _request("GET", f"/api/2/datasets/{_id(dataset_id, 'dataset id')}/")


def v2_resource_request(
    dataset_id: str, resource_id: str | None = None, *, extras: str | None = None
) -> tuple[str, str, dict[str, str], object | None]:
    if extras is not None:
        if resource_id is None or dataset_id == "":
            raise CatalogValidationError(
                "The uData v2 extras routes require both a dataset and a resource id.",
                operation=RESOURCE_OPERATION,
                platform="udata",
                safe_action="Pass both identifiers before requesting an extras route.",
            )
        return v2_extras_request(extras, dataset_id, resource_id)
    if resource_id is None:
        path = f"/api/2/datasets/{_id(dataset_id, 'dataset id')}/resources/"
    elif dataset_id == "":
        path = f"/api/2/datasets/resources/{_id(resource_id, 'resource id')}/"
    else:
        path = f"/api/2/datasets/{_id(dataset_id, 'dataset id')}/resources/{_id(resource_id, 'resource id')}/"
    return _request("GET", path)


def parse_resource(payload: object) -> NativeRecord:
    if not isinstance(payload, Mapping) or not isinstance(payload.get("id"), str) or not payload["id"]:
        raise CatalogValidationError(
            "The uData resource response must contain a non-empty id.",
            operation=RESOURCE_OPERATION,
            platform="udata",
            safe_action=RESOURCE_SCHEMA_SAFE_ACTION,
        )
    identifier = cast("str", payload["id"])
    return NativeRecord(
        platform=CatalogPlatform.UDATA,
        resource_kind=ResourceKind.RESOURCE,
        id=CatalogId(platform=CatalogPlatform.UDATA, resource_kind=ResourceKind.RESOURCE, value=identifier),
        payload=cast("Mapping[str, object]", _freeze_json(dict(payload), "udata.resource")),
    )


def parse_v2_dataset(payload: object) -> NativeRecord:
    if not isinstance(payload, Mapping) or any(
        not isinstance(payload.get(field), str) or not payload[field] for field in ("id", "title", "slug")
    ):
        raise CatalogValidationError(
            "The uData v2 dataset response requires id, title, and slug.",
            operation=RESOURCE_OPERATION,
            platform="udata",
            safe_action="Verify the response against the pinned uData v2 dataset schema.",
        )
    identifier = cast("str", payload["id"])
    return NativeRecord(
        platform=CatalogPlatform.UDATA,
        resource_kind=ResourceKind.DATASET,
        id=CatalogId(platform=CatalogPlatform.UDATA, resource_kind=ResourceKind.DATASET, value=identifier),
        payload=cast("Mapping[str, object]", _freeze_json(dict(payload), "udata.v2.dataset")),
    )


def parse_resource_page(payload: object) -> UDataPageEnvelope:
    page = parse_native_page(payload, operation=RESOURCE_OPERATION)
    return shape_native_page(page, tuple(parse_resource(item) for item in page.items))


def parse_resource_types(payload: object) -> tuple[Mapping[str, str], ...]:
    if not isinstance(payload, list):
        raise CatalogValidationError(
            "The uData resource types response must be a list.",
            operation=RESOURCE_OPERATION,
            platform="udata",
            safe_action=RESOURCE_SCHEMA_SAFE_ACTION,
        )
    rows: list[Mapping[str, str]] = []
    for item in payload:
        if not isinstance(item, Mapping) or not all(
            isinstance(item.get(key), str) and item[key] for key in ("id", "label")
        ):
            raise CatalogValidationError(
                "The uData resource type must include a non-empty id and label.",
                operation=RESOURCE_OPERATION,
                platform="udata",
                safe_action=RESOURCE_SCHEMA_SAFE_ACTION,
            )
        rows.append(MappingProxyType({"id": item["id"], "label": item["label"]}))
    return tuple(rows)
