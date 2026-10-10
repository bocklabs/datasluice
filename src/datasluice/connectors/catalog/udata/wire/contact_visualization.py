"""Exact contact-point and visualization request builders and bounded decoders for uData 17.6."""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import urlencode

from datasluice.connectors.catalog.udata.mapping import parse_native_page
from datasluice.connectors.catalog.udata.models.contact_visualization import (
    CONTACT_ROLES,
    ContactPointCreateInput,
    ContactPointUpdateInput,
    VisualizationCreateInput,
    VisualizationListQuery,
    VisualizationPage,
    VisualizationUpdateInput,
    segment,
)
from datasluice.domain.catalog.models import MappingRecord, redact_record_payload
from datasluice.errors.catalog import CatalogValidationError

PLATFORM = "udata"
LIST_VISUALIZATIONS_OPERATION = "udata/api-v1.list-visualizations"
CREATE_VISUALIZATION_OPERATION = "udata/api-v1.create-visualization"
GET_VISUALIZATION_OPERATION = "udata/api-v1.get-visualization"
UPDATE_VISUALIZATION_OPERATION = "udata/api-v1.update-visualization"
DELETE_VISUALIZATION_OPERATION = "udata/api-v1.delete-visualization"
VISUALIZATION_IMAGE_OPERATION = "udata/api-v1.visualization-image"
CREATE_CONTACT_POINT_OPERATION = "udata/api-v1.create-contact-point"
GET_CONTACT_POINT_OPERATION = "udata/api-v1.get-contact-point"
UPDATE_CONTACT_POINT_OPERATION = "udata/api-v1.update-contact-point"
DELETE_CONTACT_POINT_OPERATION = "udata/api-v1.delete-contact-point"
CONTACT_POINT_ROLES_OPERATION = "udata/api-v1.contact-point-roles"

_CONTACT_POINTS_PATH = "/api/1/contacts/"
_VISUALIZATIONS_PATH = "/api/1/visualizations/"
_PINNED_SCHEMA_ACTION = "Verify the response against the pinned uData 17.6 schema."
CONTACT_POINT_KIND = "contact-point"
VISUALIZATION_KIND = "visualization"
CONTACT_ROLE_KIND = "contact-role"

_CONTACT_POINT_READ_FIELDS = (
    "id",
    "name",
    "email",
    "contact_form",
    "role",
    "owner",
    "organization",
)
_VISUALIZATION_READ_FIELDS = (
    "id",
    "title",
    "slug",
    "description",
    "private",
    "extras",
    "deleted_at",
    "x_axis",
    "y_axis",
    "series",
    "image",
    "metrics",
    "permissions",
    "owner",
    "organization",
    "created_at",
    "last_modified",
)

type Request = tuple[str, str, dict[str, str], object | None]
type Upload = tuple[str, str, dict[str, str]]


def _request(method: str, path: str, body: object = None) -> Request:
    return method, path, {}, body


def _query(pairs: list[tuple[str, str]]) -> str:
    return urlencode(pairs)


def _upload_request(visualization_id: str) -> Upload:
    return "POST", f"{_VISUALIZATIONS_PATH}{segment(visualization_id, VISUALIZATION_IMAGE_OPERATION)}/image/", {}


def list_visualizations_request(query: VisualizationListQuery) -> Request:
    """Encode GET /api/1/visualizations/ with the exact stock index-parser query."""
    return _request("GET", f"{_VISUALIZATIONS_PATH}?{_query(query.query_params())}")


def create_visualization_request(client_input: VisualizationCreateInput) -> Request:
    return _request("POST", _VISUALIZATIONS_PATH, client_input.payload())


def get_visualization_request(visualization_id: str) -> Request:
    return _request("GET", f"{_VISUALIZATIONS_PATH}{segment(visualization_id, GET_VISUALIZATION_OPERATION)}/")


def update_visualization_request(visualization_id: str, client_input: VisualizationUpdateInput) -> Request:
    return _request(
        "PATCH",
        f"{_VISUALIZATIONS_PATH}{segment(visualization_id, UPDATE_VISUALIZATION_OPERATION)}/",
        client_input.payload(),
    )


def delete_visualization_request(visualization_id: str) -> Request:
    return _request("DELETE", f"{_VISUALIZATIONS_PATH}{segment(visualization_id, DELETE_VISUALIZATION_OPERATION)}/")


def visualization_image_request(visualization_id: str) -> Upload:
    """Encode the stock multipart image route; the shared seam carries the file part alone."""
    return _upload_request(visualization_id)


def create_contact_point_request(client_input: ContactPointCreateInput) -> Request:
    return _request("POST", _CONTACT_POINTS_PATH, client_input.payload())


def get_contact_point_request(contact_point_id: str) -> Request:
    return _request("GET", f"{_CONTACT_POINTS_PATH}{segment(contact_point_id, GET_CONTACT_POINT_OPERATION)}/")


def update_contact_point_request(contact_point_id: str, client_input: ContactPointUpdateInput) -> Request:
    return _request(
        "PUT",
        f"{_CONTACT_POINTS_PATH}{segment(contact_point_id, UPDATE_CONTACT_POINT_OPERATION)}/",
        client_input.payload(),
    )


def delete_contact_point_request(contact_point_id: str) -> Request:
    return _request("DELETE", f"{_CONTACT_POINTS_PATH}{segment(contact_point_id, DELETE_CONTACT_POINT_OPERATION)}/")


def contact_point_roles_request() -> Request:
    return _request("GET", f"{_CONTACT_POINTS_PATH}roles/")


def _required_identifier(payload: Mapping[str, object], *, operation: str) -> str:
    value = payload.get("id")
    if not isinstance(value, str) or not value:
        raise CatalogValidationError(
            "The uData response omitted its identifier.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return value


def _allowlisted(
    payload: object,
    fields: tuple[str, ...],
    *,
    kind: str,
    operation: str,
) -> MappingRecord:
    """Return one record retaining only the documented read fields, redacted.

    A contact point or visualization response is never passed through: only the
    allowlisted read fields the pinned source documents are retained, everything
    else the deployment sent is dropped, credential-shaped keys and string
    content are redacted, and nesting plus text length stay bounded.

    Raises:
        CatalogValidationError: If the payload is not an object or omits its identifier.
    """
    if not isinstance(payload, Mapping):
        raise CatalogValidationError(
            "The uData response must be a JSON object.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    identifier = _required_identifier(payload, operation=operation)
    values: dict[str, object] = redact_record_payload({key: payload[key] for key in fields if key in payload})
    values.update({"resource_kind": kind, "operation": operation})
    return MappingRecord(payload=values | {"id": identifier})


def parse_contact_point(payload: object, operation: str = GET_CONTACT_POINT_OPERATION) -> MappingRecord:
    """Decode one contact point into an allowlisted, redacted record."""
    return _allowlisted(payload, _CONTACT_POINT_READ_FIELDS, kind=CONTACT_POINT_KIND, operation=operation)


def parse_visualization(payload: object, operation: str = GET_VISUALIZATION_OPERATION) -> MappingRecord:
    """Decode one visualization into an allowlisted, redacted record."""
    return _allowlisted(payload, _VISUALIZATION_READ_FIELDS, kind=VISUALIZATION_KIND, operation=operation)


def parse_contact_point_roles(
    payload: object, operation: str = CONTACT_POINT_ROLES_OPERATION
) -> tuple[MappingRecord, ...]:
    """Decode the stock contact-role list into bounded records."""
    if not isinstance(payload, list) or not all(isinstance(item, Mapping) for item in payload):
        raise CatalogValidationError(
            "The uData contact point roles response must be a list of objects.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    documented = set(CONTACT_ROLES)
    records: list[MappingRecord] = []
    for item in payload:
        identifier = _required_identifier(item, operation=operation)
        if identifier not in documented:
            raise CatalogValidationError(
                "The uData contact point roles response carries an undocumented role.",
                operation=operation,
                platform=PLATFORM,
                safe_action=_PINNED_SCHEMA_ACTION,
            )
        label = item.get("label")
        if not isinstance(label, str) or not label:
            raise CatalogValidationError(
                "The uData contact point roles response omitted a role label.",
                operation=operation,
                platform=PLATFORM,
                safe_action=_PINNED_SCHEMA_ACTION,
            )
        records.append(
            MappingRecord(
                payload=redact_record_payload({"id": identifier, "label": label})
                | {"resource_kind": CONTACT_ROLE_KIND, "operation": operation}
            )
        )
    return tuple(records)


def parse_visualization_page(payload: object, operation: str = LIST_VISUALIZATIONS_OPERATION) -> VisualizationPage:
    """Decode the stock v1 visualization page, retaining its native pager metadata."""
    page = parse_native_page(payload, operation=operation)
    return VisualizationPage(
        items=tuple(parse_visualization(item, operation=operation) for item in page.items),
        page=page.page,
        page_size=page.page_size,
        previous_page=page.previous_page,
        next_page=page.next_page,
        total=page.total,
        present_fields=page.present_fields,
    )
