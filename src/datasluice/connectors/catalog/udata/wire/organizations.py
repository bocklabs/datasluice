"""Exact request builders and bounded decoders for uData organizations."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from types import MappingProxyType
from urllib.parse import quote, urlencode

from datasluice.connectors.catalog.udata.mapping import NativePageMetadata, UDataPageEnvelope, parse_native_page
from datasluice.connectors.catalog.udata.models.organizations import (
    MembershipRequestInput,
    MembershipRequestQuery,
    OrganizationCreateInput,
    OrganizationInvitationInput,
    OrganizationListQuery,
    OrganizationSuggestQuery,
    OrganizationUpdateInput,
)
from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.models import MappingRecord, NativeRecord, PageInfo, PlatformMetadata
from datasluice.errors.catalog import CatalogValidationError, NativeCatalogError

PLATFORM = CatalogPlatform.UDATA
ORGANIZATIONS_OPERATION = "udata/api-v1.organizations-and-memberships"

LIST_ORGANIZATIONS_OPERATION = "udata/api-v1.list-organizations"
CREATE_ORGANIZATION_OPERATION = "udata/api-v1.create-organization"
GET_ORGANIZATION_OPERATION = "udata/api-v1.get-organization"
UPDATE_ORGANIZATION_OPERATION = "udata/api-v1.update-organization"
DELETE_ORGANIZATION_OPERATION = "udata/api-v1.delete-organization"
ORGANIZATION_DATASETS_CSV_OPERATION = "udata/api-v1.organization-datasets-csv"
ORGANIZATION_DATASERVICES_CSV_OPERATION = "udata/api-v1.organization-dataservices-csv"
ORGANIZATION_DISCUSSIONS_CSV_OPERATION = "udata/api-v1.organization-discussions-csv"
ORGANIZATION_DATASETS_RESOURCES_CSV_OPERATION = "udata/api-v1.organization-datasets-resources-csv"
RDF_ORGANIZATION_OPERATION = "udata/api-v1.rdf-organization"
RDF_ORGANIZATION_FORMAT_OPERATION = "udata/api-v1.rdf-organization-format"
AVAILABLE_ORGANIZATION_BADGES_OPERATION = "udata/api-v1.available-organization-badges"
ADD_ORGANIZATION_BADGE_OPERATION = "udata/api-v1.add-organization-badge"
DELETE_ORGANIZATION_BADGE_OPERATION = "udata/api-v1.delete-organization-badge"
GET_ORGANIZATION_CONTACT_POINT_OPERATION = "udata/api-v1.get-organization-contact-point"
SUGGEST_ORG_CONTACT_POINTS_OPERATION = "udata/api-v1.suggest-org-contact-points"
LIST_MEMBERSHIP_REQUESTS_OPERATION = "udata/api-v1.list-membership-requests"
MEMBERSHIP_REQUEST_OPERATION = "udata/api-v1.membership-request"
ACCEPT_MEMBERSHIP_OPERATION = "udata/api-v1.accept-membership"
REFUSE_MEMBERSHIP_OPERATION = "udata/api-v1.refuse-membership"
CANCEL_MEMBERSHIP_OPERATION = "udata/api-v1.cancel-membership"
INVITE_ORGANIZATION_MEMBER_OPERATION = "udata/api-v1.invite-organization-member"
UPDATE_ORGANIZATION_MEMBER_OPERATION = "udata/api-v1.update-organization-member"
DELETE_ORGANIZATION_MEMBER_OPERATION = "udata/api-v1.delete-organization-member"
LIST_ORGANIZATION_ASSIGNMENTS_OPERATION = "udata/api-v1.list-organization-assignments"
SYNC_MEMBER_ASSIGNMENTS_OPERATION = "udata/api-v1.sync-member-assignments"
SUGGEST_ORGANIZATIONS_OPERATION = "udata/api-v1.suggest-organizations"
ORGANIZATION_LOGO_OPERATION = "udata/api-v1.organization-logo"
RESIZE_ORGANIZATION_LOGO_OPERATION = "udata/api-v1.resize-organization-logo"
LIST_ORGANIZATION_DATASETS_OPERATION = "udata/api-v1.list-organization-datasets"
LIST_ORGANIZATION_REUSES_OPERATION = "udata/api-v1.list-organization-reuses"
LIST_ORGANIZATION_DISCUSSIONS_OPERATION = "udata/api-v1.list-organization-discussions"
ORG_ROLES_OPERATION = "udata/api-v1.org-roles"
SEARCH_ORGANIZATIONS_OPERATION = "udata/api-v2.search-organizations"
GET_ORGANIZATION_EXTRAS_OPERATION = "udata/api-v2.get-organization-extras"
UPDATE_ORGANIZATION_EXTRAS_OPERATION = "udata/api-v2.update-organization-extras"
DELETE_ORGANIZATION_EXTRAS_OPERATION = "udata/api-v2.delete-organization-extras"
LIST_ORGANIZATION_FOLLOWERS_OPERATION = "udata/api-v1.list-organization-followers"
FOLLOW_ORGANIZATION_OPERATION = "udata/api-v1.follow-organization"
UNFOLLOW_ORGANIZATION_OPERATION = "udata/api-v1.unfollow-organization"

_ORGANIZATION_KIND = ResourceKind.ORGANIZATION
_MEMBERSHIP_KIND = ResourceKind("membership")
_MEMBER_KIND = ResourceKind("member")
_ASSIGNMENT_KIND = ResourceKind("assignment")
_BADGE_KIND = ResourceKind("badge")
_CONTACT_KIND = ResourceKind("contact-point")
_FOLLOW_KIND = ResourceKind("follow")


def _required_id(value: object, *, operation: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or any(character in value for character in "/?#")
        or value in {".", ".."}
    ):
        raise CatalogValidationError(
            "The uData organization identifier must be one non-dot path segment.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Pass the organization id or slug from a prior typed read.",
        )
    return value


def _segment(value: object, *, operation: str) -> str:
    return quote(_required_id(value, operation=operation), safe="")


def _query(params: list[tuple[str, str]]) -> str:
    return urlencode(params)


def _path_request(method: str, path: str, body: object = None) -> tuple[str, str, dict[str, str], object]:
    return method, path, {}, body


def list_organizations_request(query: OrganizationListQuery | None = None) -> tuple[str, str, dict[str, str], None]:
    query = query or OrganizationListQuery()
    return "GET", "/api/1/organizations/?" + _query(query.query_params()), {}, None


def create_organization_request(client_input: OrganizationCreateInput) -> tuple[str, str, dict[str, str], object]:
    return _path_request("POST", "/api/1/organizations/", client_input.payload())


def get_organization_request(organization_id: str) -> tuple[str, str, dict[str, str], None]:
    return "GET", f"/api/1/organizations/{_segment(organization_id, operation=GET_ORGANIZATION_OPERATION)}/", {}, None


def update_organization_request(
    organization_id: str, client_input: OrganizationUpdateInput
) -> tuple[str, str, dict[str, str], object]:
    return _path_request(
        "PUT",
        f"/api/1/organizations/{_segment(organization_id, operation=UPDATE_ORGANIZATION_OPERATION)}/",
        client_input.payload(),
    )


def delete_organization_request(organization_id: str) -> tuple[str, str, dict[str, str], None]:
    return (
        "DELETE",
        f"/api/1/organizations/{_segment(organization_id, operation=DELETE_ORGANIZATION_OPERATION)}/",
        {},
        None,
    )


def organization_export_request(organization_id: str, export_name: str) -> tuple[str, str, dict[str, str], None]:
    if export_name not in {"datasets", "dataservices", "discussions", "datasets-resources"}:
        raise CatalogValidationError(
            "The uData organization export is not documented.",
            operation=ORGANIZATIONS_OPERATION,
            platform=PLATFORM.value,
            safe_action="Use one of the pinned organization CSV routes.",
        )
    return (
        "GET",
        f"/api/1/organizations/{_segment(organization_id, operation=ORGANIZATIONS_OPERATION)}/{export_name}.csv",
        {},
        None,
    )


def rdf_organization_request(organization_id: str) -> tuple[str, str, dict[str, str], None]:
    return (
        "GET",
        f"/api/1/organizations/{_segment(organization_id, operation=RDF_ORGANIZATION_OPERATION)}/catalog",
        {},
        None,
    )


def rdf_organization_format_request(organization_id: str, fmt: str) -> tuple[str, str, dict[str, str], None]:
    if not isinstance(fmt, str) or re.fullmatch(r"[A-Za-z0-9_-]{1,16}", fmt) is None:
        raise CatalogValidationError(
            "The uData organization catalog format is invalid.",
            operation=RDF_ORGANIZATION_FORMAT_OPERATION,
            platform=PLATFORM.value,
            safe_action="Pass a short documented RDF format extension.",
        )
    segment = _segment(organization_id, operation=RDF_ORGANIZATION_FORMAT_OPERATION)
    return "GET", f"/api/1/organizations/{segment}/catalog.{fmt.lower()}", {}, None


def available_organization_badges_request() -> tuple[str, str, dict[str, str], None]:
    return "GET", "/api/1/organizations/badges/", {}, None


def organization_badge_request(
    organization_id: str, badge_kind: str, *, delete: bool = False
) -> tuple[str, str, dict[str, str], object]:
    operation = DELETE_ORGANIZATION_BADGE_OPERATION if delete else ADD_ORGANIZATION_BADGE_OPERATION
    badge = _required_id(badge_kind, operation=operation)
    path = f"/api/1/organizations/{_segment(organization_id, operation=operation)}/badges/"
    if delete:
        path += f"{_segment(badge, operation=operation)}/"
        return _path_request("DELETE", path)
    return _path_request("POST", path, {"kind": badge})


def organization_contacts_request(
    organization_id: str, query: OrganizationListQuery | None = None
) -> tuple[str, str, dict[str, str], None]:
    query = query or OrganizationListQuery()
    segment = _segment(organization_id, operation=GET_ORGANIZATION_CONTACT_POINT_OPERATION)
    return "GET", f"/api/1/organizations/{segment}/contacts/?{_query(query.query_params())}", {}, None


def organization_contacts_suggest_request(
    organization_id: str, query: OrganizationSuggestQuery
) -> tuple[str, str, dict[str, str], None]:
    segment = _segment(organization_id, operation=SUGGEST_ORG_CONTACT_POINTS_OPERATION)
    return "GET", f"/api/1/organizations/{segment}/contacts/suggest/?{_query(query.query_params())}", {}, None


def membership_requests_request(
    organization_id: str, query: MembershipRequestQuery | None = None
) -> tuple[str, str, dict[str, str], None]:
    query = query or MembershipRequestQuery()
    params = query.query_params()
    path = f"/api/1/organizations/{_segment(organization_id, operation=LIST_MEMBERSHIP_REQUESTS_OPERATION)}/membership/"
    return "GET", path + (f"?{_query(params)}" if params else ""), {}, None


def membership_request_request(
    organization_id: str, client_input: MembershipRequestInput
) -> tuple[str, str, dict[str, str], object]:
    return _path_request(
        "POST",
        f"/api/1/organizations/{_segment(organization_id, operation=MEMBERSHIP_REQUEST_OPERATION)}/membership/",
        client_input.payload(),
    )


def membership_action_request(
    organization_id: str, request_id: str, action: str
) -> tuple[str, str, dict[str, str], object]:
    if action not in {"accept", "refuse", "cancel"}:
        raise CatalogValidationError(
            "The uData membership action is not documented.",
            operation=ORGANIZATIONS_OPERATION,
            platform=PLATFORM.value,
            safe_action="Use accept, refuse, or cancel.",
        )
    operation = {
        "accept": ACCEPT_MEMBERSHIP_OPERATION,
        "refuse": REFUSE_MEMBERSHIP_OPERATION,
        "cancel": CANCEL_MEMBERSHIP_OPERATION,
    }[action]
    organization_segment = _segment(organization_id, operation=operation)
    request_segment = _segment(request_id, operation=operation)
    return _path_request("POST", f"/api/1/organizations/{organization_segment}/membership/{request_segment}/{action}/")


def invite_member_request(
    organization_id: str, client_input: OrganizationInvitationInput
) -> tuple[str, str, dict[str, str], object]:
    return _path_request(
        "POST",
        f"/api/1/organizations/{_segment(organization_id, operation=INVITE_ORGANIZATION_MEMBER_OPERATION)}/member/",
        client_input.payload(),
    )


def member_request(
    organization_id: str, user_id: str, *, method: str, body: object = None
) -> tuple[str, str, dict[str, str], object]:
    operation = UPDATE_ORGANIZATION_MEMBER_OPERATION if method == "PUT" else DELETE_ORGANIZATION_MEMBER_OPERATION
    organization_segment = _segment(organization_id, operation=operation)
    user_segment = _segment(user_id, operation=operation)
    path = f"/api/1/organizations/{organization_segment}/member/{user_segment}/"
    return _path_request(method, path, body)


def assignments_request(organization_id: str) -> tuple[str, str, dict[str, str], None]:
    segment = _segment(organization_id, operation=LIST_ORGANIZATION_ASSIGNMENTS_OPERATION)
    return "GET", f"/api/1/organizations/{segment}/assignments/", {}, None


def member_assignments_request(
    organization_id: str, user_id: str, assignments: list[Mapping[str, object]]
) -> tuple[str, str, dict[str, str], object]:
    organization_segment = _segment(organization_id, operation=SYNC_MEMBER_ASSIGNMENTS_OPERATION)
    user_segment = _segment(user_id, operation=SYNC_MEMBER_ASSIGNMENTS_OPERATION)
    path = f"/api/1/organizations/{organization_segment}/member/{user_segment}/assignments/"
    return _path_request("PUT", path, assignments)


def suggest_organizations_request(query: OrganizationSuggestQuery) -> tuple[str, str, dict[str, str], None]:
    return "GET", "/api/1/organizations/suggest/?" + _query(query.query_params()), {}, None


def organization_logo_request(organization_id: str, *, resize: bool = False) -> tuple[str, str, dict[str, str], None]:
    operation = RESIZE_ORGANIZATION_LOGO_OPERATION if resize else ORGANIZATION_LOGO_OPERATION
    method = "PUT" if resize else "POST"
    return method, f"/api/1/organizations/{_segment(organization_id, operation=operation)}/logo/", {}, None


def organization_owned_request(
    organization_id: str, name: str, query: OrganizationListQuery | None = None
) -> tuple[str, str, dict[str, str], None]:
    if name not in {"datasets", "reuses", "discussions"}:
        raise CatalogValidationError(
            "The uData organization owned-object route is not documented.",
            operation=ORGANIZATIONS_OPERATION,
            platform=PLATFORM.value,
            safe_action="Use datasets, reuses, or discussions.",
        )
    query = query or OrganizationListQuery()
    params = query.query_params() if name == "datasets" else []
    path = f"/api/1/organizations/{_segment(organization_id, operation=ORGANIZATIONS_OPERATION)}/{name}/"
    return "GET", path + (f"?{_query(params)}" if params else ""), {}, None


def organization_roles_request() -> tuple[str, str, dict[str, str], None]:
    return "GET", "/api/1/organizations/roles/", {}, None


def search_organizations_request(query: OrganizationListQuery | None = None) -> tuple[str, str, dict[str, str], None]:
    query = query or OrganizationListQuery()
    return "GET", "/api/2/organizations/search/?" + _query(query.query_params()), {}, None


def organization_extras_request(
    organization_id: str, *, method: str, body: object = None
) -> tuple[str, str, dict[str, str], object]:
    operation = {
        "GET": GET_ORGANIZATION_EXTRAS_OPERATION,
        "PUT": UPDATE_ORGANIZATION_EXTRAS_OPERATION,
        "DELETE": DELETE_ORGANIZATION_EXTRAS_OPERATION,
    }.get(method)
    if operation is None:
        raise CatalogValidationError(
            "The uData organization extras method is not documented.",
            operation=ORGANIZATIONS_OPERATION,
            platform=PLATFORM.value,
            safe_action="Use GET, PUT, or DELETE.",
        )
    return _path_request(method, f"/api/2/organizations/{_segment(organization_id, operation=operation)}/extras/", body)


def followers_request(organization_id: str, *, method: str) -> tuple[str, str, dict[str, str], None]:
    operation = {
        "GET": LIST_ORGANIZATION_FOLLOWERS_OPERATION,
        "POST": FOLLOW_ORGANIZATION_OPERATION,
        "DELETE": UNFOLLOW_ORGANIZATION_OPERATION,
    }.get(method)
    if operation is None:
        raise CatalogValidationError(
            "The uData organization follower method is not documented.",
            operation=ORGANIZATIONS_OPERATION,
            platform=PLATFORM.value,
            safe_action="Use GET, POST, or DELETE.",
        )
    return (
        "GET" if method == "GET" else method,
        f"/api/1/organizations/{_segment(organization_id, operation=operation)}/followers/",
        {},
        None,
    )


def _required_record_id(payload: Mapping[str, object], *, operation: str) -> str:
    value = payload.get("id")
    if not isinstance(value, str) or not value:
        raise CatalogValidationError(
            "The uData organization response omitted its id.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Verify the response against the pinned uData organization schema.",
        )
    return value


def parse_organization(payload: object, *, operation: str = GET_ORGANIZATION_OPERATION) -> NativeRecord:
    if not isinstance(payload, Mapping):
        raise CatalogValidationError(
            "The uData organization response must be a JSON object.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Verify the response against the pinned uData organization schema.",
        )
    identifier = _required_record_id(payload, operation=operation)
    name = payload.get("name")
    if not isinstance(name, str) or not name:
        raise CatalogValidationError(
            "The uData organization response omitted its name.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Verify the response against the pinned uData organization schema.",
        )
    return NativeRecord(
        platform=PLATFORM,
        resource_kind=_ORGANIZATION_KIND,
        id=CatalogId(platform=PLATFORM, resource_kind=_ORGANIZATION_KIND, value=identifier),
        payload=dict(payload),
    )


def _mapping_record(payload: Mapping[str, object], *, kind: ResourceKind, operation: str) -> MappingRecord:
    values = dict(payload)
    values.update({"resource_kind": kind.value, "operation": operation})
    return MappingRecord(payload=MappingProxyType(values))


def parse_records(
    payload: object, *, operation: str, kind: ResourceKind = _MEMBERSHIP_KIND
) -> tuple[MappingRecord, ...]:
    values = payload.get("data") if isinstance(payload, Mapping) and isinstance(payload.get("data"), list) else payload
    if not isinstance(values, list) or not all(isinstance(item, Mapping) for item in values):
        raise CatalogValidationError(
            "The uData organization response must be a JSON array of objects.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Verify the response against the pinned uData organization schema.",
        )
    return tuple(_mapping_record(item, kind=kind, operation=operation) for item in values)


def parse_organization_page(payload: object, *, operation: str = LIST_ORGANIZATIONS_OPERATION) -> UDataPageEnvelope:
    page = parse_native_page(payload, operation=operation)
    records = tuple(parse_organization(item, operation=operation) for item in page.items)
    page_info = (
        None
        if page.page is None
        else PageInfo(
            cursor=str(page.page), next_cursor=str(page.page + 1) if page.next_page else None, total_items=page.total
        )
    )
    native_page = NativePageMetadata(
        present_fields=page.present_fields,
        page=page.page,
        page_size=page.page_size,
        previous_page=page.previous_page,
        next_page=page.next_page,
        total=page.total,
    )
    return UDataPageEnvelope(
        items=records,
        page=page_info,
        platform=PlatformMetadata(platform=PLATFORM, extensions={"udata.page": native_page.to_dict()}),
        native_page=native_page,
    )


def parse_page(payload: object, *, operation: str, kind: ResourceKind) -> UDataPageEnvelope:
    page = parse_native_page(payload, operation=operation)
    records = tuple(
        NativeRecord(
            platform=PLATFORM,
            resource_kind=kind,
            id=CatalogId(platform=PLATFORM, resource_kind=kind, value=_required_record_id(item, operation=operation)),
            payload=dict(item),
        )
        for item in page.items
    )
    page_info = (
        None
        if page.page is None
        else PageInfo(
            cursor=str(page.page), next_cursor=str(page.page + 1) if page.next_page else None, total_items=page.total
        )
    )
    native_page = NativePageMetadata(
        present_fields=page.present_fields,
        page=page.page,
        page_size=page.page_size,
        previous_page=page.previous_page,
        next_page=page.next_page,
        total=page.total,
    )
    return UDataPageEnvelope(
        items=records,
        page=page_info,
        platform=PlatformMetadata(platform=PLATFORM, extensions={"udata.page": native_page.to_dict()}),
        native_page=native_page,
    )


def parse_organization_search(payload: object, *, operation: str = SEARCH_ORGANIZATIONS_OPERATION) -> UDataPageEnvelope:
    return parse_organization_page(payload, operation=operation)


def parse_roles(payload: object, *, operation: str = ORG_ROLES_OPERATION) -> tuple[MappingRecord, ...]:
    return parse_records(payload, operation=operation, kind=ResourceKind("role"))


def parse_extras(payload: object, *, operation: str = GET_ORGANIZATION_EXTRAS_OPERATION) -> Mapping[str, object]:
    if not isinstance(payload, Mapping):
        raise CatalogValidationError(
            "The uData organization extras response must be a JSON object.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Verify the response against the pinned uData extras schema.",
        )
    return MappingProxyType(dict(payload))


def parse_document(
    body: bytes,
    *,
    endpoint: str,
    media_type: str,
    status_code: int,
    operation: str,
    location: str | None = None,
) -> dict[str, object]:
    if not isinstance(body, bytes):
        raise NativeCatalogError(
            "The uData organization document body must be bytes.", operation=operation, platform=PLATFORM.value
        )
    digest = hashlib.sha256(body).hexdigest()
    return {
        "endpoint": endpoint,
        "media_type": media_type.split(";", 1)[0].strip().lower(),
        "status_code": status_code,
        "size_bytes": len(body),
        "sha256": digest,
        **({"location": location} if location else {}),
    }


__all__ = [
    name
    for name in globals()
    if name.endswith("_OPERATION")
    or name.endswith("_request")
    or name.startswith("parse_")
    or name in {"ORGANIZATIONS_OPERATION"}
]
