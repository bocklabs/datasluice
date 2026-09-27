"""Exact post, report, and notification request builders and bounded decoders for uData 17.6."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from urllib.parse import urlencode

from datasluice.connectors.catalog.udata.models.posts_reports import (
    NotificationQuery,
    PostCreateInput,
    PostListQuery,
    PostSearchQuery,
    PostUpdateInput,
    ReportCreateInput,
    ReportQuery,
    ReportUpdateInput,
    segment,
)
from datasluice.domain.catalog.models import MappingRecord
from datasluice.errors.catalog import CatalogValidationError, NativeCatalogError

PLATFORM = "udata"
LIST_REPORTS_OPERATION = "udata/api-v1.list-reports"
CREATE_REPORT_OPERATION = "udata/api-v1.create-report"
GET_REPORT_OPERATION = "udata/api-v1.get-report"
UPDATE_REPORT_OPERATION = "udata/api-v1.update-report"
LIST_REPORTS_REASONS_OPERATION = "udata/api-v1.list-reports-reasons"
LIST_POSTS_OPERATION = "udata/api-v1.list-posts"
CREATE_POST_OPERATION = "udata/api-v1.create-post"
RECENT_POSTS_ATOM_FEED_OPERATION = "udata/api-v1.recent-posts-atom"
GET_POST_OPERATION = "udata/api-v1.get-post"
UPDATE_POST_OPERATION = "udata/api-v1.update-post"
DELETE_POST_OPERATION = "udata/api-v1.delete-post"
PUBLISH_POST_OPERATION = "udata/api-v1.publish-post"
UNPUBLISH_POST_OPERATION = "udata/api-v1.unpublish-post"
POST_IMAGE_OPERATION = "udata/api-v1.post-image"
RESIZE_POST_IMAGE_OPERATION = "udata/api-v1.resize-post-image"
SEARCH_POSTS_OPERATION = "udata/api-v2.search-posts"
LIST_NOTIFICATIONS_OPERATION = "udata/api-v1.list-notifications"
READ_NOTIFICATION_OPERATION = "udata/api-v1.read-notification"

OPERATIONS = {
    LIST_REPORTS_OPERATION,
    CREATE_REPORT_OPERATION,
    GET_REPORT_OPERATION,
    UPDATE_REPORT_OPERATION,
    LIST_REPORTS_REASONS_OPERATION,
    LIST_POSTS_OPERATION,
    CREATE_POST_OPERATION,
    RECENT_POSTS_ATOM_FEED_OPERATION,
    GET_POST_OPERATION,
    UPDATE_POST_OPERATION,
    DELETE_POST_OPERATION,
    PUBLISH_POST_OPERATION,
    UNPUBLISH_POST_OPERATION,
    POST_IMAGE_OPERATION,
    RESIZE_POST_IMAGE_OPERATION,
    SEARCH_POSTS_OPERATION,
    LIST_NOTIFICATIONS_OPERATION,
    READ_NOTIFICATION_OPERATION,
}

_POSTS_PATH = "/api/1/posts/"
_REPORTS_PATH = "/api/1/reports/"
_NOTIFICATIONS_PATH = "/api/1/notifications/"
_ATOM_MEDIA_TYPE = "application/atom+xml"
_PINNED_SCHEMA_ACTION = "Verify the response against the pinned uData 17.6 schema."

type Request = tuple[str, str, dict[str, str], object | None]


def _request(method: str, path: str, body: object = None) -> Request:
    return method, path, {}, body


def _query(pairs: list[tuple[str, str]]) -> str:
    return urlencode(pairs)


def list_reports_request(query: ReportQuery | None = None) -> Request:
    return _request("GET", f"{_REPORTS_PATH}?{_query((query or ReportQuery()).query_params())}")


def create_report_request(client_input: ReportCreateInput) -> Request:
    return _request("POST", _REPORTS_PATH, client_input.payload())


def get_report_request(report_id: str) -> Request:
    return _request("GET", f"{_REPORTS_PATH}{segment(report_id, GET_REPORT_OPERATION)}/")


def update_report_request(report_id: str, client_input: ReportUpdateInput) -> Request:
    return _request("PATCH", f"{_REPORTS_PATH}{segment(report_id, UPDATE_REPORT_OPERATION)}/", client_input.payload())


def list_reports_reasons_request() -> Request:
    return _request("GET", f"{_REPORTS_PATH}reasons/")


def list_posts_request(query: PostListQuery | None = None) -> Request:
    return _request("GET", f"{_POSTS_PATH}?{_query((query or PostListQuery()).query_params())}")


def create_post_request(client_input: PostCreateInput) -> Request:
    return _request("POST", _POSTS_PATH, client_input.payload())


def recent_posts_atom_feed_request() -> Request:
    return _request("GET", f"{_POSTS_PATH}recent.atom")


def get_post_request(post_id: str) -> Request:
    return _request("GET", f"{_POSTS_PATH}{segment(post_id, GET_POST_OPERATION)}/")


def update_post_request(post_id: str, client_input: PostUpdateInput) -> Request:
    return _request("PUT", f"{_POSTS_PATH}{segment(post_id, UPDATE_POST_OPERATION)}/", client_input.payload())


def delete_post_request(post_id: str) -> Request:
    return _request("DELETE", f"{_POSTS_PATH}{segment(post_id, DELETE_POST_OPERATION)}/")


def publish_post_request(post_id: str) -> Request:
    return _request("POST", f"{_POSTS_PATH}{segment(post_id, PUBLISH_POST_OPERATION)}/publish/")


def unpublish_post_request(post_id: str) -> Request:
    return _request("DELETE", f"{_POSTS_PATH}{segment(post_id, UNPUBLISH_POST_OPERATION)}/publish/")


def post_image_request(post_id: str) -> tuple[str, str, dict[str, str]]:
    return "POST", f"{_POSTS_PATH}{segment(post_id, POST_IMAGE_OPERATION)}/image/", {}


def resize_post_image_request(post_id: str) -> tuple[str, str, dict[str, str]]:
    return "PUT", f"{_POSTS_PATH}{segment(post_id, RESIZE_POST_IMAGE_OPERATION)}/image/", {}


def search_posts_request(query: PostSearchQuery | None = None) -> Request:
    return _request("GET", f"/api/2/posts/search/?{_query((query or PostSearchQuery()).query_params())}")


def list_notifications_request(query: NotificationQuery | None = None) -> Request:
    return _request("GET", f"{_NOTIFICATIONS_PATH}?{_query((query or NotificationQuery()).query_params())}")


def read_notification_request(notification_id: str) -> Request:
    return _request(
        "POST",
        f"{_NOTIFICATIONS_PATH}{segment(notification_id, READ_NOTIFICATION_OPERATION)}/read/",
    )


def parse_mapping(payload: object, operation: str) -> MappingRecord:
    if not isinstance(payload, Mapping):
        raise CatalogValidationError(
            "The uData post, report, or notification response must be an object.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return MappingRecord(payload)


def parse_mapping_sequence(payload: object, operation: str) -> tuple[MappingRecord, ...]:
    if not isinstance(payload, list) or not all(isinstance(item, Mapping) for item in payload):
        raise CatalogValidationError(
            "The uData post, report, or notification response must be a list of objects.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return tuple(MappingRecord(item) for item in payload)


def parse_text_document(
    body: bytes,
    media_type: str,
    *,
    response_media_type: str | None = None,
    operation: str = RECENT_POSTS_ATOM_FEED_OPERATION,
) -> MappingRecord:
    """Bound a non-JSON atom document, retaining only media type, size, and digest."""
    if not isinstance(body, bytes):
        raise NativeCatalogError(
            "The uData post document body must be buffered bytes.",
            operation=operation,
            platform=PLATFORM,
        )
    try:
        body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise NativeCatalogError(
            "The uData post text document is not valid UTF-8.",
            operation=operation,
            platform=PLATFORM,
        ) from exc
    negotiated = (response_media_type or media_type).split(";", 1)[0].strip().lower()
    if negotiated != _ATOM_MEDIA_TYPE:
        raise NativeCatalogError(
            f"The uData post document media type {negotiated!r} is not an approved text contract.",
            operation=operation,
            platform=PLATFORM,
        )
    digest = hashlib.sha256(body).hexdigest()
    return MappingRecord({"media_type": negotiated, "size_bytes": len(body), "sha256": digest})


def content_type(headers: Mapping[str, str]) -> str | None:
    for key, value in headers.items():
        if key.lower() == "content-type":
            return value
    return None
