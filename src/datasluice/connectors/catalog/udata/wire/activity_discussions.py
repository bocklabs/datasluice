"""Exact activity and discussion request builders and bounded decoders for uData 17.6."""

from __future__ import annotations

from collections.abc import Mapping

from datasluice.connectors.catalog.udata.models.activity_discussions import (
    ActivityQuery,
    CommentInput,
    DiscussionCreateInput,
    DiscussionSearchQuery,
    DiscussionUpdateInput,
    segment,
)
from datasluice.domain.catalog.models import MappingRecord
from datasluice.errors.catalog import CatalogValidationError

PLATFORM = "udata"
ACTIVITY_OPERATION = "udata/api-v1.site-activity"
LIST_DISCUSSIONS_OPERATION = "udata/api-v1.list-discussions"
GET_DISCUSSION_OPERATION = "udata/api-v1.get-discussion"
CREATE_DISCUSSION_OPERATION = "udata/api-v1.create-discussion"
COMMENT_DISCUSSION_OPERATION = "udata/api-v1.comment-discussion"
UPDATE_DISCUSSION_OPERATION = "udata/api-v1.update-discussion"
DELETE_DISCUSSION_OPERATION = "udata/api-v1.delete-discussion"
EDIT_DISCUSSION_COMMENT_OPERATION = "udata/api-v1.edit-discussion-comment"
DELETE_DISCUSSION_COMMENT_OPERATION = "udata/api-v1.delete-discussion-comment"
SEARCH_DISCUSSIONS_OPERATION = "udata/api-v2.search-discussions"
_PINNED_SCHEMA_ACTION = "Verify the response against the pinned uData 17.6 schema."

type Request = tuple[str, str, dict[str, str], object | None]


def _request(method: str, path: str, body: object = None) -> Request:
    return method, path, {}, body


def activity_request(query: ActivityQuery) -> Request:
    suffix = query.query()
    return _request("GET", f"/api/1/activity/{'?' + suffix if suffix else ''}")


def list_discussions_request() -> Request:
    return _request("GET", "/api/1/discussions/")


def get_discussion_request(discussion_id: str) -> Request:
    return _request("GET", f"/api/1/discussions/{segment(discussion_id, GET_DISCUSSION_OPERATION)}/")


def create_discussion_request(client_input: DiscussionCreateInput) -> Request:
    return _request("POST", "/api/1/discussions/", client_input.payload())


def comment_discussion_request(discussion_id: str, client_input: CommentInput) -> Request:
    return _request(
        "POST",
        f"/api/1/discussions/{segment(discussion_id, COMMENT_DISCUSSION_OPERATION)}/",
        client_input.payload(),
    )


def update_discussion_request(discussion_id: str, client_input: DiscussionUpdateInput) -> Request:
    return _request(
        "PUT",
        f"/api/1/discussions/{segment(discussion_id, UPDATE_DISCUSSION_OPERATION)}/",
        client_input.payload(),
    )


def delete_discussion_request(discussion_id: str) -> Request:
    return _request("DELETE", f"/api/1/discussions/{segment(discussion_id, DELETE_DISCUSSION_OPERATION)}/")


def edit_discussion_comment_request(discussion_id: str, comment_id: str, client_input: CommentInput) -> Request:
    return _request(
        "PUT",
        f"/api/1/discussions/{segment(discussion_id, EDIT_DISCUSSION_COMMENT_OPERATION)}"
        f"/comments/{segment(comment_id, EDIT_DISCUSSION_COMMENT_OPERATION)}/",
        client_input.edit_payload(),
    )


def delete_discussion_comment_request(discussion_id: str, comment_id: str) -> Request:
    return _request(
        "DELETE",
        f"/api/1/discussions/{segment(discussion_id, DELETE_DISCUSSION_COMMENT_OPERATION)}"
        f"/comments/{segment(comment_id, DELETE_DISCUSSION_COMMENT_OPERATION)}/",
    )


def search_discussions_request(query: DiscussionSearchQuery) -> Request:
    return _request("GET", f"/api/2/discussions/search/?{query.query()}")


def parse_mapping(payload: object, operation: str) -> MappingRecord:
    if not isinstance(payload, Mapping):
        raise CatalogValidationError(
            "The uData activity or discussion response must be an object.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return MappingRecord(payload)
