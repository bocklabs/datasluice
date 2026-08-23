"""Decoding of CKAN Action API wire payloads into typed catalog values."""

from __future__ import annotations

import math
from collections.abc import Mapping
from types import MappingProxyType
from typing import cast

from datasluice.connectors.catalog.ckan.errors import map_envelope_error
from datasluice.connectors.catalog.ckan.results import CKANTokenResult
from datasluice.contracts.catalog.native.ckan import CKANResultItem
from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.models import MappingRecord, NativeRecord, PageInfo, ResultEnvelope, ValueRecord
from datasluice.errors.catalog import NativeCatalogError

PLATFORM = CatalogPlatform.CKAN

GROUP = ResourceKind("group")
TAG = ResourceKind("tag")
VOCABULARY = ResourceKind("vocabulary")
MEMBER = ResourceKind("member")
ACTIVITY = ResourceKind("activity")
JOB = ResourceKind("job")
TASK = ResourceKind("task")

_RECORD = "record"
_RECORD_LIST = "record-list"
_VALUE = "value"
_VALUE_LIST = "value-list"
_MAPPING = "mapping"
_TOKEN_SECRET = "token-secret"

RECORD_KINDS: Mapping[str, ResourceKind] = MappingProxyType(
    {
        "package": ResourceKind.DATASET,
        "resource": ResourceKind.RESOURCE,
        "organization": ResourceKind.ORGANIZATION,
        "user": ResourceKind.USER,
        "group": GROUP,
        "tag": TAG,
        "vocabulary": VOCABULARY,
        "member": MEMBER,
        "activity": ACTIVITY,
        "job": JOB,
        "task_status": TASK,
    }
)

RESULT_KINDS: Mapping[str, tuple[str, str | None]] = MappingProxyType(
    {
        "help_show": (_VALUE, None),
        "status_show": (_MAPPING, None),
        "package_list": (_VALUE_LIST, None),
        "current_package_list_with_resources": (_RECORD_LIST, "package"),
        "package_show": (_RECORD, "package"),
        "package_search": (_RECORD_LIST, "package"),
        "package_autocomplete": (_RECORD_LIST, "package"),
        "package_create": (_RECORD, "package"),
        "package_update": (_RECORD, "package"),
        "package_patch": (_RECORD, "package"),
        "package_revise": (_RECORD, "package"),
        "package_resource_reorder": (_MAPPING, None),
        "package_owner_org_update": (_MAPPING, None),
        "package_delete": (_RECORD, "package"),
        "dataset_purge": (_VALUE, None),
        "bulk_update_private": (_RECORD_LIST, "task_status"),
        "bulk_update_public": (_RECORD_LIST, "task_status"),
        "bulk_update_delete": (_RECORD_LIST, "task_status"),
        "package_collaborator_create": (_MAPPING, None),
        "package_collaborator_delete": (_VALUE, None),
        "package_collaborator_list": (_MAPPING, None),
        "package_collaborator_list_for_user": (_MAPPING, None),
        "resource_show": (_RECORD, "resource"),
        "resource_search": (_RECORD_LIST, "resource"),
        "resource_create": (_RECORD, "resource"),
        "resource_update": (_RECORD, "resource"),
        "resource_patch": (_RECORD, "resource"),
        "resource_delete": (_VALUE, None),
        "organization_list": (_VALUE_LIST, None),
        "organization_list_for_user": (_RECORD_LIST, "organization"),
        "organization_show": (_RECORD, "organization"),
        "organization_autocomplete": (_RECORD_LIST, "organization"),
        "organization_create": (_RECORD, "organization"),
        "organization_update": (_RECORD, "organization"),
        "organization_patch": (_RECORD, "organization"),
        "organization_delete": (_RECORD, "organization"),
        "organization_purge": (_VALUE, None),
        "organization_member_create": (_RECORD, "member"),
        "organization_member_delete": (_VALUE, None),
        "group_list": (_VALUE_LIST, None),
        "group_list_authz": (_VALUE_LIST, None),
        "group_show": (_RECORD, "group"),
        "group_package_show": (_RECORD_LIST, "package"),
        "group_autocomplete": (_RECORD_LIST, "group"),
        "group_create": (_RECORD, "group"),
        "group_update": (_RECORD, "group"),
        "group_patch": (_RECORD, "group"),
        "group_delete": (_RECORD, "group"),
        "group_purge": (_VALUE, None),
        "group_member_create": (_RECORD, "member"),
        "group_member_delete": (_VALUE, None),
        "member_create": (_RECORD, "member"),
        "member_delete": (_VALUE, None),
        "member_list": (_RECORD_LIST, "member"),
        "member_roles_list": (_VALUE_LIST, None),
        "user_list": (_RECORD_LIST, "user"),
        "user_show": (_RECORD, "user"),
        "user_autocomplete": (_RECORD_LIST, "user"),
        "user_create": (_RECORD, "user"),
        "user_invite": (_RECORD, "user"),
        "user_update": (_RECORD, "user"),
        "user_patch": (_RECORD, "user"),
        "user_delete": (_RECORD, "user"),
        "get_site_user": (_RECORD, "user"),
        "api_token_create": (_TOKEN_SECRET, None),
        "api_token_list": (_MAPPING, None),
        "api_token_revoke": (_VALUE, None),
        "tag_list": (_VALUE_LIST, None),
        "tag_show": (_RECORD, "tag"),
        "tag_search": (_MAPPING, None),
        "tag_autocomplete": (_RECORD_LIST, "tag"),
        "tag_create": (_RECORD, "tag"),
        "tag_delete": (_VALUE, None),
        "vocabulary_list": (_RECORD_LIST, "vocabulary"),
        "vocabulary_show": (_RECORD, "vocabulary"),
        "vocabulary_create": (_RECORD, "vocabulary"),
        "vocabulary_update": (_RECORD, "vocabulary"),
        "vocabulary_delete": (_VALUE, None),
        "license_list": (_MAPPING, None),
        "format_autocomplete": (_VALUE_LIST, None),
        "term_translation_show": (_MAPPING, None),
        "term_translation_update": (_VALUE, None),
        "term_translation_update_many": (_MAPPING, None),
        "package_relationships_list": (_MAPPING, None),
        "package_relationship_create": (_MAPPING, None),
        "package_relationship_update": (_MAPPING, None),
        "package_relationship_delete": (_VALUE, None),
        "follow_dataset": (_MAPPING, None),
        "unfollow_dataset": (_MAPPING, None),
        "am_following_dataset": (_VALUE, None),
        "follow_group": (_MAPPING, None),
        "unfollow_group": (_MAPPING, None),
        "am_following_group": (_VALUE, None),
        "follow_user": (_MAPPING, None),
        "unfollow_user": (_MAPPING, None),
        "am_following_user": (_VALUE, None),
        "dataset_follower_count": (_VALUE, None),
        "dataset_follower_list": (_RECORD_LIST, "user"),
        "group_follower_count": (_VALUE, None),
        "group_follower_list": (_RECORD_LIST, "user"),
        "organization_follower_count": (_VALUE, None),
        "organization_follower_list": (_RECORD_LIST, "user"),
        "user_follower_count": (_VALUE, None),
        "user_follower_list": (_RECORD_LIST, "user"),
        "dataset_followee_count": (_VALUE, None),
        "dataset_followee_list": (_RECORD_LIST, "user"),
        "group_followee_count": (_VALUE, None),
        "group_followee_list": (_RECORD_LIST, "user"),
        "organization_followee_count": (_VALUE, None),
        "organization_followee_list": (_RECORD_LIST, "user"),
        "user_followee_count": (_VALUE, None),
        "user_followee_list": (_RECORD_LIST, "user"),
        "followee_count": (_VALUE, None),
        "followee_list": (_RECORD_LIST, "user"),
        "activity_show": (_RECORD, "activity"),
        "activity_data_show": (_MAPPING, None),
        "activity_diff": (_MAPPING, None),
        "activity_create": (_RECORD, "activity"),
        "package_activity_list": (_RECORD_LIST, "activity"),
        "group_activity_list": (_RECORD_LIST, "activity"),
        "organization_activity_list": (_RECORD_LIST, "activity"),
        "user_activity_list": (_RECORD_LIST, "activity"),
        "recently_changed_packages_activity_list": (_RECORD_LIST, "activity"),
        "dashboard_activity_list": (_RECORD_LIST, "activity"),
        "dashboard_new_activities_count": (_VALUE, None),
        "dashboard_mark_activities_old": (_VALUE, None),
        "send_email_notifications": (_VALUE, None),
        "resource_view_create": (_MAPPING, None),
        "resource_view_show": (_MAPPING, None),
        "resource_view_list": (_MAPPING, None),
        "resource_view_update": (_MAPPING, None),
        "resource_view_reorder": (_MAPPING, None),
        "resource_view_delete": (_VALUE, None),
        "resource_view_clear": (_VALUE, None),
        "package_create_default_resource_views": (_MAPPING, None),
        "resource_create_default_resource_views": (_MAPPING, None),
        "datastore_search": (_MAPPING, None),
        "datastore_info": (_MAPPING, None),
        "datastore_create": (_MAPPING, None),
        "datastore_upsert": (_MAPPING, None),
        "datastore_delete": (_MAPPING, None),
        "datastore_records_delete": (_MAPPING, None),
        "datastore_function_create": (_MAPPING, None),
        "datastore_function_delete": (_VALUE, None),
        "datastore_run_triggers": (_MAPPING, None),
        "datastore_search_sql": (_MAPPING, None),
        "job_list": (_RECORD_LIST, "job"),
        "job_show": (_RECORD, "job"),
        "job_cancel": (_VALUE, None),
        "job_clear": (_VALUE, None),
        "task_status_show": (_RECORD, "task_status"),
        "task_status_update": (_RECORD, "task_status"),
        "task_status_update_many": (_MAPPING, None),
        "task_status_delete": (_VALUE, None),
        "config_option_show": (_VALUE, None),
        "config_option_list": (_VALUE_LIST, None),
        "config_option_update": (_MAPPING, None),
    }
)

_WRAPPER_KEYS = ("results", "records")
_COUNT_KEYS = ("count", "total")


def parse_action_envelope(
    payload: object,
    *,
    operation: str,
    platform: CatalogPlatform | str = PLATFORM,
) -> object:
    """Decode one CKAN Action API JSON body into its bare result value or a typed failure.

    Args:
        payload: The decoded JSON body of an Action API response.
        operation: The dispatching operation identifier for typed failures.
        platform: The platform identity carried by typed failures.

    Returns:
        The bare ``result`` value of a ``success:true`` envelope, verbatim.

    Raises:
        NativeCatalogError: If the payload is not an envelope-shaped JSON object.
        CatalogError: If the envelope reports ``success:false``.
    """
    if not isinstance(payload, Mapping):
        raise _invalid_response(operation, platform, "The deployment sent a non-object Action API payload.")
    success = payload.get("success")
    if type(success) is not bool:
        raise _invalid_response(operation, platform, "The deployment sent an envelope without a boolean success flag.")
    if success:
        return payload.get("result")
    error = payload.get("error")
    if not isinstance(error, Mapping):
        raise _invalid_response(operation, platform, "The deployment sent a failed envelope without an error object.")
    raise map_envelope_error(dict(error), operation=operation, platform=platform)


def shape_result_envelope(
    action: str,
    result: object,
    *,
    page_hint: PageInfo | None = None,
) -> ResultEnvelope[CKANResultItem]:
    """Shape one decoded CKAN result value into a typed result envelope.

    Args:
        action: The Action API action name whose result is being shaped.
        result: The bare result value from a successful envelope.
        page_hint: Pagination state supplied by the dispatching caller.

    Returns:
        A result envelope of record, value, mapping, or secret token items.

    Raises:
        NativeCatalogError: If the result contradicts the declared outcome shape.
    """
    spec = RESULT_KINDS.get(action)
    outcome, family = spec if spec is not None else _fallback_outcome(action)
    items: tuple[CKANResultItem, ...]
    if outcome == _TOKEN_SECRET:
        items = cast("tuple[CKANResultItem, ...]", (CKANTokenResult.from_token_result(result),))
    elif outcome == _MAPPING:
        items = _mapping_items(action, result)
    elif outcome == _VALUE:
        items = (_value_item(action, result),)
    elif outcome == _VALUE_LIST:
        items = tuple(_value_item(action, entry) for entry in _sequence(action, result))
    elif outcome == _RECORD:
        items = (_record_item(_object_payload(action, result), family),)
    else:
        items = tuple(_record_item(_object_payload(action, entry), family) for entry in _sequence(action, result))
    page = page_hint if page_hint is not None else _platform_page(result)
    return ResultEnvelope(items=items, page=page)


def _fallback_outcome(action: str) -> tuple[str, str | None]:
    if action.startswith("am_following_") or action.endswith(("_follower_count", "_followee_count")):
        return (_VALUE, None)
    if action.endswith(("_follower_list", "_followee_list")):
        return (_RECORD_LIST, "user")
    return (_MAPPING, None)


def _sequence(action: str, result: object) -> tuple[object, ...]:
    if isinstance(result, Mapping):
        for key in _WRAPPER_KEYS:
            nested = result.get(key)
            if isinstance(nested, list | tuple):
                return tuple(nested)
        raise _shaping_error(action, "The result object carried no results array for a list outcome.")
    if isinstance(result, list | tuple):
        return tuple(result)
    raise _shaping_error(action, "The result value was not an array for a list outcome.")


def _object_payload(action: str, entry: object) -> dict[str, object]:
    if not isinstance(entry, Mapping):
        raise _shaping_error(action, "The result carried a non-object where a record was expected.")
    return dict(entry)


def _record_item(payload: dict[str, object], family: str | None) -> CKANResultItem:
    kind = RECORD_KINDS.get(family) if family is not None else None
    identity = payload.get("id", payload.get("name"))
    if kind is not None and isinstance(identity, str) and identity:
        return NativeRecord(
            platform=PLATFORM,
            resource_kind=kind,
            id=CatalogId(PLATFORM, kind, identity),
            payload=dict(payload),
        )
    return MappingRecord(dict(payload))


def _value_item(action: str, value: object) -> CKANResultItem:
    if value is None or isinstance(value, str | bool) or type(value) is int:
        return ValueRecord(value)
    if type(value) is float and math.isfinite(value):
        return ValueRecord(value)
    if isinstance(value, Mapping):
        return MappingRecord(dict(value))
    raise _shaping_error(action, "The result value was neither scalar nor object.")


def _mapping_items(action: str, result: object) -> tuple[CKANResultItem, ...]:
    if isinstance(result, Mapping):
        return (MappingRecord(dict(result)),)
    if isinstance(result, list | tuple):
        return tuple(
            MappingRecord(dict(entry)) if isinstance(entry, Mapping) else _value_item(action, entry) for entry in result
        )
    raise _shaping_error(action, "The result value was neither object nor array for a mapping outcome.")


def _platform_page(result: object) -> PageInfo | None:
    if isinstance(result, Mapping):
        for key in _COUNT_KEYS:
            value = result.get(key)
            if type(value) is int and value >= 0:
                return PageInfo(total_items=value)
        return None
    if isinstance(result, list | tuple):
        return PageInfo(total_items=len(result))
    return None


def _shaping_error(action: str, detail: str) -> NativeCatalogError:
    return NativeCatalogError(detail, operation=f"ckan/{action}", platform=PLATFORM)


def _invalid_response(operation: str, platform: CatalogPlatform | str, detail: str) -> NativeCatalogError:
    return NativeCatalogError(detail, operation=operation, platform=platform)
