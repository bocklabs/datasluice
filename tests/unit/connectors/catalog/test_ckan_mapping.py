"""Unit coverage for CKAN wire envelope decoding and result shaping."""

from __future__ import annotations

from typing import cast

import pytest

from datasluice.connectors.catalog.ckan.errors import map_envelope_error
from datasluice.connectors.catalog.ckan.mapping import (
    GROUP,
    MEMBER,
    RECORD_KINDS,
    RESULT_KINDS,
    TAG,
    VOCABULARY,
    parse_action_envelope,
    shape_result_envelope,
)
from datasluice.connectors.catalog.ckan.results import CKANTokenResult
from datasluice.domain.catalog.ids import CatalogPlatform, ResourceKind
from datasluice.domain.catalog.models import MappingRecord, NativeRecord, ValueRecord
from datasluice.errors.catalog import (
    CatalogConflictError,
    CatalogNotFoundError,
    CatalogUnavailableError,
    CatalogValidationError,
    ForbiddenError,
    NativeCatalogError,
    UnauthenticatedError,
)

_OPERATION = "ckan/action-api-v3.dataset-list-show-search"
_UNIQUE_PAYLOAD_TOKEN = "raw-envelope-secret-9f8e7d6c5b4a"
_TOKEN_LITERAL = "ds-token-abcdef0123456789"


@pytest.mark.parametrize(
    ("result",),
    [
        ({"id": "weather"},),
        (["a", "b"],),
        (17,),
        (None,),
    ],
)
def test_parse_action_envelope_returns_the_success_result_verbatim(result: object) -> None:
    assert parse_action_envelope({"success": True, "result": result}, operation=_OPERATION) == result


def test_plain_authorization_errors_map_to_unauthenticated() -> None:
    payload = {"success": False, "error": {"__type": "Authorization Error", "message": "Access denied"}}

    with pytest.raises(UnauthenticatedError) as raised:
        parse_action_envelope(payload, operation=_OPERATION)

    assert raised.value.operation == _OPERATION
    assert raised.value.platform == "ckan"
    assert raised.value.capability_state == "unauthorized"


def test_forbidden_http_context_maps_to_a_distinguishable_forbidden_error() -> None:
    error = {"__type": "Authorization Error", "message": "Access denied"}

    mapped = map_envelope_error(error, operation=_OPERATION, platform=CatalogPlatform.CKAN, status_code=403)

    assert isinstance(mapped, ForbiddenError)
    assert mapped.capability_state == "forbidden"
    assert mapped.safe_action != ""


def test_access_denied_flavor_for_an_authenticated_caller_maps_to_forbidden() -> None:
    error = {
        "__type": "Authorization Error",
        "message": "Unauthorized to update dataset weather. User bob not authorized.",
    }

    mapped = map_envelope_error(error, operation=_OPERATION, platform=CatalogPlatform.CKAN)

    assert isinstance(mapped, ForbiddenError)
    assert mapped.capability_state == "forbidden"


@pytest.mark.parametrize(
    ("envelope_type", "expected"),
    [
        ("Not Found Error", CatalogNotFoundError),
        ("Validation Error", CatalogValidationError),
        ("Conflict", CatalogConflictError),
        ("Conflict Error", CatalogConflictError),
        ("Some Novel Failure", CatalogUnavailableError),
    ],
)
def test_envelope_types_map_to_typed_catalog_errors(envelope_type: str, expected: type[Exception]) -> None:
    mapped = map_envelope_error({"__type": envelope_type}, operation=_OPERATION, platform="ckan")

    assert isinstance(mapped, expected)


def test_mapped_errors_never_expose_the_raw_payload_or_the_marker_key() -> None:
    error = {
        "__type": "Validation Error",
        "message": "field rejection",
        "nested": {"deep_token": _UNIQUE_PAYLOAD_TOKEN},
    }

    with pytest.raises(CatalogValidationError) as raised:
        parse_action_envelope({"success": False, "error": error}, operation=_OPERATION)

    rendered = str(raised.value)
    assert _UNIQUE_PAYLOAD_TOKEN not in rendered
    assert "__type" not in rendered
    metadata = raised.value.metadata
    assert "__type" not in metadata
    assert _UNIQUE_PAYLOAD_TOKEN not in repr(metadata)


@pytest.mark.parametrize(
    ("payload",),
    [
        ([],),
        ("nope",),
        (42,),
        ({"result": 1},),
        ({"success": "yes"},),
        ({"success": False},),
        ({"success": False, "error": "boom"},),
    ],
)
def test_malformed_payloads_raise_a_bounded_native_catalog_error(payload: object) -> None:
    with pytest.raises(NativeCatalogError):
        parse_action_envelope(payload, operation=_OPERATION)


@pytest.mark.parametrize(
    ("error_dict", "status_code"),
    [
        ({"__type": "Authorization Error", "message": "Access denied"}, None),
        ({"__type": "Not Found Error", "message": "missing"}, None),
        ({"__type": "Validation Error"}, None),
        ({"__type": "Conflict"}, None),
        ({"__type": "Mystery Failure"}, 500),
    ],
)
def test_every_mapped_error_carries_operation_platform_and_safe_action(
    error_dict: dict[str, object], status_code: int | None
) -> None:
    error = map_envelope_error(error_dict, operation=_OPERATION, platform="ckan", status_code=status_code)

    assert error.operation == _OPERATION
    assert error.platform == "ckan"
    assert isinstance(error.safe_action, str) and error.safe_action


def test_package_show_shapes_to_a_lossless_dataset_native_record() -> None:
    payload = {
        "id": "weather-123",
        "name": "weather",
        "title": "Weather records",
        "revision": "legacy-server-sent-key",
    }

    envelope = shape_result_envelope("package_show", payload)

    item = envelope.items[0]
    assert isinstance(item, NativeRecord)
    assert item.platform.value == "ckan"
    assert item.resource_kind == ResourceKind.DATASET
    assert item.id.value == "weather-123"
    assert set(item.payload.keys()) == set(payload.keys())
    assert dict(item.payload) == payload


def test_package_list_shapes_scalars_with_page_total_from_the_list_length() -> None:
    envelope = shape_result_envelope("package_list", ["a", "b"])

    assert all(isinstance(item, ValueRecord) for item in envelope.items)
    assert [item.value for item in envelope.items if isinstance(item, ValueRecord)] == ["a", "b"]
    assert envelope.page is not None and envelope.page.total_items == 2


def test_status_show_shapes_to_a_single_lossless_mapping_record() -> None:
    payload = {"site_title": "Portal", "extensions": ["datastore"], "ckan_version": "2.11.5"}

    envelope = shape_result_envelope("status_show", payload)

    item = envelope.items[0]
    assert isinstance(item, MappingRecord)
    assert item.to_dict() == {"schema_version": 1, "kind": "mapping_record", "payload": payload}


@pytest.mark.parametrize(
    ("action", "expected_kind"),
    [
        ("organization_show", ResourceKind.ORGANIZATION),
        ("group_show", GROUP),
        ("tag_show", TAG),
        ("vocabulary_show", VOCABULARY),
    ],
)
def test_record_families_use_connector_declared_resource_kinds(action: str, expected_kind: ResourceKind) -> None:
    payload = {"id": "identity-1", "name": "named", "display_name": "Named"}

    envelope = shape_result_envelope(action, payload)

    item = envelope.items[0]
    assert isinstance(item, NativeRecord)
    assert item.resource_kind == expected_kind
    assert item.id.resource_kind == expected_kind


def test_follower_counts_and_am_following_shape_to_single_value_envelopes() -> None:
    counted = shape_result_envelope("dataset_follower_count", 7)
    following = shape_result_envelope("am_following_dataset", False)

    assert isinstance(counted.items[0], ValueRecord) and counted.items[0].value == 7
    assert isinstance(following.items[0], ValueRecord) and following.items[0].value is False


def test_package_search_shapes_multi_records_with_the_platform_count_total() -> None:
    result = {"count": 2, "results": [{"id": "one"}, {"id": "two"}]}

    envelope = shape_result_envelope("package_search", result)

    assert len(envelope.items) == 2
    assert all(isinstance(item, NativeRecord) for item in envelope.items)
    assert envelope.page is not None and envelope.page.total_items == 2


def test_member_lists_shape_to_connector_member_records() -> None:
    result = [{"id": "member-1", "type": "user", "capacity": "admin"}]

    envelope = shape_result_envelope("member_list", result)

    item = envelope.items[0]
    assert isinstance(item, NativeRecord)
    assert item.resource_kind == MEMBER
    assert envelope.page is not None and envelope.page.total_items == 1


def test_api_token_create_shapes_to_a_secret_safe_token_result() -> None:
    envelope = shape_result_envelope("api_token_create", {"token": _TOKEN_LITERAL, "user_id": "u-1"})

    item = envelope.items[0]
    assert isinstance(item, CKANTokenResult)
    assert item.token.reveal() == _TOKEN_LITERAL
    serialized = item.to_dict()
    assert serialized["token"] == "***"
    assert serialized["user_id"] == "u-1"
    assert _TOKEN_LITERAL not in str(serialized)
    assert _TOKEN_LITERAL not in repr(item)


def test_token_results_require_a_non_empty_token_value() -> None:
    with pytest.raises(ValueError):
        CKANTokenResult.from_token_result({"user_id": "u-1"})


def test_result_kind_tables_are_explicit_and_frozen() -> None:
    assert "package_show" in RESULT_KINDS
    assert RECORD_KINDS["package"] == ResourceKind.DATASET
    mutable_records: dict[str, object] = cast("dict[str, object]", RECORD_KINDS)
    mutable_results: dict[str, object] = cast("dict[str, object]", RESULT_KINDS)
    with pytest.raises(TypeError):
        mutable_records["intruder"] = ResourceKind.DATASET
    with pytest.raises(TypeError):
        mutable_results["intruder"] = ("record", None)


def test_unknown_actions_shape_losslessly_through_the_mapping_fallback() -> None:
    envelope = shape_result_envelope("some_brand_new_extension_action", {"anything": ["goes", 1]})

    item = envelope.items[0]
    assert isinstance(item, MappingRecord)
    assert item.to_dict()["payload"] == {"anything": ["goes", 1]}
