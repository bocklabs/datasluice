"""Unit coverage for CKAN wire envelope decoding and result shaping."""

from __future__ import annotations

import pytest

from datasluice.connectors.catalog.ckan.errors import map_envelope_error
from datasluice.connectors.catalog.ckan.mapping import parse_action_envelope
from datasluice.domain.catalog.ids import CatalogPlatform
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
