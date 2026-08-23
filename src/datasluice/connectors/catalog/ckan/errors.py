"""Typed error mapping for CKAN Action API response envelopes."""

from __future__ import annotations

from collections.abc import Mapping

from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.redaction import redact_mapping
from datasluice.errors.catalog import (
    CatalogConflictError,
    CatalogError,
    CatalogNotFoundError,
    CatalogUnavailableError,
    CatalogValidationError,
    ForbiddenError,
    UnauthenticatedError,
)

ENVELOPE_MARKER_KEY = "__type"

_AUTHORIZATION_TYPE = "authorization error"
_NOT_FOUND_TYPE = "not found error"
_VALIDATION_TYPE = "validation error"
_CONFLICT_TYPES = frozenset({"conflict", "conflict error"})
_FORBIDDEN_MESSAGE_MARKERS = ("unauthorized to", "not authorized")
_SIZE_LIMIT_MARKERS = (
    "file size",
    "too large",
    "max_size",
    "max size",
    "maximum size",
    "size limit",
    "content length",
    "content-length",
    "max_request_size",
    "ckan.max_resource_size",
)
_UPLOAD_REMEDY = "Reduce the uploaded file size below the deployment limit and retry the action."


def map_envelope_error(
    error_dict: Mapping[str, object],
    *,
    operation: str,
    platform: CatalogPlatform | str,
    status_code: int | None = None,
) -> CatalogError:
    """Map one CKAN envelope error object to a typed catalog error without echoing its payload.

    Args:
        error_dict: The raw ``error`` object from a ``success:false`` envelope.
        operation: The dispatching operation identifier for the typed error.
        platform: The platform identity carried by the typed error.
        status_code: The HTTP status observed by the transport, when known.

    Returns:
        A typed catalog error whose message and bounded redacted metadata never
        contain the raw payload or the ``__type`` marker key.

    Raises:
        TypeError: If ``error_dict`` is not a JSON object.
    """
    if not isinstance(error_dict, Mapping):
        raise TypeError("CKAN envelope error mapping requires a JSON object.")
    redacted = redact_mapping(dict(error_dict))
    details = {key: value for key, value in redacted.items() if key != ENVELOPE_MARKER_KEY}
    marker = redacted.get(ENVELOPE_MARKER_KEY)
    envelope_type = marker.strip().lower() if isinstance(marker, str) else ""
    message = redacted.get("message")
    lowered = message.lower() if isinstance(message, str) else ""
    error: CatalogError
    if envelope_type == _AUTHORIZATION_TYPE:
        forbidden = status_code == 403 or any(part in lowered for part in _FORBIDDEN_MESSAGE_MARKERS)
        if forbidden:
            error = ForbiddenError(
                "The deployment denied this authenticated action.",
                operation=operation,
                platform=platform,
                capability_state="forbidden",
                safe_action="Use credentials with the required role or request the missing permission.",
                metadata=details,
            )
        else:
            error = UnauthenticatedError(
                "The deployment rejected the credentials supplied for this action.",
                operation=operation,
                platform=platform,
                capability_state="unauthorized",
                safe_action="Provide valid credentials and retry the operation.",
                metadata=details,
            )
    elif envelope_type == _NOT_FOUND_TYPE:
        error = CatalogNotFoundError(
            "The requested catalog target was not found.",
            operation=operation,
            platform=platform,
            safe_action="Confirm the target identifier against the deployment.",
            metadata=details,
        )
    elif envelope_type == _VALIDATION_TYPE:
        size_limited = any(marker in lowered for marker in _SIZE_LIMIT_MARKERS) or any(
            isinstance(key, str) and key.lower() in {"max_size", "size"} for key in redacted
        )
        error = CatalogValidationError(
            "The deployment reported validation failures for this action.",
            operation=operation,
            platform=platform,
            safe_action=(
                _UPLOAD_REMEDY
                if size_limited
                else "Correct the rejected fields according to the deployment validation rules."
            ),
            metadata=details,
        )
    elif envelope_type in _CONFLICT_TYPES:
        error = CatalogConflictError(
            "The deployment reported a conflicting catalog state.",
            operation=operation,
            platform=platform,
            safe_action="Refresh the target state and retry the operation.",
            metadata=details,
        )
    else:
        error = CatalogUnavailableError(
            "The deployment answered with an unrecognized failure.",
            operation=operation,
            platform=platform,
            capability_state="unavailable",
            safe_action="Retry after confirming the deployment health.",
            metadata=details,
        )
    return error
