"""Shared URL-safe path-segment validation for uData connectors."""

from __future__ import annotations

from urllib.parse import quote

from datasluice.errors.catalog import CatalogValidationError

_FORBIDDEN_CHARACTERS = "/?#\"'"


def path_segment(value: str, operation: str, label: str) -> str:
    """Return *value* quoted as one URL-safe path segment.

    Args:
        value: The caller-supplied identifier destined for a URL path segment.
        operation: The owning operation name, used to build the error details.
        label: The owning resource label used in the error message.

    Returns:
        The percent-encoded identifier.

    Raises:
        CatalogValidationError: If the identifier is not a single safe path segment.
    """
    if (
        not isinstance(value, str)
        or not value
        or value in {".", ".."}
        or any(character in _FORBIDDEN_CHARACTERS for character in value)
        or any(ord(character) < 32 for character in value)
    ):
        raise CatalogValidationError(
            f"{label} identifier for {operation} must be one URL-safe path segment.",
            operation=operation,
            platform="udata",
            safe_action="Pass a prior typed read identifier.",
        )
    return quote(value, safe="")
