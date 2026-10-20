"""Shared URL-safe path-segment validation for uData connectors."""

from __future__ import annotations

from urllib.parse import quote

from datasluice.errors.catalog import CatalogValidationError

_FORBIDDEN_CHARACTERS = "/?#\"'"


def composite_target(*identifiers: str) -> str:
    """Encode each identifier before joining the confirmation target with colons."""
    return ":".join(quote(str(identifier), safe="") for identifier in identifiers)


def safe_segment(value: object) -> bool:
    """Return whether *value* is one non-empty URL-safe path segment."""
    return (
        isinstance(value, str)
        and bool(value)
        and value not in {".", ".."}
        and not any(character in _FORBIDDEN_CHARACTERS for character in value)
        and not any(ord(character) < 32 for character in value)
    )


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
    if not safe_segment(value):
        raise CatalogValidationError(
            f"{label} identifier for {operation} must be one URL-safe path segment.",
            operation=operation,
            platform="udata",
            safe_action="Pass a prior typed read identifier.",
        )
    return quote(value, safe="")


def paging(page: object, page_size: object, label: str) -> None:
    """Require a positive page and a bounded server page size."""
    if type(page) is not int or page < 1:
        raise ValueError(f"{label} page must be a positive integer.")
    if type(page_size) is not int or not 1 <= page_size <= 100:
        raise ValueError(f"{label} page_size must be an integer from 1 through 100.")
