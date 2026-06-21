"""CKAN-specific error mapping."""

from __future__ import annotations

from datasluice.exceptions import NotFoundError, PortalError, RateLimitError


def map_ckan_error(status_code: int, body: dict | str | None) -> PortalError:
    """Map a CKAN HTTP error response to a DataSluice exception.

    Args:
        status_code: HTTP status code from the response.
        body: Parsed JSON body or raw text.

    Returns:
        An appropriate :class:`datasluice.exceptions.PortalError` subclass.
    """
    message = "CKAN API error"
    if isinstance(body, dict):
        message = body.get("error", {}).get("message", body.get("error", message))
        if isinstance(message, dict):
            message = str(message)
    elif body:
        message = str(body)

    if status_code == 404:
        return NotFoundError(f"CKAN resource not found: {message}")
    if status_code == 429:
        return RateLimitError(f"CKAN rate limit exceeded: {message}")
    return PortalError(f"CKAN error ({status_code}): {message}")
