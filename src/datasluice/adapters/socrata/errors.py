"""Socrata-specific error mapping."""

from __future__ import annotations

from datasluice.exceptions import NotFoundError, PortalError, RateLimitError


def map_socrata_error(status_code: int, body: dict | str | None) -> PortalError:
    """Map a Socrata HTTP error response to a DataSluice exception."""
    message = "Socrata API error"
    if isinstance(body, dict):
        message = body.get("message", body.get("error", message))
    elif body:
        message = str(body)

    if status_code == 404:
        return NotFoundError(f"Socrata resource not found: {message}")
    if status_code == 429:
        return RateLimitError(f"Socrata rate limit exceeded: {message}")
    return PortalError(f"Socrata error ({status_code}): {message}")
