"""data.gouv.fr (udata) specific error mapping."""

from __future__ import annotations

from datasluice.exceptions import NotFoundError, PortalError, RateLimitError


def map_datagouv_error(status_code: int, body: dict | str | None) -> PortalError:
    """Map a udata HTTP error response to a DataSluice exception."""
    message = "data.gouv.fr API error"
    if isinstance(body, dict):
        message = body.get("message", body.get("error", message))
    elif body:
        message = str(body)

    if status_code == 404:
        return NotFoundError(f"data.gouv.fr resource not found: {message}")
    if status_code == 429:
        return RateLimitError(f"data.gouv.fr rate limit exceeded: {message}")
    return PortalError(f"data.gouv.fr error ({status_code}): {message}")
