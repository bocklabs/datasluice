"""Portal metadata describing known portal instances."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PortalMetadata:
    """Metadata about a detected or known portal.

    Attributes:
        portal_type: Canonical platform type (e.g. ``"ckan"``).
        base_url: Root URL of the portal.
        title: Portal title, if detected.
        description: Portal description, if detected.
        api_url: Resolved API base URL.
    """

    portal_type: str
    base_url: str
    title: str | None = None
    description: str | None = None
    api_url: str | None = None
