"""Organization model representing a dataset publisher."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Organization:
    """An organization or publisher of open-data datasets.

    Attributes:
        id: Portal-native organization identifier.
        name: Display name of the organization.
        title: Alternative human-readable title.
        description: Longer description, if available.
        url: URL to the organization's page on the portal.
        logo_url: URL to the organization's logo image.
        created: ISO-8601 creation timestamp, if available.
        extra: Portal-native fields not captured above.
    """

    id: str
    name: str | None = None
    title: str | None = None
    description: str | None = None
    url: str | None = None
    logo_url: str | None = None
    created: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)
