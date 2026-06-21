"""Dataset model representing a collection of related open-data resources."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datasluice.domain.license import License
    from datasluice.domain.organization import Organization
    from datasluice.domain.resource import Resource


@dataclass(frozen=True)
class Dataset:
    """A dataset is a logical grouping of one or more resources.

    Attributes:
        id: Portal-native dataset identifier.
        title: Human-readable dataset title.
        name: Machine-friendly slug or name.
        description: Longer free-text description (may contain Markdown/HTML).
        resources: List of downloadable resources within this dataset.
        organization: Publishing organization, if known.
        license: Default license for resources in this dataset.
        tags: Free-form tags or keywords.
        themes: Categorization themes or groups.
        language: ISO language code(s) for the data.
        created: ISO-8601 creation timestamp.
        modified: ISO-8601 last-modified timestamp.
        metadata_modified: ISO-8601 timestamp of last metadata change.
        url: Canonical URL to the dataset on the portal.
        extra: Portal-native fields not captured above.
    """

    id: str
    title: str | None = None
    name: str | None = None
    description: str | None = None
    resources: list[Resource] = field(default_factory=list)
    organization: Organization | None = None
    license: License | None = None
    tags: list[str] = field(default_factory=list)
    themes: list[str] = field(default_factory=list)
    language: list[str] = field(default_factory=list)
    created: str | None = None
    modified: str | None = None
    metadata_modified: str | None = None
    url: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)
