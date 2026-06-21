"""Query model for searching datasets across portals."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Query:
    """Portal-agnostic search parameters.

    Attributes:
        text: Free-text search query.
        tags: Filter by one or more tags.
        organizations: Filter by organization name(s).
        groups: Filter by group or theme name(s).
        res_format: Filter by resource format (e.g. ``"CSV"``).
        license_id: Filter by license identifier.
        sort: Sort field and direction (e.g. ``"metadata_modified desc"``).
        limit: Maximum number of results to return.
        offset: Number of results to skip (for pagination).
    """

    text: str | None = None
    tags: list[str] = field(default_factory=list)
    organizations: list[str] = field(default_factory=list)
    groups: list[str] = field(default_factory=list)
    res_format: str | None = None
    license_id: str | None = None
    sort: str | None = None
    limit: int = 100
    offset: int = 0
