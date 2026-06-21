"""Pagination helpers for the CKAN Action API."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class CKANPage:
    """Parameters for a single CKAN search results page.

    CKAN uses ``start`` (offset) and ``rows`` (page size) for pagination.
    """

    start: int
    rows: int

    def next_page(self) -> CKANPage:
        """Return the parameters for the following page."""
        return CKANPage(start=self.start + self.rows, rows=self.rows)

    def to_params(self) -> dict[str, int]:
        """Convert to query-string parameters for the CKAN API."""
        return {"start": self.start, "rows": self.rows}
