"""Pagination helpers for the Socrata SODA2 API.

Socrata uses offset / limit pagination on the SODA2 API.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SocrataPage:
    """Parameters for a single Socrata results page."""

    offset: int
    limit: int

    def next_page(self) -> SocrataPage:
        """Return the parameters for the following page."""
        return SocrataPage(offset=self.offset + self.limit, limit=self.limit)

    def to_params(self) -> dict[str, int]:
        """Convert to query-string parameters for the SODA2 API."""
        return {"$offset": self.offset, "$limit": self.limit}
