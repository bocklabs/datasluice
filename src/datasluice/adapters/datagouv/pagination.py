"""Pagination helpers for the data.gouv.fr (udata) API.

udata uses page-number / page-size pagination.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class DataGouvPage:
    """Parameters for a single udata results page (1-based)."""

    page: int
    page_size: int

    def next_page(self) -> DataGouvPage:
        """Return the parameters for the following page."""
        return DataGouvPage(page=self.page + 1, page_size=self.page_size)

    def to_params(self) -> dict[str, int]:
        """Convert to query-string parameters for the udata API."""
        return {"page": self.page, "page_size": self.page_size}
