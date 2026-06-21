"""Result container for paginated search responses."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datasluice.domain.dataset import Dataset


@dataclass
class SearchResult:
    """A paginated page of search results.

    Attributes:
        datasets: Datasets returned in this page.
        total: Total number of matching datasets across all pages.
        page: Current page number (1-based).
        page_size: Number of results per page.
        has_next: Whether additional pages are available.
    """

    datasets: list[Dataset] = field(default_factory=list)
    total: int = 0
    page: int = 1
    page_size: int = 100
    has_next: bool = False

    def __iter__(self) -> Iterator[Dataset]:
        return iter(self.datasets)

    def __len__(self) -> int:
        return len(self.datasets)
