"""Generic pagination helpers shared across adapters."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass


@dataclass(frozen=True)
class PaginationConfig:
    """Common pagination parameters.

    Attributes:
        page_size: Number of items per page.
        max_pages: Hard limit on the number of pages to fetch.
    """

    page_size: int = 100
    max_pages: int | None = None


def paginate[T](
    fetch_page: Callable[[int, int], tuple[list[T], bool]],
    *,
    page_size: int = 100,
    max_pages: int | None = None,
) -> Iterator[list[T]]:
    """Lazily yield pages of results.

    Args:
        fetch_page: Callable taking ``(page_number, page_size)`` and returning
            ``(items, has_next)``.
        page_size: Number of items to request per page.
        max_pages: Optional cap on the total number of pages.

    Yields:
        Lists of items, one per page.
    """
    page = 1
    pages_fetched = 0
    while True:
        items, has_next = fetch_page(page, page_size)
        yield items
        pages_fetched += 1
        if not has_next or not items:
            break
        if max_pages is not None and pages_fetched >= max_pages:
            break
        page += 1
