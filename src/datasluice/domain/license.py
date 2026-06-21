"""License model representing the license under which data is published."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class License:
    """A license under which an open-data resource or dataset is published.

    Attributes:
        id: Canonical license identifier (e.g. ``"CC-BY-4.0"``).
        title: Human-readable license name.
        url: URL to the full license text.
    """

    id: str
    title: str | None = None
    url: str | None = None
