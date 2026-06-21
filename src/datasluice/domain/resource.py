"""Resource model representing a single downloadable file within a dataset."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datasluice.domain.license import License

# Known format aliases normalised to canonical uppercase form.
_FORMAT_ALIASES: dict[str, str] = {
    "text/csv": "CSV",
    "application/json": "JSON",
    "application/vnd.ms-excel": "XLS",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "XLSX",
    "application/parquet": "PARQUET",
    "application/geo+json": "GEOJSON",
    "application/xml": "XML",
    "application/pdf": "PDF",
}


@dataclass(frozen=True)
class Resource:
    """A single downloadable resource (file) within a dataset.

    Attributes:
        id: Portal-native resource identifier.
        name: Human-readable resource name or title.
        url: Direct download URL.
        format: Canonical file format (e.g. ``"CSV"``, ``"JSON"``).
        media_type: IANA media type if known (e.g. ``"text/csv"``).
        description: Optional longer description.
        size: File size in bytes, if known.
        license: License under which this resource is published.
        created: ISO-8601 creation timestamp, if available.
        modified: ISO-8601 last-modified timestamp, if available.
        extra: Portal-native fields not captured above.
    """

    id: str
    name: str | None = None
    url: str | None = None
    format: str | None = None
    media_type: str | None = None
    description: str | None = None
    size: int | None = None
    license: License | None = None
    created: str | None = None
    modified: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def normalize_format(cls, raw: str | None) -> str | None:
        """Normalise a raw format string or media type to canonical form."""
        if raw is None:
            return None
        return _FORMAT_ALIASES.get(raw.lower(), raw.upper().strip())
