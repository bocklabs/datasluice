"""Shared stock uData RDF format vocabulary for the catalog wire surfaces."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Final

from datasluice.errors.catalog import CatalogValidationError

if TYPE_CHECKING:
    from collections.abc import Mapping

RDF_FORMAT_MEDIA_TYPES = {
    "rdf": "application/rdf+xml",
    "xml": "application/rdf+xml",
    "ttl": "application/x-turtle",
    "turtle": "application/x-turtle",
    "n3": "text/n3",
    "nt": "application/n-triples",
    "trig": "application/trig",
    "json": "application/ld+json",
    "jsonld": "application/ld+json",
}

_EXTENSION: Final = re.compile(r"[A-Za-z0-9_-]{1,16}")


def validate_format_extension(
    value: object,
    *,
    operation: str,
    platform: str = "udata",
    media_types: Mapping[str, str] = RDF_FORMAT_MEDIA_TYPES,
) -> str:
    """Validate one stock RDF format extension and return its lowercased form."""
    if not isinstance(value, str) or _EXTENSION.fullmatch(value) is None:
        raise CatalogValidationError(
            "The uData RDF format is invalid.",
            operation=operation,
            platform=platform,
            safe_action="Use a short ASCII uData RDF extension.",
        )
    extension = value.lower()
    if extension not in media_types:
        raise CatalogValidationError(
            "The uData RDF format is unsupported.",
            operation=operation,
            platform=platform,
            safe_action="Use a stock uData RDF extension.",
        )
    return extension
