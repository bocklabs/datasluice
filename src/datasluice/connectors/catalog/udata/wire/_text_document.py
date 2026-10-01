"""Shared bounded validation for non-JSON uData text documents."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.errors.catalog import NativeCatalogError

APPROVED_TEXT_MEDIA_TYPES = frozenset(
    {
        "application/atom+xml",
        "application/json",
        "application/ld+json",
        "application/n-triples",
        "application/rdf+xml",
        "application/trig",
        "application/x-turtle",
        "text/n3",
        "text/turtle",
        "text/xml",
    }
)


@dataclass(frozen=True, slots=True)
class BoundedDocument:
    """One validated text document reduced to its media type, size, and digest."""

    media_type: str
    size_bytes: int
    sha256: str

    def payload(self) -> dict[str, object]:
        """Return the no-raw-body projection retained for a native record."""
        return {"media_type": self.media_type, "size_bytes": self.size_bytes, "sha256": self.sha256}


def bound_text_document(
    body: object,
    media_type: str,
    *,
    response_media_type: str | None,
    operation: str,
    platform: CatalogPlatform | str,
    approved_media_types: frozenset[str] = APPROVED_TEXT_MEDIA_TYPES,
    subject: str = "",
    list_approved_action: bool = False,
) -> BoundedDocument:
    """Validate one buffered text document and reduce it to a bounded projection.

    ``subject`` qualifies the error messages for the owning service and is empty
    for documents that carry no service noun. ``list_approved_action`` adds the
    remediation hint listing the approved media types.
    """
    qualifier = f"{subject} " if subject else ""
    if not isinstance(body, bytes):
        raise NativeCatalogError(
            f"The uData {qualifier}document body must be buffered bytes.",
            operation=operation,
            platform=platform,
        )
    try:
        body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise NativeCatalogError(
            f"The uData {qualifier}text document is not valid UTF-8.",
            operation=operation,
            platform=platform,
        ) from exc
    negotiated = (response_media_type or media_type).split(";", 1)[0].strip().lower()
    if negotiated not in approved_media_types:
        raise NativeCatalogError(
            f"The uData {qualifier}document media type {negotiated!r} is not an approved text contract.",
            operation=operation,
            platform=platform,
            metadata={"safe_action": f"Request one of the approved media types: {media_type}."}
            if list_approved_action
            else None,
        )
    return BoundedDocument(
        media_type=negotiated,
        size_bytes=len(body),
        sha256=hashlib.sha256(body).hexdigest(),
    )
