"""Artifact model — the materialized output reference (INTG-10)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Artifact:
    """A materialized artifact produced by the data plane.

    Attributes:
        uri: URI locating the artifact (local path or remote URI).
        media_type: IANA media type, or ``None`` when unknown.
        size: Size in bytes, or ``None`` when unknown.
        checksum: Content checksum (e.g. SHA-256 hex), or ``None``.
        metadata: Artifact metadata (provenance, encoding, and similar).
        extra: Portal-native artifact fields not captured above.
    """

    uri: str
    media_type: str | None = None
    size: int | None = None
    checksum: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)
