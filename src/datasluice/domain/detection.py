"""Detection models for evidence-based portal identification."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class DetectionEvidence:
    """A single piece of evidence produced by a detection check.

    Attributes:
        check: Name of the detection check that produced this evidence.
        matched: Whether the check matched the candidate.
        detail: Free-text detail about the match or miss.
    """

    check: str
    matched: bool
    detail: str = ""


@dataclass(frozen=True)
class DetectionResult:
    """Outcome of portal auto-detection with confidence and evidence.

    Attributes:
        portal_type: Identified portal type, or ``None`` when undetected.
        confidence: Confidence score in the range ``[0.0, 1.0]``.
        evidence: Evidence records supporting the detection.
        extra: Portal-native detection fields not captured above.
    """

    portal_type: str | None
    confidence: float = 0.0
    evidence: list[DetectionEvidence] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)
