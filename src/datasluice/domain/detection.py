"""Detection models for evidence-based portal identification."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
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
    evidence: Sequence[DetectionEvidence] = field(default_factory=tuple)
    extra: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"confidence must be in [0.0, 1.0], got {self.confidence}")
        if not isinstance(self.evidence, tuple):
            object.__setattr__(self, "evidence", tuple(self.evidence))
        if not isinstance(self.extra, MappingProxyType):
            object.__setattr__(self, "extra", MappingProxyType(dict(self.extra)))
