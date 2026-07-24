"""Portal detector port Protocol."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from datasluice.domain import DetectionResult


@runtime_checkable
class PortalDetector(Protocol):
    """Detection seam protocol returning evidence-based portal identification."""

    def detect(self, url: str) -> DetectionResult: ...
