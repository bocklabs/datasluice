"""Portal detector port Protocol."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from datasluice.domain import DetectionResult


@runtime_checkable
class PortalDetector(Protocol):
    """Detection seam protocol returning evidence-based portal identification."""

    def detect(self, url: str) -> DetectionResult: ...
