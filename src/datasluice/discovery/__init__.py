"""Portal type discovery and auto-detection."""

from datasluice.discovery.detector import detect
from datasluice.discovery.fingerprints import HTML_FINGERPRINTS, PATH_FINGERPRINTS
from datasluice.discovery.portal_metadata import PortalMetadata
from datasluice.domain.detection import DetectionEvidence, DetectionResult

__all__ = [
    "DetectionEvidence",
    "DetectionResult",
    "HTML_FINGERPRINTS",
    "PATH_FINGERPRINTS",
    "PortalMetadata",
    "detect",
]
