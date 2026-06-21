"""Portal type discovery and auto-detection."""

from datasluice.discovery.detector import detect_portal, detect_portal_type
from datasluice.discovery.fingerprints import HTML_FINGERPRINTS, PATH_FINGERPRINTS
from datasluice.discovery.portal_metadata import PortalMetadata

__all__ = [
    "detect_portal_type",
    "detect_portal",
    "PATH_FINGERPRINTS",
    "HTML_FINGERPRINTS",
    "PortalMetadata",
]
