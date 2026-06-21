"""Auto-detection of portal type from a URL.

The detector probes well-known API endpoints and inspects page signatures to
determine which platform powers a given portal.
"""

from __future__ import annotations

import urllib.parse

from datasluice.discovery.fingerprints import PATH_FINGERPRINTS
from datasluice.exceptions import PortalDetectionError
from datasluice.logging import get_logger

logger = get_logger("discovery")


def _normalize_base_url(url: str) -> str:
    """Ensure *url* has a scheme and no trailing slash."""
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"
    parsed = urllib.parse.urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def detect_portal_type(base_url: str) -> str:
    """Auto-detect the platform type for *base_url*.

    Probes common API endpoints and matches against known fingerprints.

    Args:
        base_url: Root URL of the portal (e.g. ``"https://catalog.data.gov"``).

    Returns:
        Canonical portal type name (e.g. ``"ckan"``).

    Raises:
        PortalDetectionError: If the portal type cannot be determined.
    """
    normalized = _normalize_base_url(base_url)

    from datasluice.adapters.registry import registry
    from datasluice.transport import HttpClient

    client = HttpClient()

    for path, portal_type in PATH_FINGERPRINTS.items():
        if not registry.has(portal_type):
            continue
        probe_url = f"{normalized}{path}"
        try:
            client.request(probe_url)
            logger.debug("Detected %s via %s", portal_type, probe_url)
            return portal_type
        except Exception:
            continue

    raise PortalDetectionError(f"Could not auto-detect portal type for {base_url!r}. Specify portal_type explicitly.")


def detect_portal(base_url: str) -> str:
    """Alias for :func:`detect_portal_type`."""
    return detect_portal_type(base_url)
