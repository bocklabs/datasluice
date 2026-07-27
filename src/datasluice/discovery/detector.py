"""Evidence-based portal type detection (D-P5-15/16/17/18).

The detector probes well-known API endpoints through a caller-injected
transport and records every probe as a :class:`DetectionEvidence` row. The
caller decides what to do with the :class:`DetectionResult`; raising
:class:`PortalDetectionError` on failure is the session's job, not the
detector's (D-P5-20).
"""

from __future__ import annotations

import urllib.parse

from datasluice.discovery.fingerprints import PATH_FINGERPRINTS
from datasluice.domain.detection import DetectionEvidence, DetectionResult
from datasluice.exceptions import NotFoundError, PortalError
from datasluice.logging import get_logger
from datasluice.ports import Transport
from datasluice.runtime.plugin_manager import PluginManager

logger = get_logger("discovery")

#: Exception tuple caught per detection probe (D-P5-18, CONN-08).
#:
#: The legacy bare-``Exception`` swallow hid transport-layer defects
#: (T-05-08, Pitfall 6); this narrow tuple is the only acceptable catch.
#: ``NotFoundError`` is currently a ``PortalError`` subclass and dead for the
#: probe path (transports raise ``PortalError`` for 404) — it stays for
#: defence-in-depth in case a future transport surfaces it directly.
#:
#: Open Question OQ-1: callers that inject :class:`HttpxTransport` will see
#: ``httpx.ConnectError``/``httpx.TimeoutException`` PROPAGATE on connection
#: failure (those are NOT ``OSError``/``PortalError`` subclasses). The CLI
#: defaults to :class:`HttpClient` (urllib) where this is moot; Plan 05-04's
#: contract suite also uses :class:`HttpClient`. Translating httpx exceptions
#: in the transport is a future enhancement, out of Phase 5 scope.
_PROBE_EXCEPTIONS: tuple[type[BaseException], ...] = (NotFoundError, PortalError, OSError)


def _normalize_base_url(url: str) -> str:
    """Ensure *url* has a scheme and no trailing slash."""
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"
    parsed = urllib.parse.urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def detect(url: str, transport: Transport, plugin_manager: PluginManager) -> DetectionResult:
    """Probe *url* for every registered portal fingerprint (D-P5-15/16/17/18).

    Iterates :data:`PATH_FINGERPRINTS`, skipping any portal_type the
    *plugin_manager* does not list. Each probe is recorded as a
    :class:`DetectionEvidence` (hit OR miss); any single hit pins
    ``portal_type`` at confidence ``1.0`` (any-match semantics), zero hits
    yields ``portal_type=None`` and ``confidence=0.0``.

    Args:
        url: Root URL of the portal (e.g. ``"https://catalog.data.gov"``).
        transport: Caller-injected transport satisfying the
            :class:`~datasluice.ports.Transport` Protocol (D-P5-16). The
            detector NEVER constructs its own transport.
        plugin_manager: Caller-injected :class:`PluginManager`; only
            ``list_connectors()`` results are probed (D-P5-16).

    Returns:
        A :class:`DetectionResult` carrying the matched portal_type
        (or ``None``), confidence (1.0 on any hit, 0.0 otherwise), and the
        full evidence trail.

    Raises:
        Any exception not in :data:`_PROBE_EXCEPTIONS` propagates — the
        detector never silences unexpected defects (CONN-08).
    """
    normalized = _normalize_base_url(url)
    registered = set(plugin_manager.list_connectors())
    evidence: list[DetectionEvidence] = []
    matched_portal: str | None = None
    for path, portal_type in PATH_FINGERPRINTS.items():
        if portal_type not in registered:
            continue
        probe_url = f"{normalized}{path}"
        try:
            transport.request(probe_url)
        except _PROBE_EXCEPTIONS as exc:
            logger.debug("probe %s for %s missed: %r", probe_url, portal_type, exc)
            evidence.append(DetectionEvidence(check=path, matched=False, detail=str(exc)))
            continue
        evidence.append(DetectionEvidence(check=path, matched=True, detail=probe_url))
        if matched_portal is None:
            matched_portal = portal_type
    confidence = 1.0 if matched_portal is not None else 0.0
    return DetectionResult(portal_type=matched_portal, confidence=confidence, evidence=evidence)
