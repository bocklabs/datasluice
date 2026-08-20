"""Evidence-based detection through injected capability probes."""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import urlsplit

from datasluice.domain.catalog.profiles import ProbeEvidence, ProbeResponseClass
from datasluice.domain.detection import DetectionEvidence, DetectionResult
from datasluice.runtime.capability import EffectiveCapabilityCache
from datasluice.runtime.plugin_manager import PluginManager

_CANONICAL_CONNECTORS = (
    ("datasluice/ckan", "ckan"),
    ("datasluice/udata", "udata"),
    ("datasluice/socrata", "socrata"),
)


def _normalize_base_url(url: str) -> str:
    """Ensure *url* has an HTTPS scheme and is reduced to its origin."""
    candidate = url if url.startswith(("http://", "https://")) else f"https://{url}"
    parsed = urlsplit(candidate)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
        raise ValueError("Detection URLs must be sanitized HTTPS origins.")
    return f"{parsed.scheme}://{parsed.netloc}"


def detect(
    url: str,
    probe_engines: Mapping[str, EffectiveCapabilityCache],
    plugin_manager: PluginManager,
) -> DetectionResult:
    """Probe installed canonical connector profiles and return their evidence.

    Args:
        url: Root URL represented by the caller-configured probe engines.
        probe_engines: Caller-injected capability caches keyed by canonical
            connector ID. Each cache owns its profile and probe runner.
        plugin_manager: Caller-injected entry-point registry used solely to
            determine which canonical built-ins are installed.

    Returns:
        A detection result containing one evidence row for every operation
        probed from installed canonical profiles.
    """
    normalized_url = _normalize_base_url(url)
    installed = frozenset(plugin_manager.list_connectors())
    evidence_rows: list[DetectionEvidence] = []
    matched_platform: str | None = None

    for connector_id, platform in _CANONICAL_CONNECTORS:
        if connector_id not in installed:
            continue
        engine = probe_engines.get(connector_id)
        if engine is None:
            continue
        for operation_id in engine.baseline_profile.declared_profile.operations:
            effective = engine.resolve(operation_id)
            probe_evidence = effective.for_operation(operation_id).evidence
            if probe_evidence is None:
                raise ValueError(f"Capability probe for {operation_id} did not produce evidence.")
            matched = probe_evidence.observed_response_class is ProbeResponseClass.SUCCESS
            evidence_rows.append(_detection_evidence(probe_evidence, normalized_url, matched))
            if matched and matched_platform is None:
                matched_platform = platform

    return DetectionResult(
        portal_type=matched_platform,
        confidence=1.0 if matched_platform is not None else 0.0,
        evidence=evidence_rows,
    )


def _detection_evidence(probe_evidence: ProbeEvidence, normalized_url: str, matched: bool) -> DetectionEvidence:
    """Convert bounded runtime evidence into the legacy-safe detection shape."""
    if urlsplit(probe_evidence.deployment_url).netloc != urlsplit(normalized_url).netloc:
        raise ValueError("Capability probe evidence must target the detection origin.")
    return DetectionEvidence(
        check=str(probe_evidence.operation_id),
        matched=matched,
        detail=f"{probe_evidence.observed_response_class.value} at {probe_evidence.deployment_url}",
    )
