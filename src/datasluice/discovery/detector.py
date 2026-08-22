"""Evidence-based detection through injected capability probes."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol
from urllib.parse import urlsplit

from datasluice.domain.catalog.profiles import ProbeEvidence, ProbeResponseClass
from datasluice.domain.detection import DetectionEvidence, DetectionResult
from datasluice.errors.catalog import CatalogValidationError
from datasluice.exceptions import PortalError
from datasluice.runtime.capability import EffectiveCapabilityCache
from datasluice.runtime.transport.base import TransportFailure

_CANONICAL_CONNECTORS = (
    ("datasluice/ckan", "ckan"),
    ("datasluice/udata", "udata"),
    ("datasluice/socrata", "socrata"),
)

_DEFAULT_PORTS = frozenset({80, 443})


class SupportsListConnectors(Protocol):
    """Structural view of the entry-point registry consumed by detection."""

    def list_connectors(self) -> list[str]:
        """Return installed connector IDs without constructing plugins."""


def _origin(url: str) -> str:
    """Return the sanitized lowercase HTTPS origin used for origin equality."""
    try:
        parsed = urlsplit(url)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError as exc:
        raise ValueError("Detection URLs must be sanitized HTTPS origins.") from exc
    if parsed.scheme != "https" or not hostname or parsed.username or parsed.password:
        raise ValueError("Detection URLs must be sanitized HTTPS origins.")
    suffix = "" if port is None or port in _DEFAULT_PORTS else f":{port}"
    return f"https://{hostname}{suffix}"


def _normalize_base_url(url: str) -> str:
    """Ensure *url* has an HTTPS scheme and is reduced to its normalized origin."""
    candidate = url if url.startswith(("http://", "https://")) else f"https://{url}"
    return _origin(candidate)


def _require_probe_runners(installed: frozenset[str], probe_engines: Mapping[str, EffectiveCapabilityCache]) -> None:
    """Fail fast when an installed connector could never produce probe evidence."""
    for connector_id, platform in _CANONICAL_CONNECTORS:
        engine = probe_engines.get(connector_id)
        if connector_id not in installed or engine is None:
            continue
        if engine.probe_runner is None:
            raise CatalogValidationError(
                f"The capability cache wired for {connector_id} has no synchronous probe runner.",
                operation=f"{connector_id}/capability.resolve",
                platform=platform,
                capability_state="missing-probe-runner",
                safe_action=(
                    "Wire an EffectiveCapabilityCache built with a probe runner for every installed connector."
                ),
            )


def detect(
    url: str,
    probe_engines: Mapping[str, EffectiveCapabilityCache],
    plugin_manager: SupportsListConnectors,
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

    Raises:
        CatalogValidationError: If an installed connector's wired cache has no
            synchronous probe runner and could never produce evidence, or if a
            probed operation completes without probe evidence.
        ValueError: If the detection URL is not a sanitized HTTPS origin, or if
            probe evidence targets a deployment URL on a foreign origin.
    """
    normalized_origin = _normalize_base_url(url)
    installed = frozenset(plugin_manager.list_connectors())
    _require_probe_runners(installed, probe_engines)
    evidence_rows: list[DetectionEvidence] = []
    matched_platform: str | None = None

    for connector_id, platform in _CANONICAL_CONNECTORS:
        if connector_id not in installed:
            continue
        engine = probe_engines.get(connector_id)
        if engine is None:
            evidence_rows.append(
                DetectionEvidence(check=connector_id, matched=False, detail="no probe engine configured")
            )
            continue
        for operation_id in engine.baseline_profile.declared_profile.operations:
            try:
                effective = engine.resolve(operation_id)
                probe_evidence = effective.for_operation(operation_id).evidence
            except (PortalError, OSError, TransportFailure) as exc:
                evidence_rows.append(
                    DetectionEvidence(check=str(operation_id), matched=False, detail=f"probe failed: {exc}")
                )
                continue
            if probe_evidence is None:
                raise CatalogValidationError(
                    f"The capability cache wired for {connector_id} produced no probe evidence for {operation_id}.",
                    operation=str(operation_id),
                    platform=platform,
                    capability_state="missing-probe-evidence",
                    safe_action=(
                        "Wire an EffectiveCapabilityCache built with a probe runner for every installed connector."
                    ),
                )
            matched = probe_evidence.observed_response_class is ProbeResponseClass.SUCCESS
            evidence_rows.append(_detection_evidence(probe_evidence, normalized_origin, matched))
            if matched and matched_platform is None:
                matched_platform = platform

    return DetectionResult(
        portal_type=matched_platform,
        confidence=1.0 if matched_platform is not None else 0.0,
        evidence=evidence_rows,
    )


def _detection_evidence(probe_evidence: ProbeEvidence, normalized_origin: str, matched: bool) -> DetectionEvidence:
    """Convert bounded runtime evidence into the legacy-safe detection shape."""
    if _origin(probe_evidence.deployment_url) != normalized_origin:
        raise ValueError("Capability probe evidence must target the detection origin.")
    return DetectionEvidence(
        check=str(probe_evidence.operation_id),
        matched=matched,
        detail=f"{probe_evidence.observed_response_class.value} at {probe_evidence.deployment_url}",
    )
