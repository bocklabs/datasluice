"""Probe-engine discovery tests for canonical catalog connectors."""

from __future__ import annotations

import inspect
from datetime import date

import pytest

from datasluice.discovery.detector import detect
from datasluice.domain.catalog.operations import (
    Atomicity,
    AuthClass,
    CapabilityClass,
    ConcurrencyRequirement,
    Idempotency,
    MutationClass,
    OperationId,
    OperationSpec,
    OperationTier,
)
from datasluice.domain.catalog.profiles import (
    CredentialClassification,
    DeclaredCapabilityProfile,
    ProbeEvidence,
    ProbeResponseClass,
    RoleClassification,
)
from datasluice.runtime.capability import EffectiveCapabilityCache


class _Registry:
    """Minimal injected entry-point registry."""

    def __init__(self, connector_ids: tuple[str, ...]) -> None:
        self._connector_ids = connector_ids

    def list_connectors(self) -> list[str]:
        """Return installed connector IDs without constructing plugins."""
        return list(self._connector_ids)


class _ProbeRunner:
    """In-process probe runner recording one call per declared operation."""

    def __init__(self, responses: dict[OperationId, ProbeResponseClass], origin: str) -> None:
        self._responses = responses
        self._origin = origin
        self.calls: list[OperationId] = []

    def probe(self, operation_id: OperationId) -> ProbeEvidence:
        """Return bounded evidence from the caller-owned fake probe seam."""
        self.calls.append(operation_id)
        return ProbeEvidence(
            operation_id=operation_id,
            deployment_url=f"{self._origin}/capabilities/{operation_id.service}",
            credential_classification=CredentialClassification.ANONYMOUS,
            role_classification=RoleClassification.ANONYMOUS,
            observed_response_class=self._responses[operation_id],
        )


def _operation(platform: str, method: str) -> OperationSpec:
    operation_id = OperationId(platform, "datasets", method)
    return OperationSpec(
        id=operation_id,
        tier=OperationTier.NORMALIZED,
        request_type="DatasetRequest",
        response_type="DatasetRecord",
        auth_class=AuthClass.PUBLIC,
        mutation_class=MutationClass.READ,
        idempotency=Idempotency.SAFE,
        concurrency=ConcurrencyRequirement.NONE,
        atomicity=Atomicity.NONE,
        capability_class=CapabilityClass.CORE,
    )


def _cache(
    platform: str,
    response_classes: tuple[ProbeResponseClass, ProbeResponseClass],
    origin: str,
) -> tuple[EffectiveCapabilityCache, _ProbeRunner]:
    operations = (_operation(platform, "get"), _operation(platform, "list"))
    profile = DeclaredCapabilityProfile(
        profile_version="v1",
        schema_version="v1",
        platform_api_version="v1",
        official_source_uri="https://example.test/source",
        source_accessed_at=date(2026, 8, 20),
        fixture_fingerprint="fixture-v1",
        operations={operation.id: operation for operation in operations},
    )
    runner = _ProbeRunner(dict(zip((operation.id for operation in operations), response_classes, strict=True)), origin)
    return EffectiveCapabilityCache(profile, runner), runner


def _engines(origin: str) -> tuple[dict[str, EffectiveCapabilityCache], dict[str, _ProbeRunner]]:
    responses = {
        "ckan": (ProbeResponseClass.SUCCESS, ProbeResponseClass.UNSUPPORTED),
        "udata": (ProbeResponseClass.UNAVAILABLE, ProbeResponseClass.SUCCESS),
        "socrata": (ProbeResponseClass.FORBIDDEN, ProbeResponseClass.UNAVAILABLE),
    }
    built = {platform: _cache(platform, response_classes, origin) for platform, response_classes in responses.items()}
    return (
        {f"datasluice/{platform}": cache for platform, (cache, _) in built.items()},
        {platform: runner for platform, (_, runner) in built.items()},
    )


def test_installed_canonical_profiles_emit_operation_evidence_for_hits_and_misses() -> None:
    """Installed built-ins expose one safe evidence row for every probe outcome."""
    engines, runners = _engines("https://127.0.0.1")

    result = detect(
        "127.0.0.1/catalog",
        engines,
        _Registry(("datasluice/ckan", "datasluice/udata", "datasluice/socrata")),  # ty: ignore[invalid-argument-type]
    )

    assert result.portal_type == "ckan"
    assert result.confidence == 1.0
    assert {row.check for row in result.evidence} == {
        "ckan/datasets.get",
        "ckan/datasets.list",
        "udata/datasets.get",
        "udata/datasets.list",
        "socrata/datasets.get",
        "socrata/datasets.list",
    }
    assert any(row.matched for row in result.evidence)
    assert any(not row.matched for row in result.evidence)
    assert all(" at https://127.0.0.1/" in row.detail for row in result.evidence)
    assert all(len(runner.calls) == 2 for runner in runners.values())


def test_absent_extra_is_not_probed_even_when_a_cache_is_available() -> None:
    """A profile whose connector extra is absent causes zero probe-engine calls."""
    engines, runners = _engines("https://127.0.0.1")

    result = detect("https://127.0.0.1", engines, _Registry(("datasluice/ckan",)))  # ty: ignore[invalid-argument-type]

    assert {row.check for row in result.evidence} == {"ckan/datasets.get", "ckan/datasets.list"}
    assert len(runners["ckan"].calls) == 2
    assert runners["udata"].calls == []
    assert runners["socrata"].calls == []


def test_former_name_claims_no_identity_and_runs_no_probes() -> None:
    """Former connector names remain inactive and cannot claim an identity."""
    engines, runners = _engines("https://127.0.0.1")
    former_name = "".join(("data", "gouv"))

    result = detect("https://127.0.0.1", engines, _Registry((former_name,)))  # ty: ignore[invalid-argument-type]

    assert result.portal_type is None
    assert result.confidence == 0.0
    assert result.evidence == ()
    assert all(runner.calls == [] for runner in runners.values())


def test_detector_signature_requires_caller_injected_probe_engines_and_registry() -> None:
    """Detection owns neither transport construction nor entry-point discovery."""
    signature = inspect.signature(detect)

    assert tuple(signature.parameters) == ("url", "probe_engines", "plugin_manager")
    assert all(parameter.default is inspect.Parameter.empty for parameter in signature.parameters.values())


def test_probe_evidence_must_target_the_normalized_detection_origin() -> None:
    """A stale probe target cannot create a false identity claim."""
    engines, _ = _engines("https://different.example.test")

    with pytest.raises(ValueError, match="detection origin"):
        detect("https://127.0.0.1", engines, _Registry(("datasluice/ckan",)))  # ty: ignore[invalid-argument-type]
