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
from datasluice.errors.catalog import CatalogValidationError
from datasluice.exceptions import PortalError
from datasluice.runtime.capability import EffectiveCapabilityCache
from datasluice.runtime.transport.base import TransportFailure


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


class _FailingProbeRunner(_ProbeRunner):
    """Probe runner that raises injected failures for selected operations."""

    def __init__(
        self,
        responses: dict[OperationId, ProbeResponseClass],
        origin: str,
        failures: dict[OperationId, Exception],
    ) -> None:
        super().__init__(responses, origin)
        self._failures = failures

    def probe(self, operation_id: OperationId) -> ProbeEvidence:
        """Raise the injected failure or delegate to the bounded evidence seam."""
        failure = self._failures.get(operation_id)
        if failure is not None:
            self.calls.append(operation_id)
            raise failure
        return super().probe(operation_id)


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


def _declared_profile(platform: str) -> DeclaredCapabilityProfile:
    operations = (_operation(platform, "get"), _operation(platform, "list"))
    return DeclaredCapabilityProfile(
        profile_version="v1",
        schema_version="v1",
        platform_api_version="v1",
        official_source_uri="https://example.test/source",
        source_accessed_at=date(2026, 8, 20),
        fixture_fingerprint="fixture-v1",
        operations={operation.id: operation for operation in operations},
    )


def _cache(
    platform: str,
    response_classes: tuple[ProbeResponseClass, ProbeResponseClass],
    origin: str,
) -> tuple[EffectiveCapabilityCache, _ProbeRunner]:
    profile = _declared_profile(platform)
    runner = _ProbeRunner(
        dict(zip((operation.id for operation in profile.operations.values()), response_classes, strict=True)), origin
    )
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
        "127.0.0.1/catalog", engines, _Registry(("datasluice/ckan", "datasluice/udata", "datasluice/socrata"))
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

    result = detect("https://127.0.0.1", engines, _Registry(("datasluice/ckan",)))

    assert {row.check for row in result.evidence} == {"ckan/datasets.get", "ckan/datasets.list"}
    assert len(runners["ckan"].calls) == 2
    assert runners["udata"].calls == []
    assert runners["socrata"].calls == []


def test_former_name_claims_no_identity_and_runs_no_probes() -> None:
    """Former connector names remain inactive and cannot claim an identity."""
    engines, runners = _engines("https://127.0.0.1")
    former_name = "".join(("data", "gouv"))

    result = detect("https://127.0.0.1", engines, _Registry((former_name,)))

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
        detect("https://127.0.0.1", engines, _Registry(("datasluice/ckan",)))


def test_installed_connectors_without_engines_record_miss_evidence_rows() -> None:
    """Installed connectors lacking an engine get miss rows instead of silence."""
    engines, runners = _engines("https://127.0.0.1")

    result = detect(
        "https://127.0.0.1",
        {"datasluice/udata": engines["datasluice/udata"]},
        _Registry(("datasluice/ckan", "datasluice/udata", "datasluice/socrata")),
    )

    misses = {
        row.check: (row.matched, row.detail)
        for row in result.evidence
        if row.check in {"datasluice/ckan", "datasluice/socrata"}
    }
    assert misses == {
        "datasluice/ckan": (False, "no probe engine configured"),
        "datasluice/socrata": (False, "no probe engine configured"),
    }
    assert [row.check for row in result.evidence if row.check.startswith("udata/")] == [
        "udata/datasets.get",
        "udata/datasets.list",
    ]
    assert len(runners["udata"].calls) == 2
    assert runners["ckan"].calls == []
    assert runners["socrata"].calls == []
    assert result.portal_type == "udata"
    assert result.confidence == 1.0


def test_runner_less_cache_fails_fast_before_any_probing() -> None:
    """A cache without a probe runner raises a typed wiring error before dispatch."""
    engines, runners = _engines("https://127.0.0.1")
    wired_engines = {
        "datasluice/ckan": EffectiveCapabilityCache(_declared_profile("ckan")),
        "datasluice/udata": engines["datasluice/udata"],
    }

    with pytest.raises(CatalogValidationError, match="datasluice/ckan"):
        detect("https://127.0.0.1", wired_engines, _Registry(("datasluice/ckan", "datasluice/udata")))

    assert runners["udata"].calls == []


def test_detection_urls_must_be_sanitized_https_origins() -> None:
    """Plain HTTP and userinfo-bearing URLs are rejected before any probing."""
    engines, runners = _engines("https://127.0.0.1")

    with pytest.raises(ValueError, match="sanitized HTTPS origins"):
        detect("http://127.0.0.1", engines, _Registry(("datasluice/ckan",)))
    with pytest.raises(ValueError, match="sanitized HTTPS origins"):
        detect("https://user:secret@127.0.0.1", engines, _Registry(("datasluice/ckan",)))

    assert all(runner.calls == [] for runner in runners.values())


def test_origin_comparison_normalizes_hostname_case_and_default_ports() -> None:
    """Case and default-port differences between origins do not abort detection."""
    runner = _ProbeRunner(
        dict.fromkeys(_declared_profile("ckan").operations, ProbeResponseClass.SUCCESS),
        "https://CATALOG.Example.Test:443",
    )
    engine = EffectiveCapabilityCache(_declared_profile("ckan"), runner)

    result = detect("https://catalog.example.test", {"datasluice/ckan": engine}, _Registry(("datasluice/ckan",)))

    assert result.portal_type == "ckan"
    assert all(row.matched for row in result.evidence)


def test_one_flaky_probe_never_aborts_the_detection_run() -> None:
    """Typed probe failures become matched=False evidence instead of an abort."""
    profile = _declared_profile("ckan")
    get_op = next(iter(profile.operations))
    runner = _FailingProbeRunner(
        dict.fromkeys(profile.operations, ProbeResponseClass.SUCCESS),
        "https://127.0.0.1",
        {get_op: PortalError("probe exploded")},
    )
    engine = EffectiveCapabilityCache(profile, runner)
    socrata_engine, socrata_runner = _cache(
        "socrata", (ProbeResponseClass.FORBIDDEN, ProbeResponseClass.SUCCESS), "https://127.0.0.1"
    )

    result = detect(
        "https://127.0.0.1",
        {"datasluice/ckan": engine, "datasluice/socrata": socrata_engine},
        _Registry(("datasluice/ckan", "datasluice/socrata")),
    )

    by_check = {row.check: row for row in result.evidence}
    assert by_check["ckan/datasets.get"].matched is False
    assert "probe failed" in by_check["ckan/datasets.get"].detail
    assert by_check["ckan/datasets.list"].matched is True
    assert len(socrata_runner.calls) == 2
    assert result.portal_type == "ckan"
    assert result.confidence == 1.0


@pytest.mark.parametrize("failure", [OSError("socket gone"), TransportFailure("unreachable")])
def test_flaky_probe_containment_covers_transport_level_failures(failure: Exception) -> None:
    """OSError and TransportFailure are contained exactly like PortalError."""
    profile = _declared_profile("ckan")
    get_op = next(iter(profile.operations))
    runner = _FailingProbeRunner(
        dict.fromkeys(profile.operations, ProbeResponseClass.SUCCESS),
        "https://127.0.0.1",
        {get_op: failure},
    )
    engine = EffectiveCapabilityCache(profile, runner)

    result = detect("https://127.0.0.1", {"datasluice/ckan": engine}, _Registry(("datasluice/ckan",)))

    by_check = {row.check: row for row in result.evidence}
    assert by_check["ckan/datasets.get"].matched is False
    assert by_check["ckan/datasets.list"].matched is True


def test_resolve_without_evidence_fails_fast_with_typed_wiring_error() -> None:
    profile = _declared_profile("ckan")
    operation = next(iter(profile.operations))
    runner = _ProbeRunner(dict.fromkeys(profile.operations, ProbeResponseClass.SUCCESS), "https://127.0.0.1")
    engine = EffectiveCapabilityCache(profile, runner)
    engine.record_response(operation, ProbeResponseClass.SUCCESS)

    with pytest.raises(CatalogValidationError, match="no probe evidence"):
        detect("https://127.0.0.1", {"datasluice/ckan": engine}, _Registry(("datasluice/ckan",)))
