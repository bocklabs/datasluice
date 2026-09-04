"""Versioned declared and effective capability profiles."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from types import MappingProxyType
from urllib.parse import urlsplit, urlunsplit

from datasluice.domain.catalog.operations import CapabilityClass, OperationId, OperationSpec


class CredentialClassification(StrEnum):
    """Credential level used for a capability probe."""

    ANONYMOUS = "anonymous"
    AUTHENTICATED = "authenticated"
    PRIVILEGED = "privileged"


class RoleClassification(StrEnum):
    """Effective role level observed during a capability probe."""

    ANONYMOUS = "anonymous"
    USER = "user"
    ADMIN = "admin"
    UNKNOWN = "unknown"


class ProbeResponseClass(StrEnum):
    """Bounded response result recorded without a response body."""

    SUCCESS = "success"
    UNSUPPORTED = "unsupported"
    UNAUTHORIZED = "unauthorized"
    FORBIDDEN = "forbidden"
    UNAVAILABLE = "unavailable"
    DEPLOYMENT_DISABLED = "deployment-disabled"


class EffectiveCapabilityState(StrEnum):
    """Operation-specific effective capability state."""

    CORE = "core"
    OPTIONAL = "optional"
    AUTHENTICATED = "authenticated"
    ADMIN = "admin"
    UNSUPPORTED = "unsupported"
    UNAUTHORIZED = "unauthorized"
    FORBIDDEN = "forbidden"
    UNAVAILABLE = "unavailable"
    DEPLOYMENT_DISABLED = "deployment-disabled"


class EvidenceProvenance(StrEnum):
    """Provenance of capability evidence relative to the pinned platform API line."""

    VERIFIED_LINE = "verified-line"
    UNVERIFIED = "unverified"


@dataclass(frozen=True, slots=True)
class ProbeEvidence:
    """Sanitized observation used to derive one operation's effective state."""

    operation_id: OperationId
    deployment_url: str
    credential_classification: CredentialClassification
    role_classification: RoleClassification
    observed_response_class: ProbeResponseClass
    provenance: EvidenceProvenance = EvidenceProvenance.VERIFIED_LINE
    credential_scope: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "credential_classification", CredentialClassification(self.credential_classification))
        object.__setattr__(self, "role_classification", RoleClassification(self.role_classification))
        object.__setattr__(self, "observed_response_class", ProbeResponseClass(self.observed_response_class))
        object.__setattr__(self, "provenance", EvidenceProvenance(self.provenance))
        object.__setattr__(self, "deployment_url", _sanitize_deployment_url(self.deployment_url))
        if self.credential_scope is not None and (
            not isinstance(self.credential_scope, str) or not self.credential_scope
        ):
            raise ValueError("Credential scope must be a non-empty string when supplied.")


@dataclass(frozen=True, slots=True)
class DeclaredCapabilityProfile:
    """Immutable reviewed declaration for a pinned platform API version."""

    profile_version: str
    schema_version: str
    platform_api_version: str
    official_source_uri: str
    source_accessed_at: date
    fixture_fingerprint: str
    operations: Mapping[OperationId, OperationSpec]

    def __post_init__(self) -> None:
        _require_text("profile version", self.profile_version)
        _require_text("schema version", self.schema_version)
        _require_text("platform API version", self.platform_api_version)
        _require_text("fixture fingerprint", self.fixture_fingerprint)
        source = urlsplit(self.official_source_uri)
        if source.scheme != "https" or not source.netloc or source.username or source.password:
            raise ValueError("Official source URI must be a sanitized HTTPS URI.")
        operation_map = dict(self.operations)
        if not operation_map:
            raise ValueError("Declared profiles cannot have missing operation IDs.")
        for operation_id, operation in operation_map.items():
            if operation_id != operation.id:
                raise ValueError(
                    "Declared profiles cannot contain duplicate operation IDs or mismatched operation keys."
                )
        object.__setattr__(self, "operations", MappingProxyType(operation_map))


@dataclass(frozen=True, slots=True)
class EffectiveOperationCapability:
    """Effective state and optional evidence for one declared operation."""

    operation: OperationSpec
    state: EffectiveCapabilityState
    evidence: ProbeEvidence | None = None


@dataclass(frozen=True, slots=True)
class GuardDecision:
    """Safe pre-dispatch result for a requested operation."""

    operation_id: OperationId
    state: EffectiveCapabilityState
    allowed: bool
    remedy: str | None


@dataclass(frozen=True, slots=True)
class EffectiveCapabilityProfile:
    """Immutable deployment-specific state derived from declared operations."""

    declared_profile: DeclaredCapabilityProfile
    capabilities: Mapping[OperationId, EffectiveOperationCapability]

    def __post_init__(self) -> None:
        capabilities = dict(self.capabilities)
        declared_ids = set(self.declared_profile.operations)
        if set(capabilities) != declared_ids:
            raise ValueError("Effective profiles must contain exactly the declared operation IDs.")
        for operation_id, capability in capabilities.items():
            if capability.operation.id != operation_id:
                raise ValueError("Effective capability operation IDs must match their map keys.")
        object.__setattr__(self, "capabilities", MappingProxyType(capabilities))

    @classmethod
    def derive(
        cls,
        declared_profile: DeclaredCapabilityProfile,
        evidence_records: Iterable[ProbeEvidence],
    ) -> EffectiveCapabilityProfile:
        """Derive operation-specific effective states from bounded probe evidence."""
        evidence_by_operation: dict[OperationId, ProbeEvidence] = {}
        for evidence in evidence_records:
            if evidence.operation_id not in declared_profile.operations:
                raise ValueError(f"Probe evidence references undeclared operation {evidence.operation_id}.")
            if evidence.operation_id in evidence_by_operation:
                raise ValueError(f"Duplicate probe evidence for operation {evidence.operation_id}.")
            evidence_by_operation[evidence.operation_id] = evidence
        capabilities = {
            operation_id: EffectiveOperationCapability(
                operation=operation,
                state=_effective_state(operation, evidence_by_operation.get(operation_id)),
                evidence=evidence_by_operation.get(operation_id),
            )
            for operation_id, operation in declared_profile.operations.items()
        }
        return cls(declared_profile=declared_profile, capabilities=capabilities)

    def for_operation(self, operation_id: OperationId) -> EffectiveOperationCapability:
        """Return the effective capability for a declared operation."""
        return self.capabilities[operation_id]

    def guard(self, operation_id: OperationId) -> GuardDecision:
        """Return a non-dispatching authorization and availability decision."""
        capability = self.capabilities.get(operation_id)
        if capability is None:
            return GuardDecision(
                operation_id=operation_id,
                state=EffectiveCapabilityState.UNSUPPORTED,
                allowed=False,
                remedy="Use a connector profile that declares the operation.",
            )
        state = capability.state
        remedies = {
            EffectiveCapabilityState.UNSUPPORTED: "Use a connector profile that declares the operation.",
            EffectiveCapabilityState.UNAUTHORIZED: "Provide credentials with access to the operation.",
            EffectiveCapabilityState.FORBIDDEN: "Use credentials with the required role for the operation.",
            EffectiveCapabilityState.UNAVAILABLE: "Retry after the target deployment is available.",
            EffectiveCapabilityState.DEPLOYMENT_DISABLED: "Enable the capability on the target deployment.",
        }
        return GuardDecision(
            operation_id=operation_id,
            state=state,
            allowed=state not in remedies,
            remedy=remedies.get(state),
        )


@dataclass(frozen=True, slots=True)
class DriftAdvisory:
    """Immutable advisory that never mutates a declared profile."""

    profile_version: str
    observed_fixture_fingerprint: str
    message: str
    advisory_only: bool = field(default=True, init=False)

    def __post_init__(self) -> None:
        _require_text("profile version", self.profile_version)
        _require_text("observed fixture fingerprint", self.observed_fixture_fingerprint)
        _require_text("drift message", self.message)


def _effective_state(operation: OperationSpec, evidence: ProbeEvidence | None) -> EffectiveCapabilityState:
    if evidence is None:
        return EffectiveCapabilityState.UNAVAILABLE
    response_states = {
        ProbeResponseClass.UNSUPPORTED: EffectiveCapabilityState.UNSUPPORTED,
        ProbeResponseClass.UNAUTHORIZED: EffectiveCapabilityState.UNAUTHORIZED,
        ProbeResponseClass.FORBIDDEN: EffectiveCapabilityState.FORBIDDEN,
        ProbeResponseClass.UNAVAILABLE: EffectiveCapabilityState.UNAVAILABLE,
        ProbeResponseClass.DEPLOYMENT_DISABLED: EffectiveCapabilityState.DEPLOYMENT_DISABLED,
    }
    if evidence.observed_response_class in response_states:
        return response_states[evidence.observed_response_class]
    return {
        CapabilityClass.CORE: EffectiveCapabilityState.CORE,
        CapabilityClass.OPTIONAL: EffectiveCapabilityState.OPTIONAL,
        CapabilityClass.AUTHENTICATED: EffectiveCapabilityState.AUTHENTICATED,
        CapabilityClass.ADMIN: EffectiveCapabilityState.ADMIN,
    }[operation.capability_class]


def _require_text(name: str, value: str) -> None:
    if not value.strip():
        raise ValueError(f"{name.capitalize()} is required.")


def _sanitize_deployment_url(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
        raise ValueError("Deployment URL must be a sanitized HTTPS URI.")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
