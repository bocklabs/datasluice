"""Typed operation inventory for catalog connectors."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import re


_OPERATION_PART = re.compile(r"^[a-z][a-z0-9_-]*$")


class OperationTier(StrEnum):
    """Scope of an operation's public contract."""

    NORMALIZED = "normalized"
    NATIVE = "native"


class AuthClass(StrEnum):
    """Authentication requirement declared for an operation."""

    PUBLIC = "public"
    AUTHENTICATED = "authenticated"
    PRIVILEGED = "privileged"


class MutationClass(StrEnum):
    """Mutation category of an operation."""

    READ = "read"
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    ADMIN = "admin"


class Idempotency(StrEnum):
    """Retry safety declared for an operation."""

    SAFE = "safe"
    IDEMPOTENT = "idempotent"
    CONDITIONAL = "conditional"
    NON_IDEMPOTENT = "non-idempotent"


class ConcurrencyRequirement(StrEnum):
    """Concurrency token requirement for an operation."""

    NONE = "none"
    OPTIONAL = "optional"
    REQUIRED = "required"


class Atomicity(StrEnum):
    """Atomicity guarantee declared for an operation."""

    NONE = "none"
    SINGLE_RESOURCE = "single-resource"
    BATCH = "batch"


class CapabilityClass(StrEnum):
    """Declared availability class for an operation."""

    CORE = "core"
    OPTIONAL = "optional"
    AUTHENTICATED = "authenticated"
    ADMIN = "admin"


@dataclass(frozen=True, slots=True)
class OperationId:
    """Stable identifier for one platform operation."""

    platform: str
    service: str
    method: str

    def __post_init__(self) -> None:
        for name, value in (("platform", self.platform), ("service", self.service), ("method", self.method)):
            if not _OPERATION_PART.fullmatch(value):
                raise ValueError(f"Operation {name} must be a lowercase vendor identifier.")

    def __str__(self) -> str:
        """Return the portable operation identifier."""
        return f"{self.platform}/{self.service}.{self.method}"


@dataclass(frozen=True, slots=True)
class OperationSpec:
    """Complete contract specification for one dispatchable operation."""

    id: OperationId
    tier: OperationTier
    request_type: str
    response_type: str
    auth_class: AuthClass
    mutation_class: MutationClass
    idempotency: Idempotency
    concurrency: ConcurrencyRequirement
    atomicity: Atomicity
    capability_class: CapabilityClass

    def __post_init__(self) -> None:
        if not self.request_type or not self.response_type:
            raise ValueError("Operation request and response types are required.")
        if self.mutation_class is MutationClass.READ and self.atomicity is Atomicity.BATCH:
            raise ValueError("Read operations cannot declare batch atomicity.")
        if self.auth_class is AuthClass.PUBLIC and self.capability_class is CapabilityClass.ADMIN:
            raise ValueError("Admin capability operations cannot be public.")
