"""Immutable mutation and bulk-execution safety contracts."""

from __future__ import annotations

from dataclasses import dataclass

_ATOMICITIES = frozenset({"atomic", "independent"})
_MAX_BULK_PARALLELISM = 32


@dataclass(frozen=True, slots=True)
class ConfirmationPolicy:
    """Explicit caller confirmation required for destructive work."""

    confirmed: bool = False

    def __post_init__(self) -> None:
        if type(self.confirmed) is not bool:
            raise ValueError("Mutation confirmation must be a boolean.")


@dataclass(frozen=True, slots=True)
class IdempotencyPolicy:
    """Classify whether a catalog request is safe to repeat."""

    safe: bool = False
    key: str | None = None
    explicit_retry_opt_in: bool = False

    def __post_init__(self) -> None:
        if type(self.safe) is not bool or type(self.explicit_retry_opt_in) is not bool:
            raise ValueError("Idempotency flags must be booleans.")
        if self.key is not None and (not isinstance(self.key, str) or not self.key):
            raise ValueError("Idempotency keys must be non-empty strings.")

    def allows_retry(self) -> bool:
        """Return whether retrying a request cannot silently duplicate work."""
        return self.safe or self.key is not None or self.explicit_retry_opt_in


@dataclass(frozen=True, slots=True)
class ConcurrencyPolicy:
    """Require a platform version token unless overwrite is explicit."""

    token: str | None = None
    overwrite: bool = False

    def __post_init__(self) -> None:
        if self.token is not None and (not isinstance(self.token, str) or not self.token):
            raise ValueError("Concurrency tokens must be non-empty strings.")
        if type(self.overwrite) is not bool:
            raise ValueError("Concurrency overwrite must be a boolean.")

    def allows_execution(self) -> bool:
        """Return whether a mutation has a safe concurrency instruction."""
        return self.token is not None or self.overwrite


@dataclass(frozen=True, slots=True)
class DryRunPolicy:
    """Request a capability-gated server-side mutation preview."""

    requested: bool = False

    def __post_init__(self) -> None:
        if type(self.requested) is not bool:
            raise ValueError("Dry-run requests must be booleans.")

    def allows_execution(self, *, capability_supported: bool) -> bool:
        """Return whether the requested preview is available on this deployment."""
        if type(capability_supported) is not bool:
            raise ValueError("Dry-run capability state must be a boolean.")
        return not self.requested or capability_supported


@dataclass(frozen=True, slots=True)
class MutationPolicy:
    """Aggregate confirmation, concurrency, retry, and preview instructions."""

    destructive: bool = False
    confirmation: ConfirmationPolicy | None = None
    concurrency: ConcurrencyPolicy | None = None
    idempotency: IdempotencyPolicy = IdempotencyPolicy()
    dry_run: DryRunPolicy = DryRunPolicy()

    def __post_init__(self) -> None:
        if type(self.destructive) is not bool:
            raise ValueError("Mutation destructive state must be a boolean.")
        if self.confirmation is not None and not isinstance(self.confirmation, ConfirmationPolicy):
            raise ValueError("Mutation confirmation must use ConfirmationPolicy.")
        if self.concurrency is not None and not isinstance(self.concurrency, ConcurrencyPolicy):
            raise ValueError("Mutation concurrency must use ConcurrencyPolicy.")
        if not isinstance(self.idempotency, IdempotencyPolicy) or not isinstance(self.dry_run, DryRunPolicy):
            raise ValueError("Mutation policy must use typed retry and dry-run policies.")

    def allows_execution(self, *, capability_supported: bool = True) -> bool:
        """Return whether destructive confirmation and requested preview are satisfied."""
        confirmed = self.confirmation is not None and self.confirmation.confirmed
        return (not self.destructive or confirmed) and self.dry_run.allows_execution(
            capability_supported=capability_supported
        )


@dataclass(frozen=True, slots=True)
class BulkExecutionPolicy:
    """Bound native-atomic or independent-resumable bulk execution."""

    atomicity: str = "independent"
    native_atomic_available: bool = False
    max_parallelism: int = 1
    cancellation_requested: bool = False
    checkpoint_required: bool = True

    def __post_init__(self) -> None:
        if self.atomicity not in _ATOMICITIES:
            raise ValueError("Bulk atomicity must be atomic or independent.")
        if type(self.native_atomic_available) is not bool or type(self.cancellation_requested) is not bool:
            raise ValueError("Bulk availability and cancellation state must be booleans.")
        if type(self.checkpoint_required) is not bool:
            raise ValueError("Bulk checkpoint state must be a boolean.")
        if type(self.max_parallelism) is not int or not 1 <= self.max_parallelism <= _MAX_BULK_PARALLELISM:
            raise ValueError("Bulk parallelism must be between one and the configured safety limit.")
        if self.atomicity == "atomic" and not self.native_atomic_available:
            raise ValueError("Atomic bulk execution requires a native atomic endpoint.")
        if self.atomicity == "atomic" and self.max_parallelism != 1:
            raise ValueError("Native atomic bulk execution cannot parallelize independent requests.")

    @property
    def is_atomic(self) -> bool:
        """Return whether the platform performs the whole bulk operation atomically."""
        return self.atomicity == "atomic"
