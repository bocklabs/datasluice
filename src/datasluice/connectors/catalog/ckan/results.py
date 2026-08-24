"""Typed CKAN mutation outcomes and secret-safe token results."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from datasluice.contracts.catalog.native.ckan import CKANResultItem
from datasluice.domain.catalog.auth import SecretValue
from datasluice.domain.catalog.models import ResultEnvelope, _freeze_json, _thaw_json
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.redaction import REDACTED
from datasluice.domain.catalog.safety import ConcurrencyPolicy, MutationPolicy
from datasluice.errors.catalog import CatalogValidationError

_MUTATION_TIERS = frozenset({"read", "standard", "destructive"})


@dataclass(frozen=True, slots=True)
class CKANTokenResult:
    """A created CKAN API token whose value is disclosed only through an explicit accessor."""

    result_metadata: Mapping[str, object]
    _secret: SecretValue = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.result_metadata, Mapping):
            raise ValueError("CKAN token result metadata must be a JSON object.")
        frozen = _freeze_json(dict(self.result_metadata), "ckan_token_result.metadata")
        if not isinstance(frozen, Mapping):
            raise ValueError("CKAN token result metadata must be a JSON object.")
        object.__setattr__(self, "result_metadata", MappingProxyType(dict(frozen)))
        if not isinstance(self._secret, SecretValue):
            object.__setattr__(self, "_secret", SecretValue(self._secret))

    @classmethod
    def from_token_result(cls, payload: object) -> CKANTokenResult:
        """Wrap one ``api_token_create`` result mapping into the secret-safe carrier.

        Args:
            payload: The decoded result value of an ``api_token_create`` action.

        Returns:
            A token result carrying every non-token key losslessly.

        Raises:
            ValueError: If the payload is not an object or carries no token.
        """
        if not isinstance(payload, Mapping):
            raise ValueError("CKAN token results must be JSON objects.")
        secret = payload.get("token")
        if not isinstance(secret, str) or not secret:
            raise ValueError("CKAN token results must carry a non-empty token value.")
        metadata = {key: value for key, value in payload.items() if key != "token"}
        return cls(result_metadata=metadata, _secret=SecretValue(secret))

    @property
    def token(self) -> SecretValue:
        """Return the created token as a reveal-only secret."""
        return self._secret

    def reveal(self) -> str:
        """Return the raw token value for authenticated transport boundaries only."""
        return self._secret.reveal()

    def to_dict(self) -> dict[str, object]:
        """Return the redacted serialization form with the token replaced by the marker."""
        payload = _thaw_json(self.result_metadata)
        if not isinstance(payload, dict):
            raise ValueError("CKAN token result metadata must thaw into a JSON object.")
        return {**payload, "token": REDACTED}


def default_standard_policy() -> MutationPolicy:
    """Return the spine-constructed standard mutation policy requiring no caller input.

    Returns:
        A non-destructive policy with an explicit overwrite concurrency instruction,
        so standard-tier mutations always produce receipts (RUN-03).
    """
    return MutationPolicy(destructive=False, concurrency=ConcurrencyPolicy(overwrite=True))


@dataclass(frozen=True, slots=True)
class CKANMutationResult:
    """The single typed outcome of one native CKAN mutation: result plus redacted receipt."""

    result: ResultEnvelope[CKANResultItem]
    receipt: MutationReceipt

    def __post_init__(self) -> None:
        if not isinstance(self.result, ResultEnvelope):
            raise ValueError("CKAN mutation results must carry a typed result envelope.")
        if not isinstance(self.receipt, MutationReceipt):
            raise ValueError("CKAN mutation results must carry a redacted mutation receipt.")

    def to_dict(self) -> dict[str, object]:
        """Return a fresh JSON-safe serialization carrying no credential-shaped value."""
        return {
            "schema_version": 1,
            "kind": "ckan_mutation_result",
            "result": self.result.to_dict(),
            "receipt": self.receipt.to_dict(),
        }


def require_mutation_tier(
    mutation_class: str,
    operation_id: OperationId,
    policy: MutationPolicy | None,
) -> MutationPolicy | None:
    """Gate one declared-tier mutation before any transport I/O and return its effective policy.

    Args:
        mutation_class: The entry's declared mutation tier: ``read``, ``standard``, or
            ``destructive``. Read entries never engage the gate.
        operation_id: The operation about to dispatch, named in any refusal.
        policy: The caller's mutation policy, or ``None`` to receive the spine default
            on the standard tier.

    Returns:
        The effective ``MutationPolicy`` for the dispatch, or the unchanged argument
        for read entries.

    Raises:
        CatalogValidationError: If a destructive entry lacks a confirmed destructive
            policy with an executable concurrency instruction.
        TypeError: If arguments carry the wrong types.
        ValueError: If the tier label is unknown.
    """
    if not isinstance(mutation_class, str):
        raise TypeError("Mutation tiers must be declared as strings.")
    if mutation_class not in _MUTATION_TIERS:
        raise ValueError(f"Unknown mutation tier: {mutation_class!r}.")
    if policy is not None and not isinstance(policy, MutationPolicy):
        raise TypeError("Mutation policies must use MutationPolicy or be omitted.")
    if mutation_class == "read":
        return policy
    if mutation_class == "destructive":
        confirmed = (
            isinstance(policy, MutationPolicy)
            and policy.destructive
            and policy.confirmation is not None
            and policy.confirmation.confirmed
            and policy.concurrency is not None
            and policy.concurrency.allows_execution()
        )
        if not confirmed:
            raise CatalogValidationError(
                f"{operation_id} is a destructive mutation and requires explicit confirmation before dispatch.",
                operation=str(operation_id),
                platform=operation_id.platform,
                safe_action=(
                    "Provide a MutationPolicy with destructive=True, "
                    "ConfirmationPolicy(confirmed=True), and a version token or explicit overwrite."
                ),
            )
        assert policy is not None
        return policy
    if policy is None:
        return default_standard_policy()
    if not isinstance(policy, MutationPolicy):
        raise TypeError("Standard mutations require a MutationPolicy or no policy at all.")
    return policy
