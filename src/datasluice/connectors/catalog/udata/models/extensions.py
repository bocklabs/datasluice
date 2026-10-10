"""Immutable uData stock extension inputs and bounded capability evidence."""

from __future__ import annotations

from dataclasses import dataclass

from datasluice.connectors.catalog.udata.models._segment import path_segment
from datasluice.domain.catalog.profiles import ProbeResponseClass

MAX_TAG_SUGGEST_SIZE = 100
MIN_AVATAR_SIZE = 1
MAX_AVATAR_SIZE = 1024
AVAILABLE = "available"
EXTENSION_STATES = frozenset(
    {
        AVAILABLE,
        ProbeResponseClass.UNSUPPORTED.value,
        ProbeResponseClass.UNAUTHORIZED.value,
        ProbeResponseClass.FORBIDDEN.value,
        ProbeResponseClass.UNAVAILABLE.value,
        ProbeResponseClass.DEPLOYMENT_DISABLED.value,
    }
)


def segment(value: str, operation: str) -> str:
    """Return *value* quoted as one URL-safe avatar path segment."""
    return path_segment(value, operation, "uData extension")


@dataclass(frozen=True, slots=True)
class TagSuggestQuery:
    """Stock tag-suggestion query: required ``q`` and the upstream ``size`` ceiling."""

    q: str
    size: int = 10

    def __post_init__(self) -> None:
        if not isinstance(self.q, str) or not self.q:
            raise ValueError("uData tag suggest q must be a non-empty string.")
        if type(self.size) is not int or not 1 <= self.size <= MAX_TAG_SUGGEST_SIZE:
            raise ValueError(f"uData tag suggest size must be an integer from 1 through {MAX_TAG_SUGGEST_SIZE}.")

    def query_params(self) -> list[tuple[str, str]]:
        """Encode the exact tag-suggestion query-string parameter pairs."""
        return [("q", self.q), ("size", str(self.size))]


@dataclass(frozen=True, slots=True)
class AvatarRequest:
    """One bounded avatar image read: a single identifier and a pixel size."""

    identifier: str
    size: int

    def __post_init__(self) -> None:
        segment(self.identifier, "udata/api-v1.avatar")
        if not isinstance(self.size, int) or not MIN_AVATAR_SIZE <= self.size <= MAX_AVATAR_SIZE:
            raise ValueError(f"uData avatar size must be an integer from {MIN_AVATAR_SIZE} through {MAX_AVATAR_SIZE}.")


@dataclass(frozen=True, slots=True)
class ExtensionEvidence:
    """One bounded allowlisted extension capability proof.

    Only the four allowlisted identity fields are retained. Response bodies,
    deployment configuration, settings, internal endpoint metadata, private
    origins, and credential material never enter an evidence record.
    """

    operation_id: str
    state: str
    response_class: str
    credential_scope: str

    def __post_init__(self) -> None:
        if not isinstance(self.operation_id, str) or not self.operation_id:
            raise ValueError("Extension evidence requires a non-empty operation identity.")
        if self.state not in EXTENSION_STATES:
            raise ValueError(f"Extension evidence state must be one of {sorted(EXTENSION_STATES)}.")
        ProbeResponseClass(self.response_class)
        if not isinstance(self.credential_scope, str) or not self.credential_scope:
            raise ValueError("Extension evidence requires a non-empty credential scope.")

    @property
    def available(self) -> bool:
        """Return whether a harmless read-only probe established availability."""
        return self.state == AVAILABLE

    def to_dict(self) -> dict[str, object]:
        """Return exactly the allowlisted bounded evidence fields."""
        return {
            "operation_id": self.operation_id,
            "state": self.state,
            "response_class": self.response_class,
            "credential_scope": self.credential_scope,
        }
