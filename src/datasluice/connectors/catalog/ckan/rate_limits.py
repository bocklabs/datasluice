"""Portal-derived CKAN rate-policy resolution as an explicit metadata-only contract.

Resolution here is deliberately metadata-only: politeness derives from each
portal's OWN officially documented Action API limits, DataSluice invents no
throttle while no portal documents a limit, and an enforcement seam remains
future reviewed work for when a real documented limit exists.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from datasluice.connectors.catalog.ckan.settings import CKANClientSettings


@runtime_checkable
class PortalRatePolicy(Protocol):
    """Structural contract satisfied by connector rate-policy records."""

    @property
    def source_note(self) -> str:
        """Return the provenance note documenting where the policy comes from."""


@dataclass(frozen=True, slots=True)
class UnlimitedRatePolicy:
    """The typed no-op default policy used while no portal documents a limit."""

    source_note: str = "DataSluice imposes no client-invented cap while this portal documents no Action API rate limit."


@dataclass(frozen=True, slots=True)
class DocumentedPortalLimit:
    """One portal's officially documented Action API limit, if any."""

    origin: str
    requests_per_window: int | None
    window_seconds: int | None
    source_note: str

    def __post_init__(self) -> None:
        if self.requests_per_window is None and (self.window_seconds is not None or not self.source_note):
            raise ValueError("A none-documented portal limit must carry no window and a provenance note.")


PORTAL_RATE_LIMITS: Mapping[str, DocumentedPortalLimit] = MappingProxyType(
    {
        "https://demo.ckan.org": DocumentedPortalLimit(
            origin="https://demo.ckan.org",
            requests_per_window=None,
            window_seconds=None,
            source_note="demo.ckan.org publishes no documented Action API rate limit (verified 2026-08-15).",
        ),
        "https://ckan.publishing.service.gov.uk": DocumentedPortalLimit(
            origin="https://ckan.publishing.service.gov.uk",
            requests_per_window=None,
            window_seconds=None,
            source_note=(
                "data.gov.uk documents no API key and no rate limits for its Action API (verified 2026-08-15)."
            ),
        ),
    }
)


def resolve_rate_policy(settings: CKANClientSettings) -> PortalRatePolicy:
    """Resolve the effective metadata-only rate policy for one settings record.

    Args:
        settings: The immutable client settings carrying an optional explicit
            caller policy and a normalized base URL.

    Returns:
        The caller-supplied policy verbatim when present, else the known
        portal's documented-limit table entry, else the unlimited default.
    """
    if settings.rate_policy is not None:
        return settings.rate_policy
    entry = PORTAL_RATE_LIMITS.get(settings.base_url)
    if entry is not None:
        return entry
    return UnlimitedRatePolicy()
