"""CatalogCapabilities model — query-field-level capability contract (D-07)."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CatalogCapabilities:
    """Capabilities a catalog connector advertises to the runtime.

    Phase 5's reject policy (ARCH-08) reads this dataclass to produce
    actionable error messages for unsupported query filters.

    Attributes:
        supports_search: Whether free-text search is supported.
        supports_organizations: Whether organization filtering is supported.
        supports_faceted_search: Whether faceted search is supported.
        supported_query_fields: Query fields the connector honors.
        unsupported_query_fields: Query fields the connector rejects.
        notes: Human-readable capability notes keyed by field name.
    """

    supports_search: bool = True
    supports_organizations: bool = False
    supports_faceted_search: bool = False
    supported_query_fields: frozenset[str] = field(default_factory=frozenset)
    unsupported_query_fields: frozenset[str] = field(default_factory=frozenset)
    notes: dict[str, str] = field(default_factory=dict)
