"""CatalogCapabilities model — query-field-level capability contract."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType


@dataclass(frozen=True)
class CatalogCapabilities:
    """Capabilities a catalog connector advertises to the runtime.

    reject policy reads this dataclass to produce
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
    notes: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.notes, MappingProxyType):
            object.__setattr__(self, "notes", MappingProxyType(dict(self.notes)))
