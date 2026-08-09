"""Schema model describing the column-level shape of a resource."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any


@dataclass(frozen=True)
class Schema:
    """Schema describing the columns of a tabular resource.

    Attributes:
        name: Logical name for the schema.
        columns: Column descriptors (name, type, nullable, and portal-native fields).
        version: Schema evolution version.
        extra: Portal-native schema fields not captured above.
    """

    name: str
    columns: Sequence[dict[str, Any]] = field(default_factory=tuple)
    version: str = "1"
    extra: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.columns, tuple):
            object.__setattr__(self, "columns", tuple(self.columns))
        if not isinstance(self.extra, MappingProxyType):
            object.__setattr__(self, "extra", MappingProxyType(dict(self.extra)))
