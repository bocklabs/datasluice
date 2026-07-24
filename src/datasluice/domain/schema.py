"""Schema model describing the column-level shape of a resource."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Schema:
    """Schema describing the columns of a tabular resource.

    Attributes:
        name: Logical name for the schema.
        columns: Column descriptors (name, type, nullable, and portal-native fields).
        version: Schema evolution version (forward-compatible per DATA-07).
        extra: Portal-native schema fields not captured above.
    """

    name: str
    columns: list[dict[str, Any]] = field(default_factory=list)
    version: str = "1"
    extra: dict[str, Any] = field(default_factory=dict)
