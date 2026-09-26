"""Immutable uData taxonomy inputs and mutation results."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import quote

from datasluice.domain.catalog.models import MappingRecord
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.errors.catalog import CatalogValidationError

_SAFE_SEGMENT = frozenset("!$&'()*+,-.=@_~")


@dataclass(frozen=True, slots=True)
class BadgeCreateInput:
    """Typed dataset badge body."""

    kind: str

    def __post_init__(self) -> None:
        if not isinstance(self.kind, str) or not self.kind or any(c in "/?#\"'" for c in self.kind):
            raise ValueError("uData badge kind must be one non-empty URL-safe segment.")

    def payload(self) -> dict[str, str]:
        return {"kind": self.kind}


@dataclass(frozen=True, slots=True)
class SuggestQuery:
    """Required uData suggestion query and optional result limit."""

    q: str
    size: int = 10

    def __post_init__(self) -> None:
        if not isinstance(self.q, str) or not self.q:
            raise ValueError("uData taxonomy query must be a non-empty string.")
        if type(self.size) is not int or not 1 <= self.size <= 100:
            raise ValueError("uData taxonomy suggestion size must be an integer from 1 through 100.")

    def query(self) -> str:
        from urllib.parse import urlencode

        return urlencode([("q", self.q), ("size", str(self.size))])


@dataclass(frozen=True, slots=True)
class TaxonomyMutationResult:
    """Taxonomy mutation output retaining only bounded records and a redacted receipt."""

    receipt: MutationReceipt
    record: MappingRecord | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "receipt": self.receipt.to_dict(),
            "record": self.record.to_dict() if self.record is not None else None,
        }


def segment(value: str, operation: str) -> str:
    if not isinstance(value, str) or not value or any(c in "/?#\"'" for c in value) or any(ord(c) < 32 for c in value):
        raise CatalogValidationError(
            f"uData taxonomy identifier for {operation} must be one URL-safe path segment.",
            operation=operation,
            platform="udata",
            safe_action="Pass a prior typed read identifier.",
        )
    return quote(value, safe="" if value[0] not in _SAFE_SEGMENT else "")
