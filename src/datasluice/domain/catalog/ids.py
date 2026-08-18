"""Opaque identities for catalog contract records."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import ClassVar

from datasluice.exceptions import DataSluiceError

_VALUE_RE = re.compile(r"[a-z0-9][a-z0-9._/-]*$")
_ID_KEYS = frozenset({"schema_version", "kind", "platform", "resource_kind", "value"})


def _contract_error(path: str) -> DataSluiceError:
    return DataSluiceError(f"Invalid schema-v1 catalog contract at {path}")


@dataclass(frozen=True)
class CatalogPlatform:
    """An explicit platform identity for one catalog deployment family."""

    value: str

    CKAN: ClassVar[CatalogPlatform]
    UDATA: ClassVar[CatalogPlatform]
    SOCRATA: ClassVar[CatalogPlatform]

    def __post_init__(self) -> None:
        if not isinstance(self.value, str) or _VALUE_RE.fullmatch(self.value) is None:
            raise _contract_error("platform")

    def __str__(self) -> str:
        """Return the JSON platform value."""
        return self.value


@dataclass(frozen=True)
class ResourceKind:
    """An explicit resource kind scoped by a CatalogId."""

    value: str

    DATASET: ClassVar[ResourceKind]
    RESOURCE: ClassVar[ResourceKind]
    ORGANIZATION: ClassVar[ResourceKind]
    USER: ClassVar[ResourceKind]

    def __post_init__(self) -> None:
        if not isinstance(self.value, str) or _VALUE_RE.fullmatch(self.value) is None:
            raise _contract_error("resource_kind")

    def __str__(self) -> str:
        """Return the JSON resource-kind value."""
        return self.value


CatalogPlatform.CKAN = CatalogPlatform("ckan")
CatalogPlatform.UDATA = CatalogPlatform("udata")
CatalogPlatform.SOCRATA = CatalogPlatform("socrata")
ResourceKind.DATASET = ResourceKind("dataset")
ResourceKind.RESOURCE = ResourceKind("resource")
ResourceKind.ORGANIZATION = ResourceKind("organization")
ResourceKind.USER = ResourceKind("user")


@dataclass(frozen=True)
class CatalogId:
    """A platform- and resource-kind-scoped opaque catalog identifier."""

    platform: CatalogPlatform
    resource_kind: ResourceKind
    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.platform, CatalogPlatform):
            raise _contract_error("catalog_id.platform")
        if not isinstance(self.resource_kind, ResourceKind):
            raise _contract_error("catalog_id.resource_kind")
        if not isinstance(self.value, str) or not self.value:
            raise _contract_error("catalog_id.value")

    def to_dict(self) -> dict[str, object]:
        """Return a fresh JSON-safe catalog identifier envelope."""
        return {
            "schema_version": 1,
            "kind": "catalog_id",
            "platform": self.platform.value,
            "resource_kind": self.resource_kind.value,
            "value": self.value,
        }

    @classmethod
    def from_dict(cls, value: object) -> CatalogId:
        """Decode one strict schema-v1 catalog identifier envelope."""
        if not isinstance(value, dict) or set(value) != _ID_KEYS:
            raise _contract_error("catalog_id")
        if value["schema_version"] != 1 or type(value["schema_version"]) is not int or value["kind"] != "catalog_id":
            raise _contract_error("catalog_id")
        platform = value["platform"]
        resource_kind = value["resource_kind"]
        native_value = value["value"]
        if not isinstance(platform, str) or not isinstance(resource_kind, str) or not isinstance(native_value, str):
            raise _contract_error("catalog_id")
        return cls(CatalogPlatform(platform), ResourceKind(resource_kind), native_value)
