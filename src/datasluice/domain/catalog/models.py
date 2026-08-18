"""Immutable normalized and native catalog record envelopes."""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.exceptions import DataSluiceError

_EXTENSION_NAMESPACE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9.-]*[.][A-Za-z0-9][A-Za-z0-9.-]*$")


def _contract_error(path: str) -> DataSluiceError:
    return DataSluiceError(f"Invalid schema-v1 catalog contract at {path}")


def _object_dict(value: object, path: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise _contract_error(path)
    result: dict[str, object] = {}
    for key, nested in value.items():
        if not isinstance(key, str):
            raise _contract_error(path)
        result[key] = nested
    return result


def _freeze_json(value: object, path: str) -> object:
    if value is None or isinstance(value, (str, bool)) or type(value) is int:
        return value
    if type(value) is float:
        if math.isfinite(value):
            return value
        raise _contract_error(path)
    if isinstance(value, Mapping):
        frozen: dict[str, object] = {}
        for key, nested in value.items():
            if not isinstance(key, str):
                raise _contract_error(path)
            frozen[key] = _freeze_json(nested, f"{path}.{key}")
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(nested, path) for nested in value)
    raise _contract_error(path)


def _thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thaw_json(nested) for key, nested in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(nested) for nested in value]
    return value


def _freeze_extensions(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise _contract_error("extensions")
    frozen: dict[str, object] = {}
    for namespace, extension in value.items():
        if not isinstance(namespace, str) or _EXTENSION_NAMESPACE_RE.fullmatch(namespace) is None:
            raise _contract_error("extensions")
        frozen[namespace] = _freeze_json(extension, f"extensions.{namespace}")
    return MappingProxyType(frozen)


def _strict_envelope(value: object, path: str, kind: str, keys: frozenset[str]) -> dict[str, object]:
    data = _object_dict(value, path)
    if set(data) != keys or data["schema_version"] != 1 or type(data["schema_version"]) is not int:
        raise _contract_error(path)
    if data["kind"] != kind:
        raise _contract_error(path)
    return data


def _optional_string(value: object, path: str) -> str | None:
    if value is not None and not isinstance(value, str):
        raise _contract_error(path)
    return value


@dataclass(frozen=True)
class DatasetRecord:
    """A normalized immutable dataset record."""

    id: CatalogId
    name: str
    description: str | None = None
    extensions: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.id, CatalogId) or self.id.resource_kind != ResourceKind.DATASET:
            raise _contract_error("dataset.id")
        if not isinstance(self.name, str) or not self.name:
            raise _contract_error("dataset.name")
        _optional_string(self.description, "dataset.description")
        object.__setattr__(self, "extensions", _freeze_extensions(self.extensions))

    def to_dict(self) -> dict[str, object]:
        """Return a fresh JSON-safe dataset envelope."""
        return {
            "schema_version": 1,
            "kind": "dataset",
            "id": self.id.to_dict(),
            "name": self.name,
            "description": self.description,
            "extensions": _thaw_json(self.extensions),
        }

    @classmethod
    def from_dict(cls, value: object) -> DatasetRecord:
        """Decode one strict schema-v1 dataset envelope."""
        data = _strict_envelope(
            value,
            "dataset",
            "dataset",
            frozenset({"schema_version", "kind", "id", "name", "description", "extensions"}),
        )
        name = data["name"]
        if not isinstance(name, str):
            raise _contract_error("dataset.name")
        return cls(
            id=CatalogId.from_dict(data["id"]),
            name=name,
            description=_optional_string(data["description"], "dataset.description"),
            extensions=_object_dict(data["extensions"], "dataset.extensions"),
        )


@dataclass(frozen=True)
class ResourceRecord:
    """A normalized immutable resource record."""

    id: CatalogId
    dataset_id: CatalogId
    name: str
    url: str | None = None
    extensions: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.id, CatalogId) or self.id.resource_kind != ResourceKind.RESOURCE:
            raise _contract_error("resource.id")
        if not isinstance(self.dataset_id, CatalogId) or self.dataset_id.resource_kind != ResourceKind.DATASET:
            raise _contract_error("resource.dataset_id")
        if self.id.platform != self.dataset_id.platform:
            raise _contract_error("resource.dataset_id")
        if not isinstance(self.name, str) or not self.name:
            raise _contract_error("resource.name")
        _optional_string(self.url, "resource.url")
        object.__setattr__(self, "extensions", _freeze_extensions(self.extensions))

    def to_dict(self) -> dict[str, object]:
        """Return a fresh JSON-safe resource envelope."""
        return {
            "schema_version": 1,
            "kind": "resource",
            "id": self.id.to_dict(),
            "dataset_id": self.dataset_id.to_dict(),
            "name": self.name,
            "url": self.url,
            "extensions": _thaw_json(self.extensions),
        }

    @classmethod
    def from_dict(cls, value: object) -> ResourceRecord:
        """Decode one strict schema-v1 resource envelope."""
        data = _strict_envelope(
            value,
            "resource",
            "resource",
            frozenset({"schema_version", "kind", "id", "dataset_id", "name", "url", "extensions"}),
        )
        name = data["name"]
        if not isinstance(name, str):
            raise _contract_error("resource.name")
        return cls(
            id=CatalogId.from_dict(data["id"]),
            dataset_id=CatalogId.from_dict(data["dataset_id"]),
            name=name,
            url=_optional_string(data["url"], "resource.url"),
            extensions=_object_dict(data["extensions"], "resource.extensions"),
        )


@dataclass(frozen=True)
class OrganizationRecord:
    """A normalized immutable organization record."""

    id: CatalogId
    name: str
    description: str | None = None
    extensions: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.id, CatalogId) or self.id.resource_kind != ResourceKind.ORGANIZATION:
            raise _contract_error("organization.id")
        if not isinstance(self.name, str) or not self.name:
            raise _contract_error("organization.name")
        _optional_string(self.description, "organization.description")
        object.__setattr__(self, "extensions", _freeze_extensions(self.extensions))

    def to_dict(self) -> dict[str, object]:
        """Return a fresh JSON-safe organization envelope."""
        return {
            "schema_version": 1,
            "kind": "organization",
            "id": self.id.to_dict(),
            "name": self.name,
            "description": self.description,
            "extensions": _thaw_json(self.extensions),
        }

    @classmethod
    def from_dict(cls, value: object) -> OrganizationRecord:
        """Decode one strict schema-v1 organization envelope."""
        data = _strict_envelope(
            value,
            "organization",
            "organization",
            frozenset({"schema_version", "kind", "id", "name", "description", "extensions"}),
        )
        name = data["name"]
        if not isinstance(name, str):
            raise _contract_error("organization.name")
        return cls(
            id=CatalogId.from_dict(data["id"]),
            name=name,
            description=_optional_string(data["description"], "organization.description"),
            extensions=_object_dict(data["extensions"], "organization.extensions"),
        )


@dataclass(frozen=True)
class UserRecord:
    """A normalized immutable user record."""

    id: CatalogId
    username: str
    display_name: str | None = None
    extensions: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.id, CatalogId) or self.id.resource_kind != ResourceKind.USER:
            raise _contract_error("user.id")
        if not isinstance(self.username, str) or not self.username:
            raise _contract_error("user.username")
        _optional_string(self.display_name, "user.display_name")
        object.__setattr__(self, "extensions", _freeze_extensions(self.extensions))

    def to_dict(self) -> dict[str, object]:
        """Return a fresh JSON-safe user envelope."""
        return {
            "schema_version": 1,
            "kind": "user",
            "id": self.id.to_dict(),
            "username": self.username,
            "display_name": self.display_name,
            "extensions": _thaw_json(self.extensions),
        }

    @classmethod
    def from_dict(cls, value: object) -> UserRecord:
        """Decode one strict schema-v1 user envelope."""
        data = _strict_envelope(
            value,
            "user",
            "user",
            frozenset({"schema_version", "kind", "id", "username", "display_name", "extensions"}),
        )
        username = data["username"]
        if not isinstance(username, str):
            raise _contract_error("user.username")
        return cls(
            id=CatalogId.from_dict(data["id"]),
            username=username,
            display_name=_optional_string(data["display_name"], "user.display_name"),
            extensions=_object_dict(data["extensions"], "user.extensions"),
        )


@dataclass(frozen=True)
class NativeRecord:
    """A lossless immutable envelope for one platform-native record."""

    platform: CatalogPlatform
    resource_kind: ResourceKind
    id: CatalogId
    payload: Mapping[str, object]
    extensions: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.platform, CatalogPlatform) or not isinstance(self.resource_kind, ResourceKind):
            raise _contract_error("native_record.identity")
        if not isinstance(self.id, CatalogId) or (self.id.platform, self.id.resource_kind) != (
            self.platform,
            self.resource_kind,
        ):
            raise _contract_error("native_record.id")
        payload = _freeze_json(self.payload, "native_record.payload")
        if not isinstance(payload, Mapping):
            raise _contract_error("native_record.payload")
        object.__setattr__(self, "payload", payload)
        object.__setattr__(self, "extensions", _freeze_extensions(self.extensions))

    def to_dict(self) -> dict[str, object]:
        """Return a fresh JSON-safe native record envelope."""
        return {
            "schema_version": 1,
            "kind": "native_record",
            "platform": self.platform.value,
            "resource_kind": self.resource_kind.value,
            "id": self.id.to_dict(),
            "payload": _thaw_json(self.payload),
            "extensions": _thaw_json(self.extensions),
        }

    @classmethod
    def from_dict(cls, value: object) -> NativeRecord:
        """Decode one strict schema-v1 native record envelope."""
        data = _strict_envelope(
            value,
            "native_record",
            "native_record",
            frozenset({"schema_version", "kind", "platform", "resource_kind", "id", "payload", "extensions"}),
        )
        platform = data["platform"]
        resource_kind = data["resource_kind"]
        if not isinstance(platform, str) or not isinstance(resource_kind, str):
            raise _contract_error("native_record.identity")
        return cls(
            platform=CatalogPlatform(platform),
            resource_kind=ResourceKind(resource_kind),
            id=CatalogId.from_dict(data["id"]),
            payload=_object_dict(data["payload"], "native_record.payload"),
            extensions=_object_dict(data["extensions"], "native_record.extensions"),
        )


@dataclass(frozen=True)
class PageInfo:
    """Pagination state preserved with a result envelope."""

    cursor: str | None = None
    next_cursor: str | None = None
    total_items: int | None = None

    def __post_init__(self) -> None:
        _optional_string(self.cursor, "page_info.cursor")
        _optional_string(self.next_cursor, "page_info.next_cursor")
        if self.total_items is not None and (type(self.total_items) is not int or self.total_items < 0):
            raise _contract_error("page_info.total_items")

    def to_dict(self) -> dict[str, object]:
        """Return a fresh JSON-safe pagination envelope."""
        return {
            "schema_version": 1,
            "kind": "page_info",
            "cursor": self.cursor,
            "next_cursor": self.next_cursor,
            "total_items": self.total_items,
        }

    @classmethod
    def from_dict(cls, value: object) -> PageInfo:
        """Decode one strict schema-v1 pagination envelope."""
        data = _strict_envelope(
            value,
            "page_info",
            "page_info",
            frozenset({"schema_version", "kind", "cursor", "next_cursor", "total_items"}),
        )
        total_items = data["total_items"]
        if total_items is not None and type(total_items) is not int:
            raise _contract_error("page_info.total_items")
        return cls(
            cursor=_optional_string(data["cursor"], "page_info.cursor"),
            next_cursor=_optional_string(data["next_cursor"], "page_info.next_cursor"),
            total_items=total_items,
        )


@dataclass(frozen=True)
class WarningRecord:
    """A typed warning emitted alongside a catalog result."""

    code: str
    message: str

    def __post_init__(self) -> None:
        if not isinstance(self.code, str) or not self.code or not isinstance(self.message, str) or not self.message:
            raise _contract_error("warning")

    def to_dict(self) -> dict[str, object]:
        """Return a fresh JSON-safe warning envelope."""
        return {"schema_version": 1, "kind": "warning", "code": self.code, "message": self.message}

    @classmethod
    def from_dict(cls, value: object) -> WarningRecord:
        """Decode one strict schema-v1 warning envelope."""
        data = _strict_envelope(value, "warning", "warning", frozenset({"schema_version", "kind", "code", "message"}))
        code = data["code"]
        message = data["message"]
        if not isinstance(code, str) or not isinstance(message, str):
            raise _contract_error("warning")
        return cls(code=code, message=message)


@dataclass(frozen=True)
class PlatformMetadata:
    """Deployment metadata carried by a platform result envelope."""

    platform: CatalogPlatform
    api_version: str | None = None
    deployment: str | None = None
    extensions: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.platform, CatalogPlatform):
            raise _contract_error("platform_metadata.platform")
        _optional_string(self.api_version, "platform_metadata.api_version")
        _optional_string(self.deployment, "platform_metadata.deployment")
        object.__setattr__(self, "extensions", _freeze_extensions(self.extensions))

    def to_dict(self) -> dict[str, object]:
        """Return a fresh JSON-safe platform metadata envelope."""
        return {
            "schema_version": 1,
            "kind": "platform_metadata",
            "platform": self.platform.value,
            "api_version": self.api_version,
            "deployment": self.deployment,
            "extensions": _thaw_json(self.extensions),
        }

    @classmethod
    def from_dict(cls, value: object) -> PlatformMetadata:
        """Decode one strict schema-v1 platform metadata envelope."""
        data = _strict_envelope(
            value,
            "platform_metadata",
            "platform_metadata",
            frozenset({"schema_version", "kind", "platform", "api_version", "deployment", "extensions"}),
        )
        platform = data["platform"]
        if not isinstance(platform, str):
            raise _contract_error("platform_metadata.platform")
        return cls(
            platform=CatalogPlatform(platform),
            api_version=_optional_string(data["api_version"], "platform_metadata.api_version"),
            deployment=_optional_string(data["deployment"], "platform_metadata.deployment"),
            extensions=_object_dict(data["extensions"], "platform_metadata.extensions"),
        )


@dataclass(frozen=True)
class ResultEnvelope[T]:
    """A typed immutable result collection with its catalog metadata."""

    items: tuple[T, ...]
    page: PageInfo | None = None
    warnings: tuple[WarningRecord, ...] = ()
    platform: PlatformMetadata | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple):
            object.__setattr__(self, "items", tuple(self.items))
        if self.page is not None and not isinstance(self.page, PageInfo):
            raise _contract_error("result_envelope.page")
        if not isinstance(self.warnings, tuple):
            object.__setattr__(self, "warnings", tuple(self.warnings))
        if not all(isinstance(warning, WarningRecord) for warning in self.warnings):
            raise _contract_error("result_envelope.warnings")
        if self.platform is not None and not isinstance(self.platform, PlatformMetadata):
            raise _contract_error("result_envelope.platform")
        if not all(hasattr(item, "to_dict") for item in self.items):
            raise _contract_error("result_envelope.items")

    def to_dict(self) -> dict[str, object]:
        """Return a fresh JSON-safe result envelope."""
        return {
            "schema_version": 1,
            "kind": "result_envelope",
            "items": [item.to_dict() for item in self.items],  # ty: ignore[unresolved-attribute]: validated in post-init
            "page": self.page.to_dict() if self.page is not None else None,
            "warnings": [warning.to_dict() for warning in self.warnings],
            "platform": self.platform.to_dict() if self.platform is not None else None,
        }

    @classmethod
    def from_dict(cls, value: object, *, item_decoder: Callable[[object], T]) -> ResultEnvelope[T]:
        """Decode one strict schema-v1 result envelope with an item decoder."""
        data = _strict_envelope(
            value,
            "result_envelope",
            "result_envelope",
            frozenset({"schema_version", "kind", "items", "page", "warnings", "platform"}),
        )
        items = data["items"]
        warnings = data["warnings"]
        page = data["page"]
        platform = data["platform"]
        if not isinstance(items, list) or not isinstance(warnings, list):
            raise _contract_error("result_envelope")
        return cls(
            items=tuple(item_decoder(item) for item in items),
            page=PageInfo.from_dict(page) if page is not None else None,
            warnings=tuple(WarningRecord.from_dict(warning) for warning in warnings),
            platform=PlatformMetadata.from_dict(platform) if platform is not None else None,
        )
