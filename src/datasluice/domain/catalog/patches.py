"""Exact create and patch request serialization for catalog mutations."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Final

from datasluice.domain.catalog.models import _contract_error, _freeze_json, _object_dict, _thaw_json


class UnsetType:
    """The singleton marker for an omitted patch field."""

    def __repr__(self) -> str:
        """Return a stable diagnostic representation."""
        return "UNSET"


UNSET: Final = UnsetType()


def _freeze_fields(value: object, *, allow_unset: bool) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise _contract_error("request.fields")
    frozen: dict[str, object] = {}
    for name, field_value in value.items():
        if not isinstance(name, str) or not name:
            raise _contract_error("request.fields")
        if field_value is UNSET:
            if not allow_unset:
                raise _contract_error("create_request.fields")
            frozen[name] = UNSET
        else:
            frozen[name] = _freeze_json(field_value, f"request.fields.{name}")
    return MappingProxyType(frozen)


@dataclass(frozen=True)
class CreateRequest:
    """A create request that requires every supplied value to be explicit."""

    fields: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "fields", _freeze_fields(self.fields, allow_unset=False))

    def to_wire(self) -> dict[str, object]:
        """Return a fresh JSON-safe create payload."""
        return {name: _thaw_json(value) for name, value in self.fields.items()}

    def to_dict(self) -> dict[str, object]:
        """Return a strict schema-v1 create-request envelope."""
        return {"schema_version": 1, "kind": "create_request", "fields": self.to_wire()}

    @classmethod
    def from_dict(cls, value: object) -> CreateRequest:
        """Decode one strict schema-v1 create-request envelope."""
        data = _object_dict(value, "create_request")
        if set(data) != {"schema_version", "kind", "fields"}:
            raise _contract_error("create_request")
        if data["schema_version"] != 1 or type(data["schema_version"]) is not int or data["kind"] != "create_request":
            raise _contract_error("create_request")
        return cls(fields=_object_dict(data["fields"], "create_request.fields"))


@dataclass(frozen=True)
class PatchRequest:
    """A patch request preserving omitted, null, and replacement values."""

    fields: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "fields", _freeze_fields(self.fields, allow_unset=True))

    def to_wire(self) -> dict[str, object]:
        """Return a fresh JSON-safe patch payload with omitted fields removed."""
        return {name: _thaw_json(value) for name, value in self.fields.items() if value is not UNSET}

    def to_dict(self) -> dict[str, object]:
        """Return a strict schema-v1 patch-request envelope retaining tri-state intent."""
        fields: list[dict[str, object]] = []
        for name, value in self.fields.items():
            if value is UNSET:
                fields.append({"name": name, "state": "unset"})
            else:
                fields.append({"name": name, "state": "value", "value": _thaw_json(value)})
        return {"schema_version": 1, "kind": "patch_request", "fields": fields}

    @classmethod
    def from_dict(cls, value: object) -> PatchRequest:
        """Decode one strict schema-v1 patch-request envelope retaining tri-state intent."""
        data = _object_dict(value, "patch_request")
        if set(data) != {"schema_version", "kind", "fields"}:
            raise _contract_error("patch_request")
        if data["schema_version"] != 1 or type(data["schema_version"]) is not int or data["kind"] != "patch_request":
            raise _contract_error("patch_request")
        serialized_fields = data["fields"]
        if not isinstance(serialized_fields, list):
            raise _contract_error("patch_request.fields")
        fields: dict[str, object] = {}
        for index, serialized_field in enumerate(serialized_fields):
            field_data = _object_dict(serialized_field, f"patch_request.fields.{index}")
            name = field_data.get("name")
            state = field_data.get("state")
            if not isinstance(name, str) or not name or not isinstance(state, str) or name in fields:
                raise _contract_error("patch_request.fields")
            if state == "unset" and set(field_data) == {"name", "state"}:
                fields[name] = UNSET
            elif state == "value" and set(field_data) == {"name", "state", "value"}:
                fields[name] = field_data["value"]
            else:
                raise _contract_error("patch_request.fields")
        return cls(fields=fields)
