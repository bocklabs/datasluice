"""Immutable uData organization and membership inputs."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field

from datasluice.connectors.catalog.udata.models.resources import ResourceUploadInput
from datasluice.domain.catalog.models import MappingRecord, NativeRecord, _freeze_json, _thaw_json
from datasluice.domain.catalog.receipts import MutationReceipt

_ROLES = frozenset({"admin", "editor", "partial_editor"})
_EMAIL = re.compile(r"[^@\s]+@[^@\s.]+(?:\.[^@\s.]+)+")


def _text(value: object, field_name: str, *, allow_none: bool = False) -> None:
    if allow_none and value is None:
        return
    if not isinstance(value, str) or not value:
        raise ValueError(f"uData organization {field_name} must be a non-empty string.")


def _role(value: object, field_name: str, *, allow_none: bool = False) -> None:
    _text(value, field_name, allow_none=allow_none)
    if value is not None and value not in _ROLES:
        raise ValueError(f"uData organization {field_name} must be one of {sorted(_ROLES)}.")


def _email(value: str | None) -> None:
    if value is not None and _EMAIL.fullmatch(value) is None:
        raise ValueError("uData invitation email must be a valid email address.")


def _fields(value: Mapping[str, object] | None, field_name: str) -> Mapping[str, object] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping) or not all(isinstance(key, str) and key for key in value):
        raise ValueError(f"uData organization {field_name} must be a JSON mapping.")
    frozen = _freeze_json(dict(value), f"udata.organization.{field_name}")
    if not isinstance(frozen, Mapping):
        raise ValueError(f"uData organization {field_name} must be a JSON mapping.")
    return frozen


def _payload(value: Mapping[str, object] | object) -> dict[str, object]:
    payload_method = getattr(value, "payload", None)
    candidate = payload_method() if callable(payload_method) else value
    if not isinstance(candidate, Mapping):
        raise ValueError("uData organization inputs must encode a JSON mapping.")
    return {key: _thaw_json(item) for key, item in candidate.items()}


@dataclass(frozen=True, slots=True)
class OrganizationListQuery:
    """Query values accepted by organization and owned-object list routes."""

    q: str | None = None
    sort: str | None = None
    page: int = 1
    page_size: int = 20
    filters: Mapping[str, str | bool | tuple[str, ...]] | None = None

    def __post_init__(self) -> None:
        if type(self.page) is not int or self.page < 1 or type(self.page_size) is not int or self.page_size < 1:
            raise ValueError("uData organization page values must be positive integers.")
        if self.q is not None and not isinstance(self.q, str):
            raise ValueError("uData organization q must be a string when supplied.")
        if self.sort is not None and (not isinstance(self.sort, str) or not self.sort):
            raise ValueError("uData organization sort must be a non-empty string when supplied.")
        if self.filters is not None:
            if not isinstance(self.filters, Mapping):
                raise ValueError("uData organization filters must be a mapping.")
            object.__setattr__(self, "filters", _fields(self.filters, "filters"))

    def query_params(self) -> list[tuple[str, str]]:
        params = [("page", str(self.page)), ("page_size", str(self.page_size))]
        if self.q is not None:
            params.append(("q", self.q))
        if self.sort is not None:
            params.append(("sort", self.sort))
        for key, value in sorted((self.filters or {}).items()):
            if isinstance(value, tuple):
                params.extend((key, item) for item in value)
            elif isinstance(value, bool):
                params.append((key, "true" if value else "false"))
            else:
                params.append((key, value))
        return params


@dataclass(frozen=True, slots=True)
class OrganizationSuggestQuery:
    """Required text and bounded size for suggest routes."""

    q: str
    size: int = 10

    def __post_init__(self) -> None:
        _text(self.q, "suggest q")
        if type(self.size) is not int or self.size < 1:
            raise ValueError("uData organization suggest size must be positive.")

    def query_params(self) -> list[tuple[str, str]]:
        return [("q", self.q), ("size", str(self.size))]


@dataclass(frozen=True, slots=True)
class MembershipRequestQuery:
    """Optional status and user filters for membership requests."""

    status: str | None = None
    user: str | None = None

    def __post_init__(self) -> None:
        _text(self.status, "membership status", allow_none=True)
        _text(self.user, "membership user", allow_none=True)

    def query_params(self) -> list[tuple[str, str]]:
        return [(key, value) for key, value in (("status", self.status), ("user", self.user)) if value is not None]


@dataclass(frozen=True, slots=True)
class OrganizationCreateInput:
    """Organization creation payload."""

    name: str
    description: str
    acronym: str | None = None
    url: str | None = None
    business_number_id: str | None = None
    fields: Mapping[str, object] | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        _text(self.name, "name")
        _text(self.description, "description")
        _text(self.acronym, "acronym", allow_none=True)
        _text(self.url, "url", allow_none=True)
        _text(self.business_number_id, "business number", allow_none=True)
        object.__setattr__(self, "fields", _fields(self.fields, "fields"))

    def payload(self) -> dict[str, object]:
        body: dict[str, object] = {"name": self.name, "description": self.description}
        for key, value in (
            ("acronym", self.acronym),
            ("url", self.url),
            ("business_number_id", self.business_number_id),
        ):
            if value is not None:
                body[key] = value
        body.update(_payload(self.fields or {}))
        return body


@dataclass(frozen=True, slots=True)
class OrganizationUpdateInput:
    """Partial organization update payload preserving omitted fields."""

    name: str | None = None
    description: str | None = None
    acronym: str | None = None
    url: str | None = None
    business_number_id: str | None = None
    fields: Mapping[str, object] | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        for name, value in (
            ("name", self.name),
            ("description", self.description),
            ("acronym", self.acronym),
            ("url", self.url),
            ("business number", self.business_number_id),
        ):
            _text(value, name, allow_none=True)
        if (
            all(
                value is None
                for value in (self.name, self.description, self.acronym, self.url, self.business_number_id)
            )
            and not self.fields
        ):
            raise ValueError("uData organization updates require at least one field.")
        object.__setattr__(self, "fields", _fields(self.fields, "fields"))

    def payload(self) -> dict[str, object]:
        body: dict[str, object] = {
            key: value
            for key, value in (
                ("name", self.name),
                ("description", self.description),
                ("acronym", self.acronym),
                ("url", self.url),
                ("business_number_id", self.business_number_id),
            )
            if value is not None
        }
        body.update(_payload(self.fields or {}))
        return body


@dataclass(frozen=True, slots=True)
class MembershipRequestInput:
    """Membership request or invitation fields accepted by uData."""

    comment: str
    role: str | None = None
    assignments: tuple[Mapping[str, str], ...] = ()

    def __post_init__(self) -> None:
        _text(self.comment, "membership comment")
        _role(self.role, "membership role", allow_none=True)
        if not isinstance(self.assignments, tuple) or not all(isinstance(item, Mapping) for item in self.assignments):
            raise ValueError("uData membership assignments must be a tuple of mappings.")
        if self.assignments and self.role != "partial_editor":
            raise ValueError("uData membership assignments require the partial_editor role.")

    def payload(self) -> dict[str, object]:
        body: dict[str, object] = {"comment": self.comment}
        if self.role is not None:
            body["role"] = self.role
        if self.assignments:
            body["assignments"] = [_payload(item) for item in self.assignments]
        return body


@dataclass(frozen=True, slots=True)
class OrganizationInvitationInput:
    """Organization invitation payload."""

    user: str | None = None
    email: str | None = None
    role: str | None = None
    comment: str | None = None
    assignments: tuple[Mapping[str, str], ...] = ()

    def __post_init__(self) -> None:
        if (self.user is None) == (self.email is None):
            raise ValueError("uData invitations require exactly one of user or email.")
        for name, value in (("user", self.user), ("email", self.email), ("comment", self.comment)):
            _text(value, f"invitation {name}", allow_none=True)
        _email(self.email)
        _role(self.role, "invitation role", allow_none=True)
        if not isinstance(self.assignments, tuple) or not all(isinstance(item, Mapping) for item in self.assignments):
            raise ValueError("uData invitation assignments must be a tuple of mappings.")
        if self.assignments and self.role != "partial_editor":
            raise ValueError("uData invitation assignments require the partial_editor role.")

    def payload(self) -> dict[str, object]:
        body: dict[str, object] = {
            key: value
            for key, value in (
                ("user", self.user),
                ("email", self.email),
                ("role", self.role),
                ("comment", self.comment),
            )
            if value is not None
        }
        if self.assignments:
            body["assignments"] = [_payload(item) for item in self.assignments]
        return body


@dataclass(frozen=True, slots=True)
class OrganizationMemberInput:
    """Organization member role update payload."""

    role: str
    fields: Mapping[str, object] | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        _role(self.role, "member role")
        object.__setattr__(self, "fields", _fields(self.fields, "member fields"))
        if self.fields is not None and "role" in self.fields:
            raise ValueError("uData member fields cannot override the typed role.")

    def payload(self) -> dict[str, object]:
        return {"role": self.role, **_payload(self.fields or {})}


@dataclass(frozen=True, slots=True)
class OrganizationRefusalInput:
    """Required refusal comment."""

    comment: str

    def __post_init__(self) -> None:
        _text(self.comment, "refusal comment")

    def payload(self) -> dict[str, object]:
        return {"comment": self.comment}


@dataclass(frozen=True, slots=True)
class OrganizationMutationResult:
    """Immutable organization mutation result with a redacted receipt."""

    receipt: MutationReceipt
    record: NativeRecord | None = field(default=None, repr=False)
    records: tuple[NativeRecord | MappingRecord, ...] = field(default=(), repr=False)
    value: Mapping[str, object] | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.receipt, MutationReceipt):
            raise ValueError("uData organization mutations require a receipt.")
        if self.record is not None and not isinstance(self.record, NativeRecord):
            raise ValueError("uData organization records require NativeRecord.")
        if not isinstance(self.records, tuple) or not all(
            isinstance(item, (NativeRecord, MappingRecord)) for item in self.records
        ):
            raise ValueError("uData organization result records require typed records.")
        if self.value is not None:
            frozen = _freeze_json(_payload(self.value), "udata.organization.result")
            if not isinstance(frozen, Mapping):
                raise ValueError("uData organization result values must be a JSON mapping.")
            object.__setattr__(self, "value", frozen)

    def to_dict(self) -> dict[str, object]:
        return {
            "receipt": self.receipt.to_dict(),
            "record": self.record.to_dict() if self.record is not None else None,
            "records": [item.to_dict() for item in self.records],
            "value": _thaw_json(self.value) if self.value is not None else None,
        }


OrganizationDatasetQuery = OrganizationListQuery
OrganizationLogoInput = ResourceUploadInput

__all__ = [
    "MembershipRequestInput",
    "MembershipRequestQuery",
    "OrganizationCreateInput",
    "OrganizationDatasetQuery",
    "OrganizationInvitationInput",
    "OrganizationListQuery",
    "OrganizationLogoInput",
    "OrganizationMemberInput",
    "OrganizationMutationResult",
    "OrganizationRefusalInput",
    "OrganizationSuggestQuery",
    "OrganizationUpdateInput",
]
