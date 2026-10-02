"""Immutable user, invitation, and API-token inputs and results."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from urllib.parse import urlsplit

from datasluice.connectors.catalog.udata.models.resources import ResourceUploadInput
from datasluice.connectors.catalog.udata.secrets import OneTimeUDataToken
from datasluice.domain.catalog.models import MappingRecord, NativeRecord, _freeze_json, _thaw_json
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.exceptions import DataSluiceError

_EMAIL = re.compile(r"[^@\s]+@[^@\s.]+(?:\.[^@\s.]+)+")
_WEBSITE_REQUIRED = "uData user website must be an HTTP URL."
_USER_FIELDS = frozenset(
    {"first_name", "last_name", "email", "website", "about", "prefered_language", "extras", "roles", "active"}
)


def _text(value: object, name: str, *, optional: bool = False) -> None:
    if optional and value is None:
        return
    if not isinstance(value, str) or not value:
        raise ValueError(f"uData user {name} must be a non-empty string.")


def _fields(value: Mapping[str, object], name: str, *, allow_empty: bool = False) -> Mapping[str, object]:
    return _frozen(value, name, allow_empty=allow_empty)


def _frozen(value: object, name: str, *, allow_empty: bool = False) -> Mapping[str, object]:
    if (
        not isinstance(value, Mapping)
        or (not allow_empty and not value)
        or not all(isinstance(key, str) and key for key in value)
    ):
        raise ValueError(f"uData user {name} must be a JSON mapping.")
    try:
        frozen = _freeze_json(dict(value), f"udata.user.{name}")
    except DataSluiceError as error:
        raise ValueError(f"uData user {name} must contain JSON-safe values only.") from error
    if not isinstance(frozen, Mapping):
        raise ValueError(f"uData user {name} must be a JSON mapping.")
    return frozen


def _website(value: object) -> None:
    if value is None:
        return
    if not isinstance(value, str) or not value:
        raise ValueError(_WEBSITE_REQUIRED)
    if any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(_WEBSITE_REQUIRED)
    try:
        parts = urlsplit(value)
        hostname = parts.hostname
    except ValueError:
        raise ValueError(_WEBSITE_REQUIRED) from None
    if parts.scheme not in {"http", "https"} or not parts.netloc or not hostname:
        raise ValueError(_WEBSITE_REQUIRED)


def _optional_text(value: object, name: str) -> None:
    if value is not None and not isinstance(value, str):
        raise ValueError(f"uData user {name} must be a string or null.")


def _email_field(value: object) -> None:
    _text(value, "email")
    if not isinstance(value, str) or _EMAIL.fullmatch(value) is None:
        raise ValueError("uData user email must be a valid email address.")


def _roles(value: object) -> None:
    if not isinstance(value, tuple) or not all(isinstance(role, str) and role for role in value):
        raise ValueError("uData user roles must be a list of non-empty strings.")


def _extras(value: object) -> None:
    if value is not None and not isinstance(value, Mapping):
        raise ValueError("uData user extras must be a mapping or null.")


def _validate_user_fields(fields: Mapping[str, object]) -> None:
    for name in ("first_name", "last_name", "about", "prefered_language"):
        if name in fields:
            _text(fields[name], name, optional=True)
    for name, validate in (
        ("email", _email_field),
        ("website", _website),
        ("roles", _roles),
        ("extras", _extras),
    ):
        if name in fields:
            validate(fields[name])
    if "active" in fields and type(fields["active"]) is not bool:
        raise ValueError("uData user active must be a boolean.")


@dataclass(frozen=True, slots=True)
class UserListQuery:
    """Documented user list and contact-point pagination."""

    q: str | None = None
    sort: str | None = None
    page: int = 1
    page_size: int = 20

    def __post_init__(self) -> None:
        _text(self.q, "q", optional=True)
        _text(self.sort, "sort", optional=True)
        if type(self.page) is not int or self.page < 1 or type(self.page_size) is not int or self.page_size < 1:
            raise ValueError("uData user page values must be positive integers.")

    def query_params(self) -> list[tuple[str, str]]:
        params = [("page", str(self.page)), ("page_size", str(self.page_size))]
        if self.q is not None:
            params.append(("q", self.q))
        if self.sort is not None:
            params.append(("sort", self.sort))
        return params


@dataclass(frozen=True, slots=True)
class UserSuggestQuery:
    """Documented user suggestion query."""

    q: str
    size: int = 10

    def __post_init__(self) -> None:
        _text(self.q, "suggest q")
        if type(self.size) is not int or not 1 <= self.size <= 20:
            raise ValueError("uData user suggestion size must be between 1 and 20.")

    def query_params(self) -> list[tuple[str, str]]:
        return [("q", self.q), ("size", str(self.size))]


@dataclass(frozen=True, slots=True)
class UserCreateInput:
    """Required fields for an administrative user create."""

    first_name: str
    last_name: str
    email: str = field(repr=False)
    fields: Mapping[str, object] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        for name in ("first_name", "last_name", "email"):
            _text(getattr(self, name), name)
        if _EMAIL.fullmatch(self.email) is None:
            raise ValueError("uData user email must be a valid email address.")
        object.__setattr__(self, "fields", _frozen(self.fields, "create fields", allow_empty=True))
        if {"first_name", "last_name", "email"} & set(self.fields) or set(self.fields) - _USER_FIELDS:
            raise ValueError("uData user create fields must use documented writable fields.")
        _validate_user_fields(self.fields)

    def payload(self) -> dict[str, object]:
        return {
            "first_name": self.first_name,
            "last_name": self.last_name,
            "email": self.email,
            **{key: _thaw_json(value) for key, value in self.fields.items()},
        }


@dataclass(frozen=True, slots=True)
class UserUpdateInput:
    """Presence-aware user update, including documented explicit nulls."""

    fields: Mapping[str, object] = field(repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "fields", _fields(self.fields, "update fields"))
        if set(self.fields) - _USER_FIELDS:
            raise ValueError("uData user updates must use documented writable fields.")
        _validate_user_fields(self.fields)

    def payload(self) -> dict[str, object]:
        return {key: _thaw_json(value) for key, value in self.fields.items()}


@dataclass(frozen=True, slots=True)
class UserDeleteOptions:
    """Documented administrative deletion query flags."""

    send_legal_notice: bool = False
    no_mail: bool = False
    delete_comments: bool = False

    def __post_init__(self) -> None:
        if any(type(value) is not bool for value in (self.send_legal_notice, self.no_mail, self.delete_comments)):
            raise ValueError("uData user deletion flags must be booleans.")


@dataclass(frozen=True, slots=True)
class ApiTokenCreateInput:
    """Documented token creation options, excluding plaintext."""

    name: str | None = None
    expires_at: datetime | None = None
    scopes: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        _text(self.name, "token name", optional=True)
        if self.name is not None and len(self.name) > 255:
            raise ValueError("uData token name must not exceed 255 characters.")
        if self.expires_at is not None:
            if not isinstance(self.expires_at, datetime) or self.expires_at.tzinfo is None:
                raise ValueError("uData token expiry must be a timezone-aware datetime.")
            if self.expires_at <= datetime.now(UTC):
                raise ValueError("uData token expiry must be in the future.")
        if self.scopes is not None and (not isinstance(self.scopes, tuple) or self.scopes != ("admin",)):
            raise ValueError("The pinned uData token scope is admin.")

    def payload(self) -> dict[str, object]:
        body: dict[str, object] = {}
        if self.name is not None:
            body["name"] = self.name
        if self.expires_at is not None:
            body["expires_at"] = self.expires_at.isoformat()
        if self.scopes is not None:
            body["scopes"] = list(self.scopes)
        return body


@dataclass(frozen=True, slots=True)
class ApiTokenMetadata:
    """Allowlisted token identity and status without its secret."""

    id: str
    token_prefix: str
    name: str | None = None
    created_at: str | None = None
    expires_at: str | None = None
    revoked_at: str | None = None
    kind: str | None = None
    scopes: tuple[str, ...] | None = None
    last_used_at: str | None = None
    user_agents: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        _text(self.id, "token id")
        _text(self.token_prefix, "token prefix")
        for name in ("name", "created_at", "expires_at", "revoked_at", "kind", "last_used_at"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, str):
                raise ValueError(f"uData token {name} must be a string or null.")
        for name in ("scopes", "user_agents"):
            values = getattr(self, name)
            if values is not None and (
                not isinstance(values, tuple) or not all(isinstance(value, str) and value for value in values)
            ):
                raise ValueError(f"uData token {name} must be a tuple of non-empty strings or null.")

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "token_prefix": self.token_prefix,
            "name": self.name,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "revoked_at": self.revoked_at,
            "kind": self.kind,
            "scopes": list(self.scopes) if self.scopes is not None else None,
            "last_used_at": self.last_used_at,
            "user_agents": list(self.user_agents) if self.user_agents is not None else None,
        }


@dataclass(frozen=True, slots=True)
class UserMutationResult:
    """User mutation record with a redacted receipt."""

    receipt: MutationReceipt
    record: NativeRecord | None = field(default=None, repr=False)
    value: MappingRecord | None = field(default=None, repr=False)

    def to_dict(self) -> dict[str, object]:
        return {
            "receipt": self.receipt.to_dict(),
            "record": self.record.to_dict() if self.record else None,
            "value": self.value.to_dict() if self.value else None,
        }


@dataclass(frozen=True, slots=True)
class ApiTokenCreationResult:
    """Created token metadata and an isolated reveal-once secret."""

    receipt: MutationReceipt
    metadata: ApiTokenMetadata
    secret: OneTimeUDataToken = field(repr=False)

    def to_dict(self) -> dict[str, object]:
        return {"receipt": self.receipt.to_dict(), "metadata": self.metadata.to_dict()}


UserAvatarInput = ResourceUploadInput
