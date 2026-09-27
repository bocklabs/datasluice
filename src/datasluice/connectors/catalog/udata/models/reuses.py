"""Immutable uData reuse and reuse-follower inputs and mutation results."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import quote

from datasluice.domain.catalog.models import MappingRecord
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.errors.catalog import CatalogValidationError
from datasluice.runtime.transport.base import UploadPart

_REUSE_FILTERS = ("q", "dataset", "featured", "topic", "type", "tag", "organization", "organization_badge", "owner")
_REUSE_BOOLEAN_FILTERS = ("featured",)
_REUSE_SORTS = ("created", "created_at", "last_modified", "title", "slug", "datasets", "followers", "views")


def segment(value: str, operation: str) -> str:
    if not isinstance(value, str) or not value or any(c in "/?#\"'" for c in value) or any(ord(c) < 32 for c in value):
        raise CatalogValidationError(
            f"uData reuse identifier for {operation} must be one URL-safe path segment.",
            operation=operation,
            platform="udata",
            safe_action="Pass a prior typed read identifier.",
        )
    return quote(value, safe="")


@dataclass(frozen=True, slots=True)
class ReuseListQuery:
    """Stock v1 reuse collection query surface.

    ``q`` and the documented filters are omitted unless supplied, mirroring the
    upstream index parser. Paging keeps the stock ``page``/``page_size`` names
    and defaults.
    """

    q: str | None = None
    page: int = 1
    page_size: int = 20
    sort: str | None = None
    filters: Mapping[str, str | bool | tuple[str, ...]] | None = None

    def __post_init__(self) -> None:
        if type(self.page) is not int or self.page < 1:
            raise ValueError("uData reuse list page must be a positive integer.")
        if type(self.page_size) is not int or self.page_size < 1:
            raise ValueError("uData reuse list page_size must be a positive integer.")
        if self.sort is not None and self.sort not in {*(f"-{name}" for name in _REUSE_SORTS), *_REUSE_SORTS}:
            raise ValueError("uData reuse list sort is not a documented choice.")
        if self.q is not None and not isinstance(self.q, str):
            raise ValueError("uData reuse list q must be a string when supplied.")
        if self.filters is not None:
            if not isinstance(self.filters, Mapping):
                raise ValueError("uData reuse list filters must be a mapping.")
            unknown = set(self.filters) - set(_REUSE_FILTERS)
            if unknown:
                raise ValueError(f"uData reuse list filters are not documented choices: {sorted(unknown)}.")

    def query_params(self) -> list[tuple[str, str]]:
        params: list[tuple[str, str]] = [("page", str(self.page)), ("page_size", str(self.page_size))]
        if self.q is not None:
            params.append(("q", self.q))
        if self.sort is not None:
            params.append(("sort", self.sort))
        for key, value in sorted((self.filters or {}).items()):
            if isinstance(value, bool):
                params.append((key, "true" if value else "false"))
            elif isinstance(value, tuple):
                params.extend((key, item) for item in value)
            else:
                params.append((key, value))
        return params


@dataclass(frozen=True, slots=True)
class ReuseFollowersQuery:
    """Stock follower query: pagination plus the optional user filter."""

    page: int = 1
    page_size: int = 20
    user: str | None = None

    def __post_init__(self) -> None:
        if type(self.page) is not int or self.page < 1:
            raise ValueError("uData reuse follower page must be a positive integer.")
        if type(self.page_size) is not int or self.page_size < 1:
            raise ValueError("uData reuse follower page_size must be a positive integer.")
        if self.user is not None and not self.user:
            raise ValueError("uData reuse follower user must be a non-empty string when supplied.")

    def query_params(self) -> list[tuple[str, str]]:
        params = [("page", str(self.page)), ("page_size", str(self.page_size))]
        if self.user is not None:
            params.append(("user", self.user))
        return params


@dataclass(frozen=True, slots=True)
class ReuseSearchQuery:
    """Stock v2 reuse search query surface."""

    q: str | None = None
    page: int = 1
    page_size: int = 50
    filters: Mapping[str, str | bool | tuple[str, ...]] | None = None

    def __post_init__(self) -> None:
        if type(self.page) is not int or self.page < 1:
            raise ValueError("uData reuse search page must be a positive integer.")
        if type(self.page_size) is not int or self.page_size < 1:
            raise ValueError("uData reuse search page_size must be a positive integer.")
        if self.q is not None and not isinstance(self.q, str):
            raise ValueError("uData reuse search q must be a string when supplied.")

    def query_params(self) -> list[tuple[str, str]]:
        params: list[tuple[str, str]] = [("page", str(self.page)), ("page_size", str(self.page_size))]
        if self.q is not None:
            params.append(("q", self.q))
        for key, value in sorted((self.filters or {}).items()):
            if isinstance(value, bool):
                params.append((key, "true" if value else "false"))
            elif isinstance(value, tuple):
                params.extend((key, item) for item in value)
            else:
                params.append((key, value))
        return params


def _validate_text(value: object, label: str, *, required: bool = False) -> None:
    if required and (not isinstance(value, str) or not value.strip()):
        raise ValueError(f"uData reuse {label} must be a non-empty string.")
    if not required and value is not None and (not isinstance(value, str) or not value.strip()):
        raise ValueError(f"uData reuse {label} must be a string when supplied.")


def _validate_tags(value: object, *, required: bool = False) -> None:
    if value is None and not required:
        return
    if not isinstance(value, tuple):
        valid = False
    else:
        valid = all(isinstance(tag, str) and tag for tag in value)
    if not valid:
        raise ValueError("uData reuse tags must be a tuple of non-empty strings when supplied.")


def _validate_mapping(value: object, label: str) -> None:
    if value is not None and not isinstance(value, Mapping):
        raise ValueError(f"uData reuse {label} must be a mapping when supplied.")


@dataclass(frozen=True, slots=True)
class ReuseCreateInput:
    """Presence-aware create payload for POST /api/1/reuses/."""

    title: str
    description: str
    type: str
    url: str
    tags: tuple[str, ...] = ()
    topic: str = ""
    organization: Mapping[str, str] | None = None
    private: bool | None = None
    extras: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        _validate_text(self.title, "title", required=True)
        _validate_text(self.description, "description", required=True)
        _validate_text(self.type, "type", required=True)
        _validate_text(self.url, "url", required=True)
        _validate_text(self.topic, "topic", required=True)
        _validate_tags(self.tags, required=True)
        _validate_mapping(self.organization, "organization")
        if self.private is not None and type(self.private) is not bool:
            raise ValueError("uData reuse private flag must be a boolean when supplied.")
        _validate_mapping(self.extras, "extras")
        if self.extras is not None:
            object.__setattr__(self, "extras", dict(self.extras))

    def payload(self) -> dict[str, object]:
        body: dict[str, object] = {
            "title": self.title,
            "description": self.description,
            "type": self.type,
            "url": self.url,
            "topic": self.topic,
        }
        if self.tags:
            body["tags"] = list(self.tags)
        if self.organization is not None:
            body["organization"] = dict(self.organization)
        if self.private is not None:
            body["private"] = self.private
        if self.extras is not None:
            body["extras"] = dict(self.extras)
        return body


@dataclass(frozen=True, slots=True)
class ReuseUpdateInput:
    """Presence-aware patch payload for PUT /api/1/reuses/<id>/."""

    title: str | None = None
    description: str | None = None
    type: str | None = None
    url: str | None = None
    tags: tuple[str, ...] | None = None
    topic: str | None = None
    organization: Mapping[str, str] | None = None
    private: bool | None = None
    extras: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        _validate_text(self.title, "title")
        _validate_text(self.description, "description")
        _validate_text(self.type, "type")
        _validate_text(self.url, "url")
        _validate_tags(self.tags)
        _validate_text(self.topic, "topic")
        _validate_mapping(self.organization, "organization")
        if self.private is not None and type(self.private) is not bool:
            raise ValueError("uData reuse private flag must be a boolean when supplied.")
        _validate_mapping(self.extras, "extras")
        if self.extras is not None:
            object.__setattr__(self, "extras", dict(self.extras))

    def payload(self) -> dict[str, object]:
        body: dict[str, object] = {}
        if self.title is not None:
            body["title"] = self.title
        if self.description is not None:
            body["description"] = self.description
        if self.type is not None:
            body["type"] = self.type
        if self.url is not None:
            body["url"] = self.url
        if self.tags is not None:
            body["tags"] = list(self.tags)
        if self.topic is not None:
            body["topic"] = self.topic
        if self.organization is not None:
            body["organization"] = dict(self.organization)
        if self.private is not None:
            body["private"] = self.private
        if self.extras is not None:
            body["extras"] = dict(self.extras)
        return body


@dataclass(frozen=True, slots=True)
class ReuseSuggestQuery:
    """Stock reuse suggest query: required ``q`` and optional ``size``."""

    q: str
    size: int = 10

    def __post_init__(self) -> None:
        if not isinstance(self.q, str) or not self.q:
            raise ValueError("uData reuse suggest q must be a non-empty string.")
        if type(self.size) is not int or self.size < 1:
            raise ValueError("uData reuse suggest size must be a positive integer.")

    def query_params(self) -> list[tuple[str, str]]:
        return [("q", self.q), ("size", str(self.size))]


@dataclass(frozen=True, slots=True)
class ReuseImageInput:
    """Bounded reuse image upload input; bytes never enter the result record."""

    data: bytes
    content_type: str

    def __post_init__(self) -> None:
        if not isinstance(self.data, (bytes, bytearray)):
            raise ValueError("uData reuse image data must be bytes.")
        if not isinstance(self.content_type, str) or not self.content_type:
            raise ValueError("uData reuse image content type must be a non-empty string.")

    def part(self) -> UploadPart:
        return UploadPart("file", bytes(self.data), "reuse-image", self.content_type)


@dataclass(frozen=True, slots=True)
class ReuseMutationResult:
    """Reuse mutation output retaining only a bounded record and a redacted receipt."""

    receipt: MutationReceipt
    record: MappingRecord | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "receipt": self.receipt.to_dict(),
            "record": self.record.to_dict() if self.record is not None else None,
        }
