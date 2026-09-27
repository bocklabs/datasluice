"""Immutable uData post, report, and notification inputs and mutation results."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import quote

from datasluice.domain.catalog.models import MappingRecord
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.errors.catalog import CatalogValidationError
from datasluice.runtime.transport.base import UploadPart

_POST_SORTS = ("created_at", "modified", "published", "name", "headline", "content")
_POST_SEARCH_SORTS = ("created", "last_modified", "published")
_POST_KINDS = ("news", "page")
_POST_BODY_TYPES = ("markdown", "html", "blocs")
_REPORT_REASONS = ("auto_spam", "explicit_content", "illegal_content", "others", "personal_data", "security", "spam")
_REPORT_SUBJECT_TYPES = ("Dataset", "Reuse", "Discussion", "Organization", "Dataservice", "User")
_NOTIFICATION_SORTS = ("created_at", "handled_at")


def segment(value: str, operation: str) -> str:
    if not isinstance(value, str) or not value or any(c in "/?#\"'" for c in value) or any(ord(c) < 32 for c in value):
        raise CatalogValidationError(
            f"uData post, report, or notification identifier for {operation} must be one URL-safe path segment.",
            operation=operation,
            platform="udata",
            safe_action="Pass a prior typed read identifier.",
        )
    return quote(value, safe="")


def _paging(query: list[tuple[str, str]], page: int, page_size: int) -> list[tuple[str, str]]:
    return [("page", str(page)), ("page_size", str(page_size)), *query]


def _validate_paging(page: int, page_size: int, label: str) -> None:
    if type(page) is not int or page < 1 or type(page_size) is not int or page_size < 1:
        raise ValueError(f"uData {label} paging must be positive integers.")


def _text(value: object, label: str, *, required: bool = False) -> None:
    if required and (not isinstance(value, str) or not value.strip()):
        raise ValueError(f"uData {label} must be a non-empty string.")
    if not required and value is not None and (not isinstance(value, str) or not value.strip()):
        raise ValueError(f"uData {label} must be a string when supplied.")


def _mapping(value: object, label: str) -> None:
    if value is not None and not isinstance(value, Mapping):
        raise ValueError(f"uData {label} must be a mapping when supplied.")


def _mappings(value: object, label: str) -> None:
    if value is not None and (not isinstance(value, tuple) or not all(isinstance(item, Mapping) for item in value)):
        raise ValueError(f"uData {label} must be a tuple of mappings when supplied.")


def _strings(value: object, label: str) -> None:
    if value is not None and (
        not isinstance(value, tuple) or not all(isinstance(item, str) and item for item in value)
    ):
        raise ValueError(f"uData {label} must be a tuple of non-empty strings when supplied.")


def _choice(value: object, choices: tuple[str, ...], label: str) -> None:
    if value is not None and (not isinstance(value, str) or value not in choices):
        raise ValueError(f"uData {label} is not a documented choice.")


@dataclass(frozen=True, slots=True)
class PostListQuery:
    """Stock v1 post collection query surface."""

    q: str | None = None
    page: int = 1
    page_size: int = 20
    sort: str | None = None
    kind: str | None = None
    with_drafts: bool | None = None

    def __post_init__(self) -> None:
        _validate_paging(self.page, self.page_size, "post list")
        _choice(self.kind, _POST_KINDS, "post list kind")
        if self.with_drafts is not None and type(self.with_drafts) is not bool:
            raise ValueError("uData post list with_drafts must be a boolean when supplied.")
        if self.sort is not None and self.sort not in {*(f"-{name}" for name in _POST_SORTS), *_POST_SORTS}:
            raise ValueError("uData post list sort is not a documented choice.")
        _text(self.q, "post list q")

    def query_params(self) -> list[tuple[str, str]]:
        params: list[tuple[str, str]] = []
        if self.q is not None:
            params.append(("q", self.q))
        if self.sort is not None:
            params.append(("sort", self.sort))
        if self.kind is not None:
            params.append(("kind", self.kind))
        if self.with_drafts is not None:
            params.append(("with_drafts", "true" if self.with_drafts else "false"))
        return _paging(params, self.page, self.page_size)


@dataclass(frozen=True, slots=True)
class PostSearchQuery:
    """Stock v2 post search query surface."""

    q: str | None = None
    page: int = 1
    page_size: int = 20
    sort: str | None = None
    tags: tuple[str, ...] | None = None
    last_update_range: str | None = None

    def __post_init__(self) -> None:
        _validate_paging(self.page, self.page_size, "post search")
        _text(self.q, "post search q")
        _strings(self.tags, "post search tags")
        _choice(
            self.last_update_range,
            ("last_30_days", "last_12_months", "last_3_years"),
            "post search last_update_range",
        )
        if self.sort is not None and self.sort not in {
            *(f"-{name}" for name in _POST_SEARCH_SORTS),
            *_POST_SEARCH_SORTS,
        }:
            raise ValueError("uData post search sort is not a documented choice.")

    def query_params(self) -> list[tuple[str, str]]:
        params: list[tuple[str, str]] = []
        if self.q is not None:
            params.append(("q", self.q))
        if self.sort is not None:
            params.append(("sort", self.sort))
        if self.last_update_range is not None:
            params.append(("last_update_range", self.last_update_range))
        for tag in self.tags or ():
            params.append(("tag", tag))
        return _paging(params, self.page, self.page_size)


@dataclass(frozen=True, slots=True)
class PostCreateInput:
    """Presence-aware create payload for POST /api/1/posts/."""

    name: str
    content: str | None = None
    headline: str | None = None
    body_type: str | None = None
    kind: str | None = None
    image_url: str | None = None
    credit_to: str | None = None
    credit_url: str | None = None
    tags: tuple[str, ...] | None = None
    datasets: tuple[Mapping[str, str], ...] | None = None
    reuses: tuple[Mapping[str, str], ...] | None = None
    blocs: tuple[Mapping[str, object], ...] | None = None

    def __post_init__(self) -> None:
        _text(self.name, "post name", required=True)
        for name in ("content", "headline", "image_url", "credit_to", "credit_url"):
            _text(getattr(self, name), f"post {name}")
        _choice(self.body_type, _POST_BODY_TYPES, "post body_type")
        _choice(self.kind, _POST_KINDS, "post kind")
        _strings(self.tags, "post tags")
        for name in ("datasets", "reuses", "blocs"):
            _mappings(getattr(self, name), f"post {name}")

    def payload(self) -> dict[str, object]:
        body: dict[str, object] = {"name": self.name}
        for name in ("content", "headline", "body_type", "kind", "image_url", "credit_to", "credit_url"):
            if (value := getattr(self, name)) is not None:
                body[name] = value
        if self.tags is not None:
            body["tags"] = list(self.tags)
        for name in ("datasets", "reuses", "blocs"):
            if (value := getattr(self, name)) is not None:
                body[name] = [dict(item) for item in value]
        return body


@dataclass(frozen=True, slots=True)
class PostUpdateInput:
    """Presence-aware patch payload for PUT /api/1/posts/<id>/."""

    name: str | None = None
    content: str | None = None
    headline: str | None = None
    body_type: str | None = None
    kind: str | None = None
    image_url: str | None = None
    credit_to: str | None = None
    credit_url: str | None = None
    tags: tuple[str, ...] | None = None
    datasets: tuple[Mapping[str, str], ...] | None = None
    reuses: tuple[Mapping[str, str], ...] | None = None
    blocs: tuple[Mapping[str, object], ...] | None = None

    def __post_init__(self) -> None:
        _text(self.name, "post name")
        for name in ("content", "headline", "image_url", "credit_to", "credit_url"):
            _text(getattr(self, name), f"post {name}")
        _choice(self.body_type, _POST_BODY_TYPES, "post body_type")
        _choice(self.kind, _POST_KINDS, "post kind")
        _strings(self.tags, "post tags")
        for name in ("datasets", "reuses", "blocs"):
            _mappings(getattr(self, name), f"post {name}")

    def payload(self) -> dict[str, object]:
        body: dict[str, object] = {}
        for name in ("name", "content", "headline", "body_type", "kind", "image_url", "credit_to", "credit_url"):
            if (value := getattr(self, name)) is not None:
                body[name] = value
        if self.tags is not None:
            body["tags"] = list(self.tags)
        for name in ("datasets", "reuses", "blocs"):
            if (value := getattr(self, name)) is not None:
                body[name] = [dict(item) for item in value]
        return body


@dataclass(frozen=True, slots=True)
class PostImageInput:
    """Bounded post image upload input; bytes never enter the result record."""

    data: bytes
    content_type: str

    def __post_init__(self) -> None:
        if not isinstance(self.data, (bytes, bytearray)):
            raise ValueError("uData post image data must be bytes.")
        if not isinstance(self.content_type, str) or not self.content_type:
            raise ValueError("uData post image content type must be a non-empty string.")

    def part(self) -> UploadPart:
        return UploadPart("file", bytes(self.data), "post-image", self.content_type)


@dataclass(frozen=True, slots=True)
class ReportQuery:
    """Stock v1 report collection query surface."""

    q: str | None = None
    page: int = 1
    page_size: int = 20
    sort: str | None = None
    handled: bool | None = None
    subject_type: str | None = None

    def __post_init__(self) -> None:
        _validate_paging(self.page, self.page_size, "report list")
        _text(self.q, "report list q")
        _choice(self.subject_type, _REPORT_SUBJECT_TYPES, "report list subject_type")
        if self.handled is not None and type(self.handled) is not bool:
            raise ValueError("uData report list handled must be a boolean when supplied.")
        if self.sort is not None and self.sort not in {"reported_at", "-reported_at"}:
            raise ValueError("uData report list sort is not a documented choice.")

    def query_params(self) -> list[tuple[str, str]]:
        params: list[tuple[str, str]] = []
        if self.q is not None:
            params.append(("q", self.q))
        if self.sort is not None:
            params.append(("sort", self.sort))
        if self.subject_type is not None:
            params.append(("subject_type", self.subject_type))
        if self.handled is not None:
            params.append(("handled", "true" if self.handled else "false"))
        return _paging(params, self.page, self.page_size)


@dataclass(frozen=True, slots=True)
class ReportCreateInput:
    """Presence-aware create payload for POST /api/1/reports/."""

    subject: Mapping[str, str]
    reason: str
    message: str | None = None
    dismissed_at: str | None = None
    dismissed_by: Mapping[str, str] | None = None
    subject_embed_id: str | None = None
    callbacks: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        _choice(self.reason, _REPORT_REASONS, "report reason")
        _text(self.message, "report message")
        _text(self.dismissed_at, "report dismissed_at")
        _text(self.subject_embed_id, "report subject_embed_id")
        _mapping(self.dismissed_by, "report dismissed_by")
        _mapping(self.callbacks, "report callbacks")
        if not isinstance(self.subject, Mapping) or not self.subject.get("id") or not self.subject.get("class"):
            raise ValueError("uData report subject must name both an id and a class.")
        if self.subject["class"] not in _REPORT_SUBJECT_TYPES:
            raise ValueError("uData report subject class is not a documented choice.")

    def payload(self) -> dict[str, object]:
        body: dict[str, object] = {"subject": dict(self.subject), "reason": self.reason}
        for name in ("message", "dismissed_at", "subject_embed_id"):
            if (value := getattr(self, name)) is not None:
                body[name] = value
        if self.dismissed_by is not None:
            body["dismissed_by"] = dict(self.dismissed_by)
        if self.callbacks is not None:
            body["callbacks"] = dict(self.callbacks)
        return body


@dataclass(frozen=True, slots=True)
class ReportUpdateInput:
    """Presence-aware patch payload for PATCH /api/1/reports/<id>/."""

    subject: Mapping[str, str] | None = None
    reason: str | None = None
    message: str | None = None
    dismissed_at: str | None = None
    dismissed_by: Mapping[str, str] | None = None
    subject_embed_id: str | None = None
    callbacks: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        _choice(self.reason, _REPORT_REASONS, "report reason")
        _text(self.message, "report message")
        _text(self.dismissed_at, "report dismissed_at")
        _text(self.subject_embed_id, "report subject_embed_id")
        _mapping(self.dismissed_by, "report dismissed_by")
        _mapping(self.callbacks, "report callbacks")
        if self.subject is not None:
            if not isinstance(self.subject, Mapping) or not self.subject.get("id") or not self.subject.get("class"):
                raise ValueError("uData report subject must name both an id and a class.")
            if self.subject["class"] not in _REPORT_SUBJECT_TYPES:
                raise ValueError("uData report subject class is not a documented choice.")

    def payload(self) -> dict[str, object]:
        body: dict[str, object] = {}
        for name in ("subject", "dismissed_by", "callbacks"):
            if (value := getattr(self, name)) is not None:
                body[name] = dict(value)
        for name in ("reason", "message", "dismissed_at", "subject_embed_id"):
            if (value := getattr(self, name)) is not None:
                body[name] = value
        return body


@dataclass(frozen=True, slots=True)
class NotificationQuery:
    """Stock current-user notification collection query surface."""

    page: int = 1
    page_size: int = 20
    sort: str | None = None
    handled: bool | None = None

    def __post_init__(self) -> None:
        _validate_paging(self.page, self.page_size, "notification list")
        _choice(self.sort, _NOTIFICATION_SORTS, "notification list sort")
        if self.handled is not None and type(self.handled) is not bool:
            raise ValueError("uData notification list handled must be a boolean when supplied.")

    def query_params(self) -> list[tuple[str, str]]:
        params: list[tuple[str, str]] = []
        if self.sort is not None:
            params.append(("sort", self.sort))
        if self.handled is not None:
            params.append(("handled", "true" if self.handled else "false"))
        return _paging(params, self.page, self.page_size)


@dataclass(frozen=True, slots=True)
class PostMutationResult:
    """Post/report/notification mutation output retaining only bounded data and a receipt."""

    receipt: MutationReceipt
    record: MappingRecord | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "receipt": self.receipt.to_dict(),
            "record": self.record.to_dict() if self.record is not None else None,
        }
