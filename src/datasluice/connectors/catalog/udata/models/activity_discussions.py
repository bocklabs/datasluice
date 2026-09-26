"""Immutable uData activity and discussion inputs and mutation results."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import quote

from datasluice.domain.catalog.models import MappingRecord
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.errors.catalog import CatalogValidationError

_ACTIVITY_FILTERS = ("organization", "user", "related_to")


@dataclass(frozen=True, slots=True)
class ActivityQuery:
    """Optional stock activity filters; only supplied keys are sent."""

    user: str | None = None
    organization: str | None = None
    related_to: str | None = None

    def __post_init__(self) -> None:
        for name in _ACTIVITY_FILTERS:
            value = getattr(self, name)
            if value is not None and (not isinstance(value, str) or not value):
                raise ValueError(f"uData activity filter {name} must be a non-empty string when supplied.")

    def query(self) -> str:
        from urllib.parse import urlencode

        return urlencode([(name, value) for name in _ACTIVITY_FILTERS if (value := getattr(self, name))])


@dataclass(frozen=True, slots=True)
class DiscussionCreateInput:
    """Stock `DiscussionStart` body: the top-level comment opens the first message."""

    title: str
    comment: str
    subject: Mapping[str, str]
    organization: Mapping[str, str] | None = None
    extras: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        for name in ("title", "comment"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise ValueError(f"uData discussion {name} must be a non-empty string.")
        if not isinstance(self.subject, Mapping) or not self.subject.get("id") or not self.subject.get("class"):
            raise ValueError("uData discussion subject must name both an id and a class.")

    def payload(self) -> dict[str, object]:
        body: dict[str, object] = {
            "title": self.title,
            "comment": self.comment,
            "subject": dict(self.subject),
        }
        if self.organization is not None:
            body["organization"] = dict(self.organization)
        if self.extras is not None:
            body["extras"] = dict(self.extras)
        return body


@dataclass(frozen=True, slots=True)
class DiscussionUpdateInput:
    """Stock `DiscussionEdit` body: only the title is writable."""

    title: str

    def __post_init__(self) -> None:
        if not isinstance(self.title, str) or not self.title:
            raise ValueError("uData discussion title must be a non-empty string.")

    def payload(self) -> dict[str, str]:
        return {"title": self.title}


@dataclass(frozen=True, slots=True)
class CommentInput:
    """Stock `DiscussionResponse` and `DiscussionEditComment` body."""

    comment: str = ""
    organization: Mapping[str, str] | None = None
    close: bool | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.comment, str):
            raise ValueError("uData discussion comment must be a string.")
        if self.close is not None and not isinstance(self.close, bool):
            raise ValueError("uData discussion close flag must be a boolean when supplied.")
        if not self.comment and not self.close and self.organization is None:
            raise ValueError("uData discussion requires a comment, a close request, or an organization.")

    def payload(self) -> dict[str, object]:
        body: dict[str, object] = {}
        if self.comment:
            body["comment"] = self.comment
        if self.organization is not None:
            body["organization"] = dict(self.organization)
        if self.close is not None:
            body["close"] = self.close
        return body

    def edit_payload(self) -> dict[str, str]:
        if not self.comment:
            raise ValueError("uData discussion comment edit must supply a non-empty comment.")
        return {"comment": self.comment}


_DISCUSSION_SORTS = ("created", "closed")
_LAST_UPDATE_RANGES = ("last_30_days", "last_12_months", "last_3_years")


@dataclass(frozen=True, slots=True)
class DiscussionSearchQuery:
    """Stock `DiscussionSearch` request-parser arguments.

    `q` and the documented filters are omitted unless supplied, mirroring
    `DiscussionSearch.as_request_parser(store_missing=False)`. Paging keeps the
    stock `page`/`page_size` names and their documented defaults.
    """

    q: str | None = None
    page: int = 1
    page_size: int = 20
    sort: str | None = None
    closed: bool | None = None
    subject_ids: tuple[str, ...] = ()
    org: str | None = None
    user: str | None = None
    last_update_range: str | None = None

    def __post_init__(self) -> None:
        if self.page < 1 or self.page_size < 1:
            raise ValueError("uData discussion search paging must be strictly positive.")
        if self.sort is not None and self.sort not in {
            *(f"-{name}" for name in _DISCUSSION_SORTS),
            *_DISCUSSION_SORTS,
        }:
            raise ValueError("uData discussion search sort is not a documented choice.")
        if self.last_update_range is not None and self.last_update_range not in _LAST_UPDATE_RANGES:
            raise ValueError("uData discussion search last_update_range is not a documented choice.")

    def query(self) -> str:
        from urllib.parse import urlencode

        pairs: list[tuple[str, str]] = []
        if self.q is not None:
            pairs.append(("q", self.q))
        if self.closed is not None:
            pairs.append(("closed", "true" if self.closed else "false"))
        for subject_id in self.subject_ids:
            pairs.append(("for", subject_id))
        if self.org is not None:
            pairs.append(("org", self.org))
        if self.user is not None:
            pairs.append(("user", self.user))
        if self.last_update_range is not None:
            pairs.append(("last_update_range", self.last_update_range))
        if self.sort is not None:
            pairs.append(("sort", self.sort))
        pairs.extend((("page", str(self.page)), ("page_size", str(self.page_size))))
        return urlencode(pairs)


@dataclass(frozen=True, slots=True)
class DiscussionMutationResult:
    """Discussion mutation output retaining only a bounded record and a redacted receipt."""

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
            f"uData discussion identifier for {operation} must be one URL-safe path segment.",
            operation=operation,
            platform="udata",
            safe_action="Pass a prior typed read identifier.",
        )
    return quote(value, safe="")
