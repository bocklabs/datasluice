from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import ClassVar

from datasluice.connectors.catalog.udata.models._segment import paging
from datasluice.domain.catalog.models import _freeze_json
from datasluice.exceptions import DataSluiceError


def is_filter_value(value: object) -> bool:
    return isinstance(value, (str, bool)) or (
        type(value) is tuple and all(isinstance(item, str) and item for item in value)
    )


def validated_filters(
    filters: Mapping[str, str | bool | tuple[str, ...]] | None,
    *,
    subject: str,
    query: str,
    documented: frozenset[str] | tuple[str, ...],
    boolean: frozenset[str] | tuple[str, ...],
) -> None:
    if filters is None:
        return
    if not isinstance(filters, Mapping):
        raise ValueError(f"uData {subject} {query} filters must be a mapping.")
    unknown = set(filters) - set(documented)
    if unknown:
        raise ValueError(f"uData {subject} {query} filters are not documented choices: {sorted(unknown)}.")
    for key, value in filters.items():
        if not is_filter_value(value):
            raise ValueError(
                f"uData {subject} {query} filter {key!r} must be a string, boolean, or tuple of non-empty strings."
            )
        if key in boolean and type(value) is not bool:
            raise ValueError(f"uData {subject} {query} filter {key!r} must be a boolean.")


def freeze_mapping(value: Mapping[str, object], label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"uData {label} must be a mapping when supplied.")
    try:
        frozen = _freeze_json(dict(value), f"udata.{label}")
    except DataSluiceError as error:
        raise ValueError(f"uData {label} must contain JSON-safe values only.") from error
    if not isinstance(frozen, Mapping):
        raise ValueError(f"uData {label} must be a mapping when supplied.")
    return frozen


@dataclass(frozen=True, slots=True)
class FollowerQuery:
    page: int = 1
    page_size: int = 20
    user: str | None = None
    _paging_label: ClassVar[str] = ""
    _user_subject: ClassVar[str] = ""

    def __post_init__(self) -> None:
        paging(self.page, self.page_size, self._paging_label)
        if self.user is not None and (not isinstance(self.user, str) or not self.user):
            raise ValueError(f"uData {self._user_subject} follower user must be a non-empty string when supplied.")

    def query_params(self) -> list[tuple[str, str]]:
        params = [("page", str(self.page)), ("page_size", str(self.page_size))]
        if self.user is not None:
            params.append(("user", self.user))
        return params
