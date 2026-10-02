"""Immutable uData dataservice, relationship, and follower inputs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from datasluice.connectors.catalog.udata.models._segment import path_segment
from datasluice.domain.catalog.models import MappingRecord, _freeze_json, _thaw_json
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.exceptions import DataSluiceError

_LIST_FILTERS = (
    "tag",
    "topic",
    "organization",
    "organization_badge",
    "archived",
    "featured",
    "access_type",
    "producer_type",
    "last_update_range",
    "is_restricted",
    "dataset",
    "contact_point",
    "reuse",
)
_LIST_BOOLEAN_FILTERS = ("archived", "featured", "is_restricted")
_LIST_SORTS = ("created", "created_at", "last_modified", "title", "followers", "views")
_LIST_SORT_CHOICES = frozenset(_LIST_SORTS) | frozenset(f"-{name}" for name in _LIST_SORTS)
_SEARCH_FILTERS = (
    "tag",
    "topic",
    "organization",
    "archived",
    "featured",
    "access_type",
    "producer_type",
    "last_update_range",
    "is_restricted",
)
_SEARCH_BOOLEAN_FILTERS = ("archived", "featured", "is_restricted")
_SEARCH_SORTS = ("created", "views", "followers")
_SEARCH_SORT_CHOICES = frozenset(_SEARCH_SORTS) | frozenset(f"-{name}" for name in _SEARCH_SORTS)
_MAX_PAGE_SIZE = 100


def segment(value: str, operation: str) -> str:
    return path_segment(value, operation, "uData dataservice")


def _validated_filters(
    filters: Mapping[str, str | bool | tuple[str, ...]] | None,
    *,
    query: str,
    documented: frozenset[str] | tuple[str, ...],
    boolean: frozenset[str] | tuple[str, ...],
) -> None:
    if filters is None:
        return
    if not isinstance(filters, Mapping):
        raise ValueError(f"uData dataservice {query} filters must be a mapping.")
    unknown = set(filters) - set(documented)
    if unknown:
        raise ValueError(f"uData dataservice {query} filters are not documented choices: {sorted(unknown)}.")
    for key, value in filters.items():
        if not _is_filter_value(value):
            raise ValueError(
                f"uData dataservice {query} filter {key!r} must be a string, boolean, or tuple of non-empty strings."
            )
        if key in boolean and type(value) is not bool:
            raise ValueError(f"uData dataservice {query} filter {key!r} must be a boolean.")


def _is_filter_value(value: object) -> bool:
    return isinstance(value, (str, bool)) or (
        type(value) is tuple and all(isinstance(item, str) and item for item in value)
    )


def _frozen_mapping(value: Mapping[str, object], label: str) -> Mapping[str, object]:
    try:
        frozen = _freeze_json(dict(value), f"udata.{label}")
    except DataSluiceError as error:
        raise ValueError(f"uData {label} must contain JSON-safe values only.") from error
    if not isinstance(frozen, Mapping):
        raise ValueError(f"uData {label} must be a mapping when supplied.")
    return frozen


def _query_params(
    page: int,
    page_size: int,
    q: str | None,
    sort: str | None,
    filters: Mapping[str, object] | None,
) -> list[tuple[str, str]]:
    params: list[tuple[str, str]] = [("page", str(page)), ("page_size", str(page_size))]
    if q is not None:
        params.append(("q", q))
    if sort is not None:
        params.append(("sort", sort))
    for key, value in sorted((filters or {}).items()):
        if isinstance(value, bool):
            params.append((key, "true" if value else "false"))
        elif isinstance(value, tuple):
            params.extend((key, item) for item in value)
        else:
            params.append((key, cast_str(value)))
    return params


def cast_str(value: object) -> str:
    return value if isinstance(value, str) else str(value)


def _paging(page: int, page_size: int, label: str) -> None:
    if type(page) is not int or page < 1:
        raise ValueError(f"uData dataservice {label} page must be a positive integer.")
    if type(page_size) is not int or not 1 <= page_size <= _MAX_PAGE_SIZE:
        raise ValueError(f"uData dataservice {label} page_size must be an integer from 1 through {_MAX_PAGE_SIZE}.")


def _optional_text(value: object, label: str, *, required: bool = False) -> None:
    if required and (not isinstance(value, str) or not value.strip()):
        raise ValueError(f"uData dataservice {label} must be a non-empty string.")
    if not required and value is not None and (not isinstance(value, str) or not value.strip()):
        raise ValueError(f"uData dataservice {label} must be a string when supplied.")


@dataclass(frozen=True, slots=True)
class DataserviceListQuery:
    """Stock v1 dataservice collection query over the generated index parser."""

    q: str | None = None
    page: int = 1
    page_size: int = 20
    sort: str | None = None
    filters: Mapping[str, str | bool | tuple[str, ...]] | None = None

    def __post_init__(self) -> None:
        _paging(self.page, self.page_size, "list")
        if self.sort is not None and self.sort not in _LIST_SORT_CHOICES:
            raise ValueError("uData dataservice list sort is not a documented choice.")
        _optional_text(self.q, "list q")
        _validated_filters(
            self.filters,
            query="list",
            documented=frozenset(_LIST_FILTERS),
            boolean=frozenset(_LIST_BOOLEAN_FILTERS),
        )
        if self.filters is not None:
            object.__setattr__(self, "filters", _frozen_mapping(self.filters, "dataservice list filters"))

    def query_params(self) -> list[tuple[str, str]]:
        return _query_params(self.page, self.page_size, self.q, self.sort, self.filters)


@dataclass(frozen=True, slots=True)
class DataserviceSearchQuery:
    """Stock v2 dataservice search query; every optional key is omitted unless supplied."""

    q: str | None = None
    page: int = 1
    page_size: int = 50
    sort: str | None = None
    filters: Mapping[str, str | bool | tuple[str, ...]] | None = None

    def __post_init__(self) -> None:
        _paging(self.page, self.page_size, "search")
        if self.sort is not None and self.sort not in _SEARCH_SORT_CHOICES:
            raise ValueError("uData dataservice search sort is not a documented choice.")
        _optional_text(self.q, "search q")
        _validated_filters(
            self.filters,
            query="search",
            documented=frozenset(_SEARCH_FILTERS),
            boolean=frozenset(_SEARCH_BOOLEAN_FILTERS),
        )
        if self.filters is not None:
            object.__setattr__(self, "filters", _frozen_mapping(self.filters, "dataservice search filters"))

    def query_params(self) -> list[tuple[str, str]]:
        return _query_params(self.page, self.page_size, self.q, self.sort, self.filters)


@dataclass(frozen=True, slots=True)
class DataserviceFollowersQuery:
    """Stock follower query: pagination plus the optional user filter."""

    page: int = 1
    page_size: int = 20
    user: str | None = None

    def __post_init__(self) -> None:
        _paging(self.page, self.page_size, "follower")
        if self.user is not None and (not isinstance(self.user, str) or not self.user):
            raise ValueError("uData dataservice follower user must be a non-empty string when supplied.")

    def query_params(self) -> list[tuple[str, str]]:
        params = [("page", str(self.page)), ("page_size", str(self.page_size))]
        if self.user is not None:
            params.append(("user", self.user))
        return params


@dataclass(frozen=True, slots=True)
class DataserviceDeleteOptions:
    """DELETE options for /api/1/dataservices/<id>/."""

    send_legal_notice: bool = False

    def __post_init__(self) -> None:
        if type(self.send_legal_notice) is not bool:
            raise ValueError("uData dataservice delete send_legal_notice must be a boolean.")

    def query_params(self) -> list[tuple[str, str]]:
        """Encode the exact delete query string."""
        if not self.send_legal_notice:
            return []
        return [("send_legal_notice", "true")]


@dataclass(frozen=True, slots=True)
class DataserviceDatasetLinkInput:
    """The documented list of ``{"id": ...}`` objects the dataset relationship route accepts."""

    dataset_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.dataset_ids, tuple) or not self.dataset_ids:
            raise ValueError("uData dataservice dataset links must be a non-empty tuple of identifiers.")
        if not all(isinstance(dataset_id, str) and dataset_id for dataset_id in self.dataset_ids):
            raise ValueError("uData dataservice dataset links must be non-empty strings.")

    def payload(self) -> list[dict[str, str]]:
        return [{"id": dataset_id} for dataset_id in self.dataset_ids]


_DATASERVICE_TEXT = (
    "title",
    "base_api_url",
    "acronym",
    "description",
    "machine_documentation_url",
    "technical_documentation_url",
    "business_documentation_url",
    "rate_limiting",
    "rate_limiting_url",
    "availability_url",
)
_DATASERVICE_URLS = (
    "base_api_url",
    "machine_documentation_url",
    "technical_documentation_url",
    "business_documentation_url",
    "rate_limiting_url",
    "availability_url",
)
_DATASERVICE_FORMATS = ("REST", "WMS", "WSL")


def cast_mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError("uData dataservice value must be a mapping when supplied.")
    return value  # noqa


def _validated_dataservice_fields(fields: Mapping[str, object], *, required: frozenset[str]) -> None:
    """Validate every documented dataservice field, requiring the named keys when creating."""
    for key in _DATASERVICE_TEXT:
        _optional_text(fields.get(key), key, required=key in required)
    for key in _DATASERVICE_URLS:
        value = fields.get(key)
        if value is not None and not str(value).lower().startswith(("http://", "https://")):
            raise ValueError(f"uData dataservice {key} must be an absolute http(s) URL.")
    availability = fields.get("availability")
    if availability is not None and (
        isinstance(availability, bool) or not isinstance(availability, int | float) or not 0 <= availability <= 100
    ):
        raise ValueError("uData dataservice availability must be a number between 0 and 100.")
    if fields.get("format") is not None and fields["format"] not in _DATASERVICE_FORMATS:
        raise ValueError("uData dataservice format is not a stock choice.")
    _optional_text(fields.get("license"), "license")
    tags = fields.get("tags")
    if tags is not None and (not isinstance(tags, tuple) or not all(isinstance(tag, str) and tag for tag in tags)):
        raise ValueError("uData dataservice tags must be a tuple of non-empty strings when supplied.")
    if fields.get("private") is not None and type(fields["private"]) is not bool:
        raise ValueError("uData dataservice private flag must be a boolean when supplied.")


def _frozen_nested(instance: DataserviceCreateInput | DataserviceUpdateInput, labels: tuple[str, ...]) -> None:
    """Deep-freeze every supplied nested mapping in place, rejecting non-JSON values."""
    for label in labels:
        value = getattr(instance, label)
        if value is not None:
            object.__setattr__(instance, label, _frozen_mapping(cast_mapping(value), f"dataservice {label}"))


def _frozen_contact_points(instance: DataserviceCreateInput | DataserviceUpdateInput) -> None:
    """Deep-freeze the supplied contact-point tuple in place, rejecting non-mapping members."""
    points = instance.contact_points
    if points is None or points == ():
        return
    if not isinstance(points, tuple) or not all(isinstance(point, Mapping) for point in points):
        raise ValueError("uData dataservice contact points must be a tuple of mappings when supplied.")
    object.__setattr__(
        instance, "contact_points", tuple(_frozen_mapping(point, "dataservice contact point") for point in points)
    )


@dataclass(frozen=True, slots=True)
class DataserviceCreateInput:
    """Presence-aware create payload for POST /api/1/dataservices/."""

    title: str
    base_api_url: str
    acronym: str | None = None
    description: str | None = None
    machine_documentation_url: str | None = None
    technical_documentation_url: str | None = None
    business_documentation_url: str | None = None
    rate_limiting: str | None = None
    rate_limiting_url: str | None = None
    availability: float | None = None
    availability_url: str | None = None
    format: str | None = None
    license: str | None = None
    tags: tuple[str, ...] = ()
    private: bool | None = None
    organization: Mapping[str, str] | None = None
    access_type: Mapping[str, object] | None = None
    contact_points: tuple[Mapping[str, object], ...] = ()
    extras: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        _validated_dataservice_fields(
            {name: getattr(self, name) for name in self.__dataclass_fields__},
            required=frozenset({"title", "base_api_url"}),
        )
        _frozen_nested(self, ("organization", "access_type", "extras"))
        _frozen_contact_points(self)

    def payload(self) -> dict[str, object]:
        body: dict[str, object] = {"title": self.title, "base_api_url": self.base_api_url}
        for key in ("acronym", "description", "rate_limiting"):
            value = getattr(self, key)
            if value is not None:
                body[key] = value
        for key in _DATASERVICE_URLS[1:]:
            value = getattr(self, key)
            if value is not None:
                body[key] = value
        for key in ("availability", "format", "license", "private"):
            value = getattr(self, key)
            if value is not None:
                body[key] = value
        if self.tags:
            body["tags"] = list(self.tags)
        if self.organization is not None:
            body["organization"] = _thaw_json(self.organization)
        if self.access_type is not None:
            body["access_type"] = _thaw_json(self.access_type)
        if self.contact_points:
            body["contact_points"] = [_thaw_json(point) for point in self.contact_points]
        if self.extras is not None:
            body["extras"] = _thaw_json(self.extras)
        return body


@dataclass(frozen=True, slots=True)
class DataserviceUpdateInput:
    """Presence-aware patch payload for PATCH /api/1/dataservices/<id>/."""

    title: str | None = None
    base_api_url: str | None = None
    acronym: str | None = None
    description: str | None = None
    machine_documentation_url: str | None = None
    technical_documentation_url: str | None = None
    business_documentation_url: str | None = None
    rate_limiting: str | None = None
    rate_limiting_url: str | None = None
    availability: float | None = None
    availability_url: str | None = None
    format: str | None = None
    license: str | None = None
    tags: tuple[str, ...] | None = None
    private: bool | None = None
    organization: Mapping[str, str] | None = None
    access_type: Mapping[str, object] | None = None
    contact_points: tuple[Mapping[str, object], ...] | None = None
    extras: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        if all(getattr(self, name) is None for name in self.__dataclass_fields__):
            raise ValueError("uData dataservice updates require at least one field.")
        _validated_dataservice_fields(
            {name: getattr(self, name) for name in self.__dataclass_fields__},
            required=frozenset(),
        )
        _frozen_nested(self, ("organization", "access_type", "extras"))
        _frozen_contact_points(self)

    def payload(self) -> dict[str, object]:
        body: dict[str, object] = {}
        for key in _DATASERVICE_TEXT:
            value = getattr(self, key)
            if value is not None:
                body[key] = value
        for key in ("availability", "format", "license", "private"):
            value = getattr(self, key)
            if value is not None:
                body[key] = value
        if self.tags is not None:
            body["tags"] = list(self.tags)
        for key in ("organization", "access_type", "extras"):
            value = getattr(self, key)
            if value is not None:
                body[key] = _thaw_json(value)
        if self.contact_points is not None:
            body["contact_points"] = [_thaw_json(point) for point in self.contact_points]
        return body


@dataclass(frozen=True, slots=True)
class DataserviceMutationResult:
    """Dataservice mutation output retaining only a bounded record and a redacted receipt."""

    receipt: MutationReceipt
    record: MappingRecord | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "receipt": self.receipt.to_dict(),
            "record": self.record.to_dict() if self.record is not None else None,
        }
