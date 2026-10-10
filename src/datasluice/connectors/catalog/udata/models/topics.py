"""Immutable uData topic, topic-element, and feature-state inputs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from datasluice.connectors.catalog.udata.models._segment import path_segment
from datasluice.connectors.catalog.udata.models.reuses import linked_identifier
from datasluice.domain.catalog.models import MappingRecord, _freeze_json, _thaw_json
from datasluice.exceptions import DataSluiceError

if TYPE_CHECKING:
    from datasluice.domain.catalog.receipts import MutationReceipt

_MAX_PAGE_SIZE = 100
_DEFAULT_PAGE_SIZE = 20
_TOPIC_SORTS = ("name", "created", "last_modified")
_TOPIC_LIST_FILTERS = (
    "dataset",
    "dataservice",
    "featured",
    "geozone",
    "granularity",
    "organization",
    "owner",
    "private",
    "reuse",
    "tag",
)
_TOPIC_LIST_BOOLEAN_FILTERS = frozenset({"private", "featured"})
_TOPIC_ELEMENT_CLASS_CHOICES = frozenset({"Dataset", "Dataservice", "None", "Reuse"})
_TOPIC_SEARCH_FILTERS = ("featured", "last_update_range", "organization", "producer_type", "tag")
_TOPIC_SEARCH_BOOLEAN_FILTERS = frozenset({"featured"})
_TOPIC_SEARCH_LAST_UPDATE_RANGES = frozenset({"last_30_days", "last_12_months", "last_3_years"})
_TOPIC_SEARCH_PRODUCER_TYPES = frozenset(
    {
        "association",
        "company",
        "local-authority",
        "not-specified",
        "public-service",
        "user",
    }
)
_ELEMENT_REFERENCE_CLASSES = ("Dataset", "Dataservice", "Reuse")


def segment(value: str, operation: str) -> str:
    """Return *value* quoted as one URL-safe topic path segment."""
    return path_segment(value, operation, "Topic")


def _paging(page: int, page_size: int, label: str) -> None:
    """Bound the stock page and page_size pair before a request is ever built."""
    if type(page) is not int or page < 1:
        raise ValueError(f"uData topic {label} page must be a positive integer.")
    if type(page_size) is not int or not 1 <= page_size <= _MAX_PAGE_SIZE:
        raise ValueError(f"uData topic {label} page_size must be an integer from 1 through {_MAX_PAGE_SIZE}.")


def _optional_text(value: object, label: str, *, required: bool = False) -> None:
    if required and (not isinstance(value, str) or not value):
        raise ValueError(f"uData topic {label} must be a non-empty string.")
    if not required and value is not None and not isinstance(value, str):
        raise ValueError(f"uData topic {label} must be a string when supplied.")


def _is_filter_value(value: object) -> bool:
    return isinstance(value, (str, bool)) or (
        type(value) is tuple and all(isinstance(item, str) and item for item in value)
    )


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
        raise ValueError(f"uData topic {query} filters must be a mapping.")
    unknown = set(filters) - set(documented)
    if unknown:
        raise ValueError(f"uData topic {query} filters are not documented choices: {sorted(unknown)}.")
    for key, value in filters.items():
        if not _is_filter_value(value):
            raise ValueError(
                f"uData topic {query} filter {key!r} must be a string, boolean, or tuple of non-empty strings."
            )
        if key in boolean and type(value) is not bool:
            raise ValueError(f"uData topic {query} filter {key!r} must be a boolean.")


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
            params.append((key, str(value)))
    return params


def _validated_sort(value: object, label: str) -> None:
    if value is None:
        return
    if not isinstance(value, str) or not value:
        raise ValueError(f"uData topic {label} sort must be a non-empty string when supplied.")
    key = value[1:] if value.startswith("-") else value
    if key.startswith("-") or key not in _TOPIC_SORTS:
        raise ValueError(f"uData topic {label} sort is not a documented choice.")


def _element_reference(value: object, operation: str) -> Mapping[str, str]:
    """Freeze one documented ``{"class": ..., "id": ...}`` element reference body."""
    if not isinstance(value, Mapping):
        raise ValueError("uData topic element reference must be a mapping when supplied.")
    missing = {"class", "id"} - set(value)
    if missing:
        raise ValueError(f"uData topic element reference is missing documented keys: {sorted(missing)}.")
    element_class = value["class"]
    element_id = value["id"]
    if not isinstance(element_class, str) or element_class not in _ELEMENT_REFERENCE_CLASSES:
        raise ValueError("uData topic element reference class is not a stock element class.")
    if not isinstance(element_id, str):
        raise ValueError("uData topic element reference id must be a string.")
    linked_identifier(element_id, operation, "uData topic element")
    return {"class": element_class, "id": element_id}


@dataclass(frozen=True, slots=True)
class TopicListQuery:
    """Stock v2 topic collection query over the pinned ``TopicApiParser``.

    ``q`` and every documented filter are omitted unless supplied, while
    ``page``/``page_size`` keep their stock names and defaults.
    """

    q: str | None = None
    page: int = 1
    page_size: int = _DEFAULT_PAGE_SIZE
    sort: str | None = None
    filters: Mapping[str, str | bool | tuple[str, ...]] | None = None

    def __post_init__(self) -> None:
        _paging(self.page, self.page_size, "list")
        _validated_sort(self.sort, "list")
        _optional_text(self.q, "list q")
        _validated_filters(
            self.filters,
            query="list",
            documented=frozenset(_TOPIC_LIST_FILTERS),
            boolean=_TOPIC_LIST_BOOLEAN_FILTERS,
        )
        if self.filters is not None:
            object.__setattr__(self, "filters", _frozen_mapping(self.filters, "topic list filters"))

    def query_params(self) -> list[tuple[str, str]]:
        return _query_params(self.page, self.page_size, self.q, self.sort, self.filters)


@dataclass(frozen=True, slots=True)
class TopicSearchQuery:
    """Stock v2 topic search query over the pinned ``TopicSearch`` request parser."""

    q: str | None = None
    page: int = 1
    page_size: int = _DEFAULT_PAGE_SIZE
    sort: str | None = None
    filters: Mapping[str, str | bool | tuple[str, ...]] | None = None

    def __post_init__(self) -> None:
        _paging(self.page, self.page_size, "search")
        _validated_sort(self.sort, "search")
        _optional_text(self.q, "search q")
        _validated_filters(
            self.filters,
            query="search",
            documented=frozenset(_TOPIC_SEARCH_FILTERS),
            boolean=_TOPIC_SEARCH_BOOLEAN_FILTERS,
        )
        if self.filters is not None:
            update_range = self.filters.get("last_update_range")
            if update_range is not None and (
                not isinstance(update_range, str) or update_range not in _TOPIC_SEARCH_LAST_UPDATE_RANGES
            ):
                raise ValueError("uData topic search last_update_range must be a documented range choice.")
            producer_type = self.filters.get("producer_type")
            if producer_type is not None and (
                not isinstance(producer_type, str) or producer_type not in _TOPIC_SEARCH_PRODUCER_TYPES
            ):
                raise ValueError("uData topic search producer_type must be a documented producer type.")
            object.__setattr__(self, "filters", _frozen_mapping(self.filters, "topic search filters"))

    def query_params(self) -> list[tuple[str, str]]:
        return _query_params(self.page, self.page_size, self.q, self.sort, self.filters)


@dataclass(frozen=True, slots=True)
class TopicElementsQuery:
    """Stock topic-element query: bounded paging plus the documented element filters."""

    page: int = 1
    page_size: int = _DEFAULT_PAGE_SIZE
    element_class: str | None = None
    q: str | None = None
    tag: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _paging(self.page, self.page_size, "element")
        if self.element_class is not None and self.element_class not in _TOPIC_ELEMENT_CLASS_CHOICES:
            raise ValueError("uData topic element class filter is not a documented element class.")
        _optional_text(self.q, "element q")
        if not isinstance(self.tag, tuple) or not all(isinstance(item, str) and item for item in self.tag):
            raise ValueError("uData topic element tag filter must be a tuple of non-empty strings.")

    def query_params(self) -> list[tuple[str, str]]:
        params: list[tuple[str, str]] = [("page", str(self.page)), ("page_size", str(self.page_size))]
        if self.element_class is not None:
            params.append(("class", self.element_class))
        if self.q is not None:
            params.append(("q", self.q))
        params.extend(("tag", item) for item in self.tag)
        return params


@dataclass(frozen=True, slots=True)
class TopicElementInput:
    """One stock topic-element body: the documented element fields and linked reference.

    The pinned model rejects an element carrying neither a title nor a linked
    element, so that check is enforced before dispatch rather than by a 400.
    """

    title: str | None = None
    description: str | None = None
    tags: tuple[str, ...] | None = None
    extras: Mapping[str, object] | None = None
    element: Mapping[str, str] | None = None

    def __post_init__(self) -> None:
        _optional_text(self.title, "element title")
        _optional_text(self.description, "element description")
        if self.tags is not None and (
            not isinstance(self.tags, tuple) or not all(isinstance(tag, str) and tag for tag in self.tags)
        ):
            raise ValueError("uData topic element tags must be a tuple of non-empty strings when supplied.")
        if self.extras is not None:
            object.__setattr__(self, "extras", _frozen_mapping(self.extras, "topic element extras"))
        if self.element is not None:
            operation = "udata/api-v2.topic-element"
            object.__setattr__(self, "element", _element_reference(self.element, operation))
        if self.title is None and self.element is None:
            raise ValueError("uData topic element must carry a title or a linked element.")

    def payload(self) -> dict[str, object]:
        body: dict[str, object] = {}
        if self.title is not None:
            body["title"] = self.title
        if self.description is not None:
            body["description"] = self.description
        if self.tags is not None:
            body["tags"] = list(self.tags)
        if self.extras is not None:
            body["extras"] = _thaw_json(self.extras)
        if self.element is not None:
            body["element"] = _thaw_json(self.element)
        return body


@dataclass(frozen=True, slots=True)
class TopicElementLink:
    """One documented element reference the topic element collection accepts."""

    element_class: str
    element_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.element_class, str) or self.element_class not in _ELEMENT_REFERENCE_CLASSES:
            raise ValueError("uData topic element link class is not a stock element class.")
        if not isinstance(self.element_id, str) or not self.element_id:
            raise ValueError("uData topic element link id must be a non-empty string.")
        linked_identifier(self.element_id, "udata/api-v2.topic-elements-create", "uData topic element")

    def payload(self) -> dict[str, str]:
        return {"class": self.element_class, "id": self.element_id}


@dataclass(frozen=True, slots=True)
class TopicElementsCreateInput:
    """The documented list of element references POST /elements/ accepts."""

    elements: tuple[TopicElementLink, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.elements, tuple) or not self.elements:
            raise ValueError("uData topic elements must be a non-empty tuple of element links.")
        if not all(isinstance(link, TopicElementLink) for link in self.elements):
            raise ValueError("uData topic elements must be documented element links.")

    def payload(self) -> list[dict[str, str]]:
        return [link.payload() for link in self.elements]


@dataclass(frozen=True, slots=True)
class TopicCreateInput:
    """Presence-aware create payload for POST /api/2/topics/."""

    name: str
    description: str | None = None
    tags: tuple[str, ...] = ()
    private: bool | None = None
    color: int | None = None
    extras: Mapping[str, object] | None = None
    organization: Mapping[str, object] | None = None
    owner: Mapping[str, object] | None = None
    spatial: Mapping[str, object] | None = None
    elements: tuple[TopicElementInput, ...] = ()

    def __post_init__(self) -> None:
        _optional_text(self.name, "name", required=True)
        _optional_text(self.description, "description")
        if not isinstance(self.tags, tuple) or not all(isinstance(tag, str) and tag for tag in self.tags):
            raise ValueError("uData topic tags must be a tuple of non-empty strings.")
        if self.private is not None and type(self.private) is not bool:
            raise ValueError("uData topic private flag must be a boolean when supplied.")
        if self.color is not None and type(self.color) is not int:
            raise ValueError("uData topic color must be an integer when supplied.")
        for label in ("extras", "organization", "owner", "spatial"):
            value = getattr(self, label)
            if value is not None:
                object.__setattr__(self, label, _frozen_mapping(value, f"topic {label}"))
        if not isinstance(self.elements, tuple) or not all(
            isinstance(element, TopicElementInput) for element in self.elements
        ):
            raise ValueError("uData topic elements must be a tuple of topic element inputs.")

    def payload(self) -> dict[str, object]:
        body: dict[str, object] = {"name": self.name}
        if self.description is not None:
            body["description"] = self.description
        if self.tags:
            body["tags"] = list(self.tags)
        if self.private is not None:
            body["private"] = self.private
        if self.color is not None:
            body["color"] = self.color
        for label in ("extras", "organization", "owner", "spatial"):
            value = getattr(self, label)
            if value is not None:
                body[label] = _thaw_json(value)
        if self.elements:
            body["elements"] = [element.payload() for element in self.elements]
        return body


@dataclass(frozen=True, slots=True)
class TopicUpdateInput:
    """Presence-aware replace payload for PUT /api/2/topics/<topic>/."""

    name: str | None = None
    description: str | None = None
    tags: tuple[str, ...] | None = None
    private: bool | None = None
    color: int | None = None
    extras: Mapping[str, object] | None = None
    organization: Mapping[str, object] | None = None
    owner: Mapping[str, object] | None = None
    spatial: Mapping[str, object] | None = None
    elements: tuple[TopicElementInput, ...] | None = None

    def __post_init__(self) -> None:
        if all(getattr(self, name) is None for name in self.__dataclass_fields__):
            raise ValueError("uData topic updates require at least one field.")
        _optional_text(self.name, "name")
        _optional_text(self.description, "description")
        if self.tags is not None and (
            not isinstance(self.tags, tuple) or not all(isinstance(tag, str) and tag for tag in self.tags)
        ):
            raise ValueError("uData topic tags must be a tuple of non-empty strings when supplied.")
        if self.private is not None and type(self.private) is not bool:
            raise ValueError("uData topic private flag must be a boolean when supplied.")
        if self.color is not None and type(self.color) is not int:
            raise ValueError("uData topic color must be an integer when supplied.")
        for label in ("extras", "organization", "owner", "spatial"):
            value = getattr(self, label)
            if value is not None:
                object.__setattr__(self, label, _frozen_mapping(value, f"topic {label}"))
        if self.elements is not None and not isinstance(self.elements, tuple):
            raise ValueError("uData topic elements must be a tuple of topic element inputs when supplied.")

    def payload(self) -> dict[str, object]:
        body: dict[str, object] = {}
        for key in ("name", "description", "tags", "private", "color"):
            value = getattr(self, key)
            if value is not None:
                body[key] = list(value) if key == "tags" else value
        for label in ("extras", "organization", "owner", "spatial"):
            value = getattr(self, label)
            if value is not None:
                body[label] = _thaw_json(value)
        if self.elements is not None:
            body["elements"] = [element.payload() for element in self.elements]
        return body


@dataclass(frozen=True, slots=True)
class TopicMutationResult:
    """Topic mutation output retaining only a bounded record and a redacted receipt."""

    receipt: MutationReceipt
    record: MappingRecord | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "receipt": self.receipt.to_dict(),
            "record": self.record.to_dict() if self.record is not None else None,
        }
