"""Immutable uData dataset inputs and typed query values."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datasluice.domain.catalog.models import NativeRecord

_V1_LIST_SORTS = frozenset({"title", "created", "last_update", "reuses", "followers", "views"})
_V1_LIST_FILTERS = frozenset(
    {
        "tag",
        "license",
        "featured",
        "geozone",
        "granularity",
        "temporal_coverage",
        "access_type",
        "organization",
        "badge",
        "organization_badge",
        "owner",
        "followed_by",
        "format",
        "schema",
        "schema_version",
        "topic",
        "credit",
        "dataservice",
        "reuse",
        "archived",
        "deleted",
        "private",
    }
)


_RESERVED_CREATE_FIELDS = frozenset({"title", "description", "private", "tags"})
_RESERVED_UPDATE_FIELDS = frozenset({"title", "description", "private", "tags"})


def _freeze_json(value: object) -> object:
    """Freeze a strictly JSON-safe structure into immutable equivalents."""
    if isinstance(value, Mapping):
        frozen: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("uData input mappings require string keys.")
            frozen[key] = _freeze_json(item)
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item) for item in value)
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        import math

        if not math.isfinite(value):
            raise ValueError("uData input mappings require finite JSON numbers.")
        return value
    raise ValueError(f"uData input mappings accept JSON-safe values only, got {type(value).__name__}.")


def _thaw_json(value: object) -> object:
    """Thaw a frozen structure into ordinary JSON containers for serialization."""
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw_json(item) for item in value]
    return value


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"uData dataset {field} must be a non-empty string.")
    return value


def _optional_text(value: object, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"uData dataset {field} must be a string when supplied.")
    return value


@dataclass(frozen=True, slots=True)
class DatasetListQuery:
    """The pinned v1/v2 dataset collection query surface.

    ``sort`` accepts the stock keys (optionally ``-`` prefixed); ``tag``
    accepts repeated values; ``featured``, ``archived``, ``deleted``, and
    ``private`` accept explicit booleans only.
    """

    q: str | None = None
    sort: str | None = None
    page: int = 1
    page_size: int = 20
    filters: Mapping[str, str | bool | tuple[str, ...]] | None = None

    def __post_init__(self) -> None:
        if type(self.page) is not int or self.page < 1:
            raise ValueError("Dataset list page must be a positive integer.")
        if type(self.page_size) is not int or self.page_size < 1:
            raise ValueError("Dataset list page_size must be a positive integer.")
        if self.sort is not None:
            key = self.sort.lstrip("-")
            if key not in _V1_LIST_SORTS:
                raise ValueError(f"Dataset list sort must be one of {sorted(_V1_LIST_SORTS)} with optional '-'.")
        if self.q is not None and not isinstance(self.q, str):
            raise ValueError("Dataset list q must be a string when supplied.")
        if self.filters is not None:
            if not isinstance(self.filters, Mapping):
                raise ValueError("Dataset list filters must be a mapping.")
            unknown = set(self.filters) - _V1_LIST_FILTERS
            if unknown:
                raise ValueError(f"Unknown dataset list filters: {sorted(unknown)}.")
            object.__setattr__(self, "filters", _freeze_json(dict(self.filters)))

    def query_params(self) -> list[tuple[str, str]]:
        """Encode the query into exact stock query-string parameter pairs."""
        params: list[tuple[str, str]] = [("page", str(self.page)), ("page_size", str(self.page_size))]
        if self.q is not None:
            params.append(("q", self.q))
        if self.sort is not None:
            params.append(("sort", self.sort))
        for key, value in sorted((self.filters or {}).items()):
            if isinstance(value, bool):
                params.append((key, "true" if value else "false"))
            elif isinstance(value, tuple):
                params.extend((key, _required_text(item, key)) for item in value)
            else:
                params.append((key, _required_text(value, key)))
        return params


@dataclass(frozen=True, slots=True)
class DatasetCreateInput:
    """Presence-aware create payload for POST /api/1/datasets/."""

    title: str
    description: str
    private: bool = False
    tags: tuple[str, ...] = ()
    fields: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        _required_text(self.title, "title")
        _required_text(self.description, "description")
        if type(self.private) is not bool:
            raise ValueError("Dataset create private flag must be a boolean.")
        if not isinstance(self.tags, tuple) or not all(isinstance(tag, str) and tag for tag in self.tags):
            raise ValueError("Dataset create tags must be a tuple of non-empty strings.")
        if self.fields is not None:
            if not isinstance(self.fields, Mapping):
                raise ValueError("Dataset create extra fields must be a mapping.")
            reserved = set(self.fields) & _RESERVED_CREATE_FIELDS
            if reserved:
                raise ValueError(f"Dataset create fields cannot override typed fields: {sorted(reserved)}.")
            object.__setattr__(self, "fields", _freeze_json(dict(self.fields)))

    def payload(self) -> dict[str, object]:
        """Encode the exact create JSON body."""
        body: dict[str, object] = {
            "title": self.title,
            "description": self.description,
            "private": self.private,
        }
        if self.tags:
            body["tags"] = list(self.tags)
        for key, value in (self.fields or {}).items():
            body[key] = _thaw_json(value)
        return body


@dataclass(frozen=True, slots=True)
class DatasetUpdateInput:
    """Presence-aware full-replace payload for PUT /api/1/datasets/<id>/.

    Only supplied fields are sent; absent fields are omitted from the JSON
    body exactly as the stock form performs a partial-safe replace.
    """

    title: str | None = None
    description: str | None = None
    private: bool | None = None
    tags: tuple[str, ...] | None = None
    fields: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        _optional_text(self.title, "title")
        _optional_text(self.description, "description")
        if self.private is not None and type(self.private) is not bool:
            raise ValueError("Dataset update private flag must be a boolean when supplied.")
        if self.tags is not None and (
            not isinstance(self.tags, tuple) or not all(isinstance(tag, str) and tag for tag in self.tags)
        ):
            raise ValueError("Dataset update tags must be a tuple of non-empty strings when supplied.")
        if self.fields is not None:
            if not isinstance(self.fields, Mapping):
                raise ValueError("Dataset update extra fields must be a mapping.")
            reserved = set(self.fields) & _RESERVED_UPDATE_FIELDS
            if reserved:
                raise ValueError(f"Dataset update fields cannot override typed fields: {sorted(reserved)}.")
            object.__setattr__(self, "fields", _freeze_json(dict(self.fields)))

    def payload(self) -> dict[str, object]:
        """Encode the exact update JSON body with omission semantics."""
        body: dict[str, object] = {}
        if self.title is not None:
            body["title"] = self.title
        if self.description is not None:
            body["description"] = self.description
        if self.private is not None:
            body["private"] = self.private
        if self.tags is not None:
            body["tags"] = list(self.tags)
        for key, value in (self.fields or {}).items():
            body[key] = _thaw_json(value)
        return body


@dataclass(frozen=True, slots=True)
class DatasetDeleteOptions:
    """DELETE options for /api/1/datasets/<id>/."""

    send_legal_notice: bool = False

    def __post_init__(self) -> None:
        if type(self.send_legal_notice) is not bool:
            raise ValueError("Dataset delete send_legal_notice must be a boolean.")

    def query_params(self) -> list[tuple[str, str]]:
        """Encode the exact delete query string."""
        if not self.send_legal_notice:
            return []
        return [("send_legal_notice", "true")]


@dataclass(frozen=True, slots=True)
class DatasetSuggestQuery:
    """GET /api/1/datasets/suggest/ parameters."""

    q: str
    size: int = 10

    def __post_init__(self) -> None:
        _required_text(self.q, "suggest q")
        if type(self.size) is not int or self.size < 1:
            raise ValueError("Dataset suggest size must be a positive integer.")

    def query_params(self) -> list[tuple[str, str]]:
        """Encode the exact suggest query string."""
        return [("q", self.q), ("size", str(self.size))]


@dataclass(frozen=True, slots=True)
class DatasetExtrasUpdate:
    """PUT /api/2/datasets/<id>/extras/ payload with null-deletes semantics."""

    values: Mapping[str, object]

    def __post_init__(self) -> None:
        if not isinstance(self.values, Mapping):
            raise ValueError("Dataset extras update values must be a mapping.")
        object.__setattr__(self, "values", _freeze_json(dict(self.values)))

    def payload(self) -> dict[str, object]:
        """Encode the exact extras JSON body."""
        return {key: _thaw_json(value) for key, value in self.values.items()}

    @property
    def removal_keys(self) -> list[str]:
        """Return the keys whose JSON null values delete the extra."""
        return [key for key, value in self.values.items() if value is None]


@dataclass(frozen=True, slots=True)
class DatasetExtrasDelete:
    """DELETE /api/2/datasets/<id>/extras/ payload."""

    keys: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.keys, tuple) or not all(isinstance(key, str) and key for key in self.keys):
            raise ValueError("Dataset extras delete keys must be a tuple of non-empty strings.")

    def payload(self) -> list[str]:
        """Encode the exact extras delete JSON body."""
        return list(self.keys)


@dataclass(frozen=True, slots=True)
class DatasetMutationOutcome:
    """A redacted mutation receipt for one dataset transition.

    Receipts retain only bounded metadata: operation identity, dataset
    identity, wire status, and outcome kind. Bodies and credentials never
    enter receipts (D-07/D-11).
    """

    operation_id: str
    dataset_id: str
    status_code: int
    outcome: str

    def __post_init__(self) -> None:
        for name in ("operation_id", "dataset_id", "outcome"):
            _required_text(getattr(self, name), name)
        if type(self.status_code) is not int or not 0 <= self.status_code <= 599:
            raise ValueError("Dataset mutation receipt requires a valid HTTP status code.")

    def to_dict(self) -> dict[str, object]:
        """Return the bounded redacted receipt projection for error metadata."""
        return {
            "schema_version": 1,
            "operation_id": self.operation_id,
            "dataset_id": self.dataset_id,
            "status_code": self.status_code,
            "outcome": self.outcome,
        }


_V2_SEARCH_FILTERS = frozenset(
    {
        "tag",
        "badge",
        "organization",
        "organization_badge",
        "organization_name",
        "owner",
        "license",
        "geozone",
        "granularity",
        "format",
        "schema",
        "temporal_coverage",
        "featured",
        "topic",
        "access_type",
        "format_family",
        "producer_type",
        "last_update_range",
    }
)
_V2_SEARCH_LAST_UPDATE_RANGES = frozenset({"last_30_days", "last_12_months", "last_3_years"})


@dataclass(frozen=True, slots=True)
class DatasetSearchQuery:
    """The documented v2 search surface (`DatasetSearch` request parser)."""

    q: str | None = None
    sort: str | None = None
    page: int = 1
    page_size: int = 20
    filters: Mapping[str, str | bool | tuple[str, ...]] | None = None

    def __post_init__(self) -> None:
        if type(self.page) is not int or self.page < 1:
            raise ValueError("Dataset search page must be a positive integer.")
        if type(self.page_size) is not int or self.page_size < 1:
            raise ValueError("Dataset search page_size must be a positive integer.")
        if self.sort is not None:
            key = self.sort.lstrip("-")
            if key not in _V1_LIST_SORTS:
                raise ValueError(f"Dataset search sort must be one of {sorted(_V1_LIST_SORTS)} with optional '-'.")
        if self.q is not None and not isinstance(self.q, str):
            raise ValueError("Dataset search q must be a string when supplied.")
        if self.filters is not None:
            if not isinstance(self.filters, Mapping):
                raise ValueError("Dataset search filters must be a mapping.")
            unknown = set(self.filters) - _V2_SEARCH_FILTERS
            if unknown:
                raise ValueError(f"Unknown dataset search filters: {sorted(unknown)}.")
            range_value = self.filters.get("last_update_range")
            if range_value is not None and range_value not in _V2_SEARCH_LAST_UPDATE_RANGES:
                raise ValueError("Dataset search last_update_range must be a documented range choice.")
            object.__setattr__(self, "filters", _freeze_json(dict(self.filters)))

    def query_params(self) -> list[tuple[str, str]]:
        """Encode the exact v2 search query-string parameter pairs."""
        params: list[tuple[str, str]] = [("page", str(self.page)), ("page_size", str(self.page_size))]
        if self.q is not None:
            params.append(("q", self.q))
        if self.sort is not None:
            params.append(("sort", self.sort))
        for key, value in sorted((self.filters or {}).items()):
            if isinstance(value, bool):
                params.append((key, "true" if value else "false"))
            elif isinstance(value, tuple):
                params.extend((key, _required_text(item, key)) for item in value)
            else:
                params.append((key, _required_text(value, key)))
        return params


@dataclass(frozen=True, slots=True)
class DatasetMutationResult:
    """The unified mutation outcome: a redacted receipt plus any returned value.

    Every dataset mutation produces this result on success; failures raise
    typed errors carrying the same bounded receipt in their metadata.
    """

    receipt: DatasetMutationOutcome
    record: NativeRecord | None = None
    extras: Mapping[str, object] | None = None
