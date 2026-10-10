"""Immutable uData contact-point and visualization inputs, records, and mutation results."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from datasluice.connectors.catalog.udata.models._segment import path_segment
from datasluice.domain.catalog.models import MappingRecord, _freeze_json, _thaw_json
from datasluice.exceptions import DataSluiceError

if TYPE_CHECKING:
    from datasluice.domain.catalog.receipts import MutationReceipt
    from datasluice.runtime.transport.base import UploadPart

MAX_PAGE_SIZE = 100
DEFAULT_PAGE_SIZE = 20
MAX_FIELD_LENGTH = 255
CONTACT_ROLES = (
    "contact",
    "creator",
    "publisher",
    "rightsHolder",
    "custodian",
    "distributor",
    "originator",
    "principalInvestigator",
    "processor",
    "resourceProvider",
    "user",
)
CONTACT_ROLE_CONTACT = "contact"
VISUALIZATION_SORTS = ("title", "created", "last_modified")
X_AXIS_TYPES = ("discrete", "continuous")
X_AXIS_SORT_BY = ("axis_x", "axis_y")
X_AXIS_SORT_DIRECTIONS = ("asc", "desc")
FILTER_CONDITIONS = (
    "exact",
    "differs",
    "is_null",
    "is_not_null",
    "greater",
    "less",
    "strictly_greater",
    "strictly_less",
)
SERIES_TYPES = ("line", "histogram")
SERIES_AGGREGATES = ("avg", "sum", "count", "min", "max")
UNIT_POSITIONS = ("prefix", "suffix")
_IMAGE_MEDIA_TYPES = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
}
IMAGE_MEDIA_TYPES = tuple(_IMAGE_MEDIA_TYPES)
VISUALIZATION_PAGE_KIND = "udata_visualization_page"


_CONTACT_POINT_NAME = "contact point name"


def segment(value: str, operation: str) -> str:
    """Return one URL-safe contact-point or visualization path segment."""
    return path_segment(value, operation, "uData contact point or visualization")


def linked_identifier(value: str, operation: str, label: str) -> str:
    """Return *value* after checking it is one documented relationship identifier.

    Owner and organization identifiers travel in a JSON body rather than in a URL
    path, so the shared segment validator is applied for its identifier policy and
    the value itself is returned unencoded.

    Args:
        value: The caller-supplied identifier destined for a relationship body.
        operation: The owning operation name, used to build the error details.
        label: The owning linked resource label used in the error message.

    Returns:
        The unchanged identifier, ready to send as a body value.

    Raises:
        CatalogValidationError: If the identifier is not one safe identifier.
    """
    path_segment(value, operation, label)
    return value


def _text(value: object, label: str, *, required: bool = False) -> None:
    if required and (not isinstance(value, str) or not value.strip()):
        raise ValueError(f"uData {label} must be a non-empty string.")
    if not required and value is not None and (not isinstance(value, str) or not value.strip()):
        raise ValueError(f"uData {label} must be a string when supplied.")
    if isinstance(value, str) and len(value) > MAX_FIELD_LENGTH:
        raise ValueError(f"uData {label} must be at most {MAX_FIELD_LENGTH} characters.")


def _no_url(value: object, label: str) -> None:
    if isinstance(value, str) and "://" in value:
        raise ValueError(f"uData {label} must not carry a URL.")


def _email(value: object, label: str) -> None:
    if value is None:
        return
    valid = isinstance(value, str) and value == value.strip() and len(value) <= MAX_FIELD_LENGTH
    if isinstance(value, str) and valid:
        local, separator, domain = value.rpartition("@")
        labels = domain.split(".")
        valid = bool(
            separator
            and local
            and not any(character.isspace() for character in value)
            and len(labels) >= 2
            and all(labels)
            and labels[-1].isalpha()
            and len(labels[-1]) >= 2
        )
    if not valid:
        raise ValueError(f"uData {label} must be a valid email address.")


def _url(value: object, label: str) -> None:
    if value is not None and not str(value).lower().startswith(("http://", "https://")):
        raise ValueError(f"uData {label} must be an absolute http(s) URL.")


def _choice(value: object, choices: tuple[str, ...], label: str) -> None:
    if value is not None and (not isinstance(value, str) or value not in choices):
        raise ValueError(f"uData {label} is not a documented choice.")


def _paging(page: object, page_size: object, label: str) -> None:
    if type(page) is not int or page < 1:
        raise ValueError(f"uData {label} page must be a positive integer.")
    if type(page_size) is not int or not 1 <= page_size <= MAX_PAGE_SIZE:
        raise ValueError(f"uData {label} page_size must be an integer from 1 through {MAX_PAGE_SIZE}.")


def _frozen_mapping(value: Mapping[str, object], label: str) -> Mapping[str, object]:
    """Return *value* deep-frozen, rejecting anything that is not JSON serializable."""
    try:
        frozen = _freeze_json(dict(value), f"udata.{label}")
    except DataSluiceError as error:
        raise ValueError(f"uData {label} must contain JSON-safe values only.") from error
    if not isinstance(frozen, Mapping):
        raise ValueError(f"uData {label} must be a mapping when supplied.")
    return frozen


def _frozen_mappings(value: object, label: str) -> tuple[Mapping[str, object], ...]:
    """Return *value* as a tuple of deep-frozen JSON mappings."""
    if value is None:
        return ()
    if not isinstance(value, tuple) or not all(isinstance(item, Mapping) for item in value):
        raise ValueError(f"uData {label} must be a tuple of mappings when supplied.")
    return tuple(_frozen_mapping(item, label) for item in value)


def _validate_x_axis(axis: Mapping[str, object]) -> None:
    required = ("column_x", "type")
    for key in required:
        _text(axis.get(key), f"visualization x_axis {key}", required=True)
    for key in ("sort_x_by", "sort_x_direction"):
        _choice(axis.get(key), (X_AXIS_SORT_BY + X_AXIS_SORT_DIRECTIONS), f"visualization x_axis {key}")
    _choice(axis.get("type"), X_AXIS_TYPES, "visualization x_axis type")
    unknown = set(axis) - {"column_x", "type", "sort_x_by", "sort_x_direction"}
    if unknown:
        raise ValueError(f"uData visualization x_axis keys are not documented: {sorted(unknown)}.")


def _validate_y_axis(axis: Mapping[str, object]) -> None:
    for key in ("label", "unit"):
        _text(axis.get(key), f"visualization y_axis {key}")
    _choice(axis.get("unit_position"), UNIT_POSITIONS, "visualization y_axis unit_position")
    unknown = set(axis) - {"min", "max", "label", "unit", "unit_position"}
    if unknown:
        raise ValueError(f"uData visualization y_axis keys are not documented: {sorted(unknown)}.")


def _validate_series_filters(filters: object) -> None:
    if filters is not None:
        if not isinstance(filters, Mapping):
            raise ValueError("uData visualization series filters must be a mapping when supplied.")
        if "filters" in filters:
            nested = _frozen_mappings(filters.get("filters"), "visualization series filter")
            for member in nested:
                _text(member.get("column"), "visualization series filter column", required=True)
                _choice(member.get("condition"), FILTER_CONDITIONS, "visualization series filter condition")
        else:
            _text(filters.get("column"), "visualization series filter column", required=True)
            _choice(filters.get("condition"), FILTER_CONDITIONS, "visualization series filter condition")


def _validate_series(series: tuple[Mapping[str, object], ...]) -> None:
    if not series:
        raise ValueError("uData visualization series must carry at least one series.")
    for item in series:
        _text(item.get("resource_id"), "visualization series resource_id", required=True)
        _choice(item.get("type"), SERIES_TYPES, "visualization series type")
        _choice(item.get("aggregate_y"), SERIES_AGGREGATES, "visualization series aggregate_y")
        _validate_series_filters(item.get("filters"))
        unknown = set(item) - {
            "type",
            "column_y",
            "aggregate_y",
            "resource_id",
            "column_x_name_override",
            "filters",
        }
        if unknown:
            raise ValueError(f"uData visualization series keys are not documented: {sorted(unknown)}.")


def _validated_axis(value: object, label: str, *, required: bool = False) -> Mapping[str, object] | None:
    if value is None:
        if required:
            raise ValueError(f"uData visualization {label} is required.")
        return None
    if not isinstance(value, Mapping):
        raise ValueError(f"uData {label} must be a mapping when supplied.")
    if label == "x_axis":
        _validate_x_axis(value)
    else:
        _validate_y_axis(value)
    return _frozen_mapping(value, f"visualization {label}")


def _validated_series(value: object, label: str, *, required: bool = False) -> tuple[Mapping[str, object], ...]:
    series = _frozen_mappings(value, label)
    if required and not series:
        raise ValueError(f"uData {label} must carry at least one series.")
    _validate_series(series)
    return series


@dataclass(frozen=True, slots=True)
class VisualizationListQuery:
    """The stock v1 visualization collection query decoded from ``Chart.__index_parser__``.

    ``page`` and ``page_size`` keep the stock pager names and defaults, ``sort``
    accepts the documented sortable keys with an optional ``-`` prefix, and the
    ``private``, ``owner``, and ``organization`` filters are omitted unless
    supplied.
    """

    page: int = 1
    page_size: int = DEFAULT_PAGE_SIZE
    sort: str | None = None
    private: bool | None = None
    owner: str | None = None
    organization: str | None = None

    def __post_init__(self) -> None:
        _paging(self.page, self.page_size, "visualization list")
        if self.sort is not None and self.sort not in {
            *(f"-{name}" for name in VISUALIZATION_SORTS),
            *VISUALIZATION_SORTS,
        }:
            raise ValueError("uData visualization list sort is not a documented choice.")
        if self.private is not None and type(self.private) is not bool:
            raise ValueError("uData visualization list private must be a boolean when supplied.")
        _text(self.owner, "visualization list owner")
        _text(self.organization, "visualization list organization")

    def query_params(self) -> list[tuple[str, str]]:
        """Encode the query into exact stock query-string parameter pairs."""
        params: list[tuple[str, str]] = [("page", str(self.page)), ("page_size", str(self.page_size))]
        if self.sort is not None:
            params.append(("sort", self.sort))
        if self.private is not None:
            params.append(("private", "true" if self.private else "false"))
        if self.owner is not None:
            params.append(("owner", self.owner))
        if self.organization is not None:
            params.append(("organization", self.organization))
        return params


@dataclass(frozen=True, slots=True)
class ContactPointCreateInput:
    """Presence-aware create payload for POST /api/1/contacts/.

    ``name`` and ``role`` are the documented required fields; ``email``,
    ``contact_form``, ``owner``, and ``organization`` are omitted unless
    supplied. The stock ``contact`` role requires an email or a contact form.
    """

    name: str
    role: str
    email: str | None = None
    contact_form: str | None = None
    owner: str | None = None
    organization: str | None = None

    def __post_init__(self) -> None:
        _text(self.name, _CONTACT_POINT_NAME, required=True)
        _no_url(self.name, _CONTACT_POINT_NAME)
        _email(self.email, "contact point email")
        _url(self.contact_form, "contact point contact_form")
        _choice(self.role, CONTACT_ROLES, "contact point role")
        if self.role == CONTACT_ROLE_CONTACT and self.email is None and self.contact_form is None:
            raise ValueError("uData contact point role 'contact' requires an email or a contact form.")
        if self.owner is not None and self.organization is not None:
            raise ValueError("uData contact point accepts either an owner or an organization, not both.")
        if self.owner is not None:
            linked_identifier(self.owner, "udata/api-v1.create-contact-point", "uData contact point owner")
        if self.organization is not None:
            linked_identifier(
                self.organization, "udata/api-v1.create-contact-point", "uData contact point organization"
            )

    def payload(self) -> dict[str, object]:
        """Encode the exact create JSON body, omitting every absent key."""
        body: dict[str, object] = {"name": self.name, "role": self.role}
        for key in ("email", "contact_form", "owner", "organization"):
            value = getattr(self, key)
            if value is not None:
                body[key] = value
        return body


@dataclass(frozen=True, slots=True)
class ContactPointUpdateInput:
    """Presence-aware patch payload for PUT /api/1/contacts/<id>/.

    Only the supplied fields are sent, so an absent key leaves the stored value
    unchanged. ``owner`` and ``organization`` are creation-only on the stock
    route, so they are not part of the documented update surface.
    """

    name: str | None = None
    role: str | None = None
    email: str | None = None
    contact_form: str | None = None

    def __post_init__(self) -> None:
        if all(value is None for value in (self.name, self.role, self.email, self.contact_form)):
            raise ValueError("uData contact point updates require at least one field.")
        _text(self.name, _CONTACT_POINT_NAME)
        _no_url(self.name, _CONTACT_POINT_NAME)
        _email(self.email, "contact point email")
        _url(self.contact_form, "contact point contact_form")
        _choice(self.role, CONTACT_ROLES, "contact point role")

    def payload(self) -> dict[str, object]:
        """Encode the exact update JSON body with omission semantics."""
        body: dict[str, object] = {}
        for key in ("name", "role", "email", "contact_form"):
            value = getattr(self, key)
            if value is not None:
                body[key] = value
        return body


@dataclass(frozen=True, slots=True)
class VisualizationCreateInput:
    """Presence-aware create payload for POST /api/1/visualizations/.

    ``title``, ``description``, ``x_axis``, and ``series`` are the documented
    required fields; ``private``, ``y_axis``, ``extras``, ``owner``, and
    ``organization`` are omitted or defaulted exactly as the stock write fields
    document.
    """

    title: str
    description: str
    x_axis: Mapping[str, object]
    series: tuple[Mapping[str, object], ...]
    private: bool = False
    y_axis: Mapping[str, object] | None = None
    extras: Mapping[str, object] | None = None
    owner: str | None = None
    organization: str | None = None

    def __post_init__(self) -> None:
        _text(self.title, "visualization title", required=True)
        _text(self.description, "visualization description", required=True)
        if type(self.private) is not bool:
            raise ValueError("uData visualization create private flag must be a boolean.")
        object.__setattr__(self, "x_axis", _validated_axis(self.x_axis, "x_axis", required=True))
        object.__setattr__(self, "y_axis", _validated_axis(self.y_axis, "y_axis"))
        object.__setattr__(self, "series", _validated_series(self.series, "visualization series", required=True))
        if self.extras is not None:
            if not isinstance(self.extras, Mapping):
                raise ValueError("uData visualization extras must be a mapping when supplied.")
            object.__setattr__(self, "extras", _frozen_mapping(self.extras, "visualization extras"))
        if self.owner is not None:
            linked_identifier(self.owner, "udata/api-v1.create-visualization", "uData visualization owner")
        if self.organization is not None:
            linked_identifier(
                self.organization, "udata/api-v1.create-visualization", "uData visualization organization"
            )

    def payload(self) -> dict[str, object]:
        """Encode the exact create JSON body, omitting every absent key."""
        body: dict[str, object] = {
            "title": self.title,
            "description": self.description,
            "private": self.private,
            "x_axis": _thaw_json(self.x_axis),
            "series": [_thaw_json(item) for item in self.series],
        }
        if self.y_axis is not None:
            body["y_axis"] = _thaw_json(self.y_axis)
        if self.extras is not None:
            body["extras"] = _thaw_json(self.extras)
        for key in ("owner", "organization"):
            value = getattr(self, key)
            if value is not None:
                body[key] = value
        return body


@dataclass(frozen=True, slots=True)
class VisualizationUpdateInput:
    """Presence-aware patch payload for PATCH /api/1/visualizations/<id>/."""

    title: str | None = None
    description: str | None = None
    private: bool | None = None
    x_axis: Mapping[str, object] | None = None
    y_axis: Mapping[str, object] | None = None
    series: tuple[Mapping[str, object], ...] | None = None
    extras: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        if all(
            value is None
            for value in (
                self.title,
                self.description,
                self.private,
                self.x_axis,
                self.y_axis,
                self.series,
                self.extras,
            )
        ):
            raise ValueError("uData visualization updates require at least one field.")
        _text(self.title, "visualization title")
        _text(self.description, "visualization description")
        if self.private is not None and type(self.private) is not bool:
            raise ValueError("uData visualization update private flag must be a boolean when supplied.")
        object.__setattr__(self, "x_axis", _validated_axis(self.x_axis, "x_axis"))
        object.__setattr__(self, "y_axis", _validated_axis(self.y_axis, "y_axis"))
        if self.series is not None:
            object.__setattr__(self, "series", _validated_series(self.series, "visualization series"))
        if self.extras is not None:
            if not isinstance(self.extras, Mapping):
                raise ValueError("uData visualization extras must be a mapping when supplied.")
            object.__setattr__(self, "extras", _frozen_mapping(self.extras, "visualization extras"))

    def payload(self) -> dict[str, object]:
        """Encode the exact update JSON body with omission semantics."""
        body: dict[str, object] = {}
        for key in ("title", "description", "private"):
            value = getattr(self, key)
            if value is not None:
                body[key] = value
        for key in ("x_axis", "y_axis", "extras"):
            value = getattr(self, key)
            if value is not None:
                body[key] = _thaw_json(value)
        if self.series is not None:
            body["series"] = [_thaw_json(item) for item in self.series]
        return body


@dataclass(frozen=True, slots=True)
class VisualizationImageInput:
    """Bounded visualization image upload input; bytes never enter the result record.

    The stock route also accepts an optional ``bbox`` cropping hint, but the
    shared multipart seam carries the file part alone, so the typed surface
    exposes only the file the stock parser requires.
    """

    data: bytes
    content_type: str

    def __post_init__(self) -> None:
        if not isinstance(self.data, (bytes, bytearray)):
            raise ValueError("uData visualization image data must be bytes.")
        if self.content_type not in IMAGE_MEDIA_TYPES:
            raise ValueError(f"uData visualization image content type must be one of {list(IMAGE_MEDIA_TYPES)}.")

    def part(self) -> UploadPart:
        """Return the stock multipart file part for the image upload route."""
        from datasluice.runtime.transport.base import UploadPart

        suffix = _IMAGE_MEDIA_TYPES[self.content_type]
        return UploadPart("file", bytes(self.data), f"visualization-image{suffix}", self.content_type)


@dataclass(frozen=True, slots=True)
class VisualizationPage:
    """One bounded native v1 visualization page retaining its pager metadata."""

    items: tuple[MappingRecord, ...]
    page: int | None = None
    page_size: int | None = None
    previous_page: str | None = None
    next_page: str | None = None
    total: int | None = None
    present_fields: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple) or not all(isinstance(item, MappingRecord) for item in self.items):
            raise ValueError("uData visualization page items must be a tuple of mapping records.")
        for name in ("page", "page_size", "total"):
            value = getattr(self, name)
            if value is not None and type(value) is not int:
                raise ValueError(f"uData visualization page field {name} must be an integer or None.")
        for name in ("previous_page", "next_page"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, str):
                raise ValueError(f"uData visualization page field {name} must be a string URL or None.")
        if not isinstance(self.present_fields, frozenset):
            raise ValueError("uData visualization page present_fields must be a frozenset.")

    def to_dict(self) -> dict[str, object]:
        """Return the bounded page as a fresh redacted JSON-safe mapping."""
        return {
            "kind": VISUALIZATION_PAGE_KIND,
            "items": [item.to_dict() for item in self.items],
            "page": self.page,
            "page_size": self.page_size,
            "previous_page": self.previous_page,
            "next_page": self.next_page,
            "total": self.total,
            "present_fields": sorted(self.present_fields),
        }


@dataclass(frozen=True, slots=True)
class ContactVisualizationMutationResult:
    """Contact-point and visualization mutation output retaining only bounded records and a receipt."""

    receipt: MutationReceipt
    record: MappingRecord | None = None

    def to_dict(self) -> dict[str, object]:
        """Return the mutation result as a fresh redacted JSON-safe mapping."""
        return {
            "receipt": self.receipt.to_dict(),
            "record": self.record.to_dict() if self.record is not None else None,
        }
