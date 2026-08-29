"""Presence-aware native page decoding and bounded uData projections."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from datasluice.contracts.catalog.native.udata import UDataResultItem
from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.models import DatasetRecord, NativeRecord, PageInfo, ResultEnvelope
from datasluice.errors.catalog import CatalogValidationError, NativeCatalogError

PLATFORM = CatalogPlatform.UDATA

NATIVE_PAGE_FIELDS = ("data", "page", "page_size", "previous_page", "next_page", "total")
_PAGED_PATH = "/api/1/datasets/"
_DATASETS_OPERATION_ID = "udata/api-v1.dataset-list-search-show-create-update-delete"


@dataclass(frozen=True, slots=True)
class NativePage:
    """The six-field uData pager decoded with explicit field presence.

    Absent and JSON-null fields are distinguishable: ``present_fields``
    records exactly which pager fields the deployment emitted, while the
    typed accessors normalize both to ``None`` for callers.
    """

    items: tuple[Mapping[str, object], ...]
    present_fields: frozenset[str]
    page: int | None
    page_size: int | None
    previous_page: str | None
    next_page: str | None
    total: int | None

    def __post_init__(self) -> None:
        if not isinstance(self.items, tuple) or not all(isinstance(item, Mapping) for item in self.items):
            raise ValueError("Native page items must be a tuple of mappings.")
        unknown = self.present_fields - set(NATIVE_PAGE_FIELDS)
        if unknown:
            raise ValueError(f"Native page presence tracks only the declared pager fields: {sorted(unknown)}")
        for field_name in ("page", "page_size", "total"):
            value = getattr(self, field_name)
            if value is not None and type(value) is not int:
                raise ValueError(f"Native page field {field_name} must decode to an integer or None.")
        for field_name in ("previous_page", "next_page"):
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, str):
                raise ValueError(f"Native page field {field_name} must decode to a string URL or None.")


def _int_field(payload: Mapping[str, object], field_name: str) -> int | None:
    value = payload.get(field_name)
    if value is None:
        return None
    if type(value) is not int:
        raise CatalogValidationError(
            f"The uData page field {field_name!r} must be an integer or null.",
            operation=_DATASETS_OPERATION_ID,
            platform=PLATFORM.value,
            safe_action="Verify the deployment serves the stock uData v1 page envelope.",
        )
    return value


def _string_field(payload: Mapping[str, object], field_name: str, *, operation: str) -> str | None:
    value = payload.get(field_name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise CatalogValidationError(
            f"The uData page field {field_name!r} must be a string URL or null.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Verify the deployment serves the stock uData v1 page envelope.",
        )
    return value


def parse_native_page(payload: object, *, operation: str = _DATASETS_OPERATION_ID) -> NativePage:
    """Decode one uData v1 page envelope preserving pager field presence.

    Raises:
        CatalogValidationError: When the payload is not an object, ``data``
            is not a list of objects, or a present pager field has the wrong
            JSON type.
    """
    if not isinstance(payload, Mapping):
        raise CatalogValidationError(
            "The uData page response must be a JSON object.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Retry against the stock uData v1 page endpoint.",
        )
    items_raw = payload.get("data")
    if items_raw is None:
        raise CatalogValidationError(
            "The uData page response omitted the data list.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Verify the deployment serves the stock uData v1 page envelope.",
        )
    if not isinstance(items_raw, list) or not all(isinstance(item, Mapping) for item in items_raw):
        raise CatalogValidationError(
            "The uData page data field must be a list of JSON objects.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Verify the deployment serves the stock uData v1 page envelope.",
        )
    return NativePage(
        items=tuple(dict(item) for item in items_raw),
        present_fields=frozenset(field for field in NATIVE_PAGE_FIELDS if field in payload),
        page=_int_field(payload, "page"),
        page_size=_int_field(payload, "page_size"),
        previous_page=_string_field(payload, "previous_page", operation=operation),
        next_page=_string_field(payload, "next_page", operation=operation),
        total=_int_field(payload, "total"),
    )


def parse_dataset_summary(item: Mapping[str, object], *, operation: str = _DATASETS_OPERATION_ID) -> NativeRecord:
    """Bound one dataset list item into the lossless native record envelope.

    Raises:
        CatalogValidationError: When the item omits its identity.
    """
    identifier = item.get("id")
    if not isinstance(identifier, str) or not identifier:
        raise CatalogValidationError(
            "The uData dataset summary requires a non-empty string id.",
            operation=operation,
            platform=PLATFORM.value,
            safe_action="Verify the page items against the pinned source oracle.",
        )
    return NativeRecord(
        platform=PLATFORM,
        resource_kind=ResourceKind.DATASET,
        id=CatalogId(platform=PLATFORM, resource_kind=ResourceKind.DATASET, value=identifier),
        payload=MappingProxyType(dict(item)),
    )


def normalized_dataset(record: NativeRecord) -> DatasetRecord:
    """Project one native dataset record onto the normalized catalog core.

    Raises:
        CatalogValidationError: When the record is not a uData dataset or
            carries neither a title nor a slug for the normalized name.
    """
    if record.platform is not PLATFORM or record.resource_kind is not ResourceKind.DATASET:
        raise CatalogValidationError(
            "Normalized dataset projection requires a native uData dataset record.",
            operation=_DATASETS_OPERATION_ID,
            platform=PLATFORM.value,
            safe_action="Project only native uData dataset records.",
        )
    title = record.payload.get("title")
    slug = record.payload.get("slug")
    name = title if isinstance(title, str) and title else (slug if isinstance(slug, str) and slug else None)
    if not name:
        raise CatalogValidationError(
            "The uData dataset summary requires a title or slug for normalized identity.",
            operation=_DATASETS_OPERATION_ID,
            platform=PLATFORM.value,
            safe_action="Verify the page items against the pinned source oracle.",
        )
    description = record.payload.get("description")
    return DatasetRecord(
        id=record.id,
        name=name,
        description=description if isinstance(description, str) and description else None,
    )


def shape_dataset_page(page: NativePage, *, operation: str = _DATASETS_OPERATION_ID) -> ResultEnvelope[UDataResultItem]:
    """Shape one decoded native page into the typed result envelope."""
    records = tuple(parse_dataset_summary(item, operation=operation) for item in page.items)
    page_info = None
    if page.page is not None:
        next_cursor = str(page.page + 1) if page.next_page is not None else None
        page_info = PageInfo(cursor=str(page.page), next_cursor=next_cursor, total_items=page.total)
    return ResultEnvelope(items=records, page=page_info)


def unimplemented_family(operation: str) -> NativeCatalogError:
    """Return the typed error for tracer-unassigned native operations."""
    return NativeCatalogError(
        "This uData native operation is not implemented by the tracer slice; use the strict site "
        "probe and dataset list until the owning family plan ships.",
        operation=operation,
        platform=PLATFORM.value,
    )
