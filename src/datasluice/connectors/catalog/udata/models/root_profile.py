"""Immutable uData site-profile values and root operation inputs."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import cast

from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.models import _freeze_json, _thaw_json
from datasluice.domain.catalog.receipts import MutationReceipt

ROOT_OPERATION = "udata/api-v1.root-and-effective-profile-probe"
SITE_RESOURCE_KIND = ResourceKind("site")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_VERSION = re.compile(r"^[0-9]+[.][0-9]+[.][0-9]+$")
_SITE_STRING_FIELDS = frozenset({"id", "title", "version"})
_SITE_LIST_FIELDS = frozenset({"keywords", "datasets_blocs", "reuses_blocs", "dataservices_blocs"})
_SITE_MAPPING_FIELDS = frozenset({"configs", "themes", "settings", "metrics"})
_SITE_FIELDS = _SITE_STRING_FIELDS | _SITE_LIST_FIELDS | _SITE_MAPPING_FIELDS | frozenset({"feed_size"})
_PATCH_LIST_FIELDS = frozenset({"keywords", "datasets_blocs", "reuses_blocs", "dataservices_blocs"})
_PATCH_MAPPING_FIELDS = frozenset({"configs", "themes", "settings"})
_CATALOG_FILTERS = frozenset(
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
_CATALOG_SORTS = frozenset({"title", "created", "last_update", "reuses", "followers", "views"})


def _freeze_mapping(value: Mapping[str, object], path: str) -> Mapping[str, object]:
    frozen = _freeze_json(dict(value), path)
    if not isinstance(frozen, Mapping):
        raise ValueError(f"{path} must be a JSON object.")
    return frozen


def _validate_text(value: object, field_name: str, *, allow_none: bool = False) -> None:
    if allow_none and value is None:
        return
    if not isinstance(value, str) or not value:
        raise ValueError(f"uData site {field_name} must be a non-empty string.")


def _validate_json_mapping(value: object, field_name: str, *, allow_none: bool = False) -> None:
    if allow_none and value is None:
        return
    if not isinstance(value, Mapping):
        raise ValueError(f"uData site {field_name} must be a JSON object.")
    _freeze_mapping(value, f"udata.site.{field_name}")


def _validate_blocks(value: object, field_name: str, *, allow_none: bool = False) -> None:
    if allow_none and value is None:
        return
    if not isinstance(value, tuple) or not all(isinstance(item, Mapping) for item in value):
        raise ValueError(f"uData site {field_name} must be a tuple of JSON objects.")
    for item in value:
        _freeze_mapping(item, f"udata.site.{field_name}")


@dataclass(frozen=True, slots=True)
class SiteProfile:
    """A lossless immutable representation of the uData site document."""

    payload: Mapping[str, object]
    present_fields: frozenset[str] | None = None
    extensions: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.payload, Mapping):
            raise ValueError("uData site profiles require a JSON object payload.")
        frozen = _freeze_mapping(self.payload, "udata.site_profile.payload")
        for field_name in ("id", "title", "version"):
            _validate_text(frozen.get(field_name), field_name)
        if not _VERSION.fullmatch(cast(str, frozen["version"])):
            raise ValueError("uData site version must be a semantic version string.")
        keywords = frozen.get("keywords")
        if keywords is not None and (
            not isinstance(keywords, (list, tuple)) or not all(isinstance(keyword, str) for keyword in keywords)
        ):
            raise ValueError("uData site keywords must be a list of strings.")
        feed_size = frozen.get("feed_size")
        if feed_size is not None and (type(feed_size) is not int or feed_size < 0):
            raise ValueError("uData site feed_size must be a non-negative integer.")
        for field_name in _SITE_MAPPING_FIELDS:
            if field_name in frozen:
                _validate_json_mapping(frozen[field_name], field_name)
        for field_name in _SITE_LIST_FIELDS - {"keywords"}:
            if field_name in frozen:
                value = frozen[field_name]
                if not isinstance(value, (list, tuple)) or not all(isinstance(item, Mapping) for item in value):
                    raise ValueError(f"uData site {field_name} must be a list of JSON objects.")
        present = frozenset(frozen) if self.present_fields is None else self.present_fields
        if not isinstance(present, frozenset) or not all(isinstance(name, str) for name in present):
            raise ValueError("uData site presence must be an immutable set of field names.")
        if not present.issubset(frozen):
            raise ValueError("uData site presence cannot include omitted fields.")
        object.__setattr__(self, "payload", frozen)
        object.__setattr__(self, "present_fields", present)
        unknown = {key: value for key, value in frozen.items() if key not in _SITE_FIELDS}
        provided_extensions = _freeze_mapping(self.extensions, "udata.site_profile.extensions")
        if unknown:
            existing = dict(provided_extensions)
            existing.setdefault("udata.site", unknown)
            provided_extensions = _freeze_mapping(existing, "udata.site_profile.extensions")
        object.__setattr__(self, "extensions", provided_extensions)

    @property
    def id(self) -> str:
        """Return the native site identifier."""
        return cast(str, self.payload["id"])

    @property
    def site_id(self) -> str:
        """Return the native site identifier under an explicit name."""
        return self.id

    @property
    def title(self) -> str:
        """Return the native site title."""
        return cast(str, self.payload["title"])

    @property
    def version(self) -> str:
        """Return the native uData version."""
        return cast(str, self.payload["version"])

    @property
    def keywords(self) -> tuple[str, ...] | None:
        """Return the native site keyword list when it was present."""
        value = self.payload.get("keywords")
        return tuple(cast(list[str] | tuple[str, ...], value)) if value is not None else None

    @property
    def feed_size(self) -> int | None:
        """Return the native site feed size when it was present."""
        value = self.payload.get("feed_size")
        return cast(int | None, value)

    @property
    def configs(self) -> Mapping[str, object] | None:
        """Return the native site configuration mapping when present."""
        return cast(Mapping[str, object] | None, self.payload.get("configs"))

    @property
    def themes(self) -> Mapping[str, object] | None:
        """Return the native site theme mapping when present."""
        return cast(Mapping[str, object] | None, self.payload.get("themes"))

    @property
    def settings(self) -> Mapping[str, object] | None:
        """Return the native site settings mapping when present."""
        return cast(Mapping[str, object] | None, self.payload.get("settings"))

    @property
    def metrics(self) -> Mapping[str, object] | None:
        """Return the native site metrics mapping when present."""
        return cast(Mapping[str, object] | None, self.payload.get("metrics"))

    def _block_values(self, field_name: str) -> tuple[Mapping[str, object], ...] | None:
        value = self.payload.get(field_name)
        return (
            tuple(cast(list[Mapping[str, object]] | tuple[Mapping[str, object], ...], value))
            if value is not None
            else None
        )

    @property
    def datasets_blocs(self) -> tuple[Mapping[str, object], ...] | None:
        """Return the native dataset editorial blocks when present."""
        return self._block_values("datasets_blocs")

    @property
    def reuses_blocs(self) -> tuple[Mapping[str, object], ...] | None:
        """Return the native reuse editorial blocks when present."""
        return self._block_values("reuses_blocs")

    @property
    def dataservices_blocs(self) -> tuple[Mapping[str, object], ...] | None:
        """Return the native dataservice editorial blocks when present."""
        return self._block_values("dataservices_blocs")

    @property
    def catalog_id(self) -> CatalogId:
        """Return the site identifier as a typed catalog identity."""
        return CatalogId(platform=CatalogPlatform.UDATA, resource_kind=SITE_RESOURCE_KIND, value=self.id)

    def to_dict(self) -> dict[str, object]:
        """Return a versioned JSON-safe site-profile envelope."""
        return {
            "schema_version": 1,
            "kind": "udata_site_profile",
            "payload": _thaw_json(self.payload),
            "present_fields": sorted(cast(frozenset[str], self.present_fields)),
            "extensions": _thaw_json(self.extensions),
        }

    @classmethod
    def from_payload(cls, payload: object, *, operation: str = ROOT_OPERATION) -> SiteProfile:
        """Decode one site payload after the route-aware wire boundary validates it."""
        if not isinstance(payload, Mapping):
            raise ValueError(f"{operation} site response must be a JSON object.")
        return cls(payload=payload)


class _UnsetValue:
    __slots__ = ()

    def __repr__(self) -> str:
        return "UNSET"


UNSET = _UnsetValue()


@dataclass(frozen=True, slots=True)
class SitePatchInput:
    """Presence-aware PATCH input for the writable uData site fields."""

    title: str | None | _UnsetValue = UNSET
    keywords: tuple[str, ...] | None | _UnsetValue = UNSET
    feed_size: int | None | _UnsetValue = UNSET
    configs: Mapping[str, object] | None | _UnsetValue = UNSET
    themes: Mapping[str, object] | None | _UnsetValue = UNSET
    settings: Mapping[str, object] | None | _UnsetValue = UNSET
    datasets_blocs: tuple[Mapping[str, object], ...] | None | _UnsetValue = UNSET
    reuses_blocs: tuple[Mapping[str, object], ...] | None | _UnsetValue = UNSET
    dataservices_blocs: tuple[Mapping[str, object], ...] | None | _UnsetValue = UNSET

    def __post_init__(self) -> None:
        if self.title is not UNSET:
            _validate_text(self.title, "title")
        if self.keywords is not UNSET and self.keywords is not None:
            if not isinstance(self.keywords, tuple) or not all(
                isinstance(keyword, str) and keyword for keyword in self.keywords
            ):
                raise ValueError("uData site keywords must be a tuple of non-empty strings when supplied.")
        if self.feed_size is not UNSET and (
            self.feed_size is None or type(self.feed_size) is not int or self.feed_size < 0
        ):
            raise ValueError("uData site feed_size must be a non-negative integer when supplied.")
        for field_name in _PATCH_MAPPING_FIELDS:
            value = getattr(self, field_name)
            if value is not UNSET:
                _validate_json_mapping(value, field_name, allow_none=True)
        for field_name in _PATCH_LIST_FIELDS - {"keywords"}:
            value = getattr(self, field_name)
            if value is not UNSET:
                _validate_blocks(value, field_name, allow_none=True)
        for field_name in _PATCH_MAPPING_FIELDS:
            value = getattr(self, field_name)
            if isinstance(value, Mapping):
                object.__setattr__(self, field_name, _freeze_mapping(value, f"udata.site_patch.{field_name}"))
        for field_name in _PATCH_LIST_FIELDS - {"keywords"}:
            value = getattr(self, field_name)
            if isinstance(value, tuple):
                object.__setattr__(
                    self,
                    field_name,
                    tuple(_freeze_mapping(item, f"udata.site_patch.{field_name}") for item in value),
                )

    @property
    def present_fields(self) -> frozenset[str]:
        """Return the exact fields that will be sent, including explicit nulls."""
        return frozenset(
            field_name
            for field_name in (
                "title",
                "keywords",
                "feed_size",
                "configs",
                "themes",
                "settings",
                "datasets_blocs",
                "reuses_blocs",
                "dataservices_blocs",
            )
            if getattr(self, field_name) is not UNSET
        )

    def payload(self) -> dict[str, object]:
        """Encode the exact PATCH body while preserving omission and null semantics."""
        body: dict[str, object] = {}
        for field_name in (
            "title",
            "keywords",
            "feed_size",
            "configs",
            "themes",
            "settings",
            "datasets_blocs",
            "reuses_blocs",
            "dataservices_blocs",
        ):
            value = getattr(self, field_name)
            if value is UNSET:
                continue
            if isinstance(value, tuple):
                body[field_name] = [_thaw_json(item) for item in value]
            elif isinstance(value, Mapping):
                body[field_name] = _thaw_json(value)
            else:
                body[field_name] = value
        return body

    def to_dict(self) -> dict[str, object]:
        """Return the JSON-safe PATCH body."""
        return self.payload()


@dataclass(frozen=True, slots=True)
class SiteCatalogQuery:
    """Typed query parameters shared by root RDF catalog and CSV reads."""

    q: str | None = None
    sort: str | None = None
    page: int = 1
    page_size: int = 100
    filters: Mapping[str, str | bool | tuple[str, ...]] | None = None

    def __post_init__(self) -> None:
        if type(self.page) is not int or self.page < 1:
            raise ValueError("uData site catalog page must be a positive integer.")
        if type(self.page_size) is not int or self.page_size < 1:
            raise ValueError("uData site catalog page_size must be a positive integer.")
        if self.q is not None and not isinstance(self.q, str):
            raise ValueError("uData site catalog q must be a string when supplied.")
        if self.sort is not None:
            sort_key = self.sort[1:] if self.sort.startswith("-") else self.sort
            if not self.sort or sort_key not in _CATALOG_SORTS:
                raise ValueError("uData site catalog sort must be a documented value.")
        if self.filters is not None:
            if not isinstance(self.filters, Mapping):
                raise ValueError("uData site catalog filters must be a mapping.")
            for key, value in self.filters.items():
                if key not in _CATALOG_FILTERS:
                    raise ValueError(f"Unknown uData site catalog filter: {key}.")
                if isinstance(value, tuple):
                    if not value or not all(isinstance(item, str) and item for item in value):
                        raise ValueError("uData site catalog repeated filters require non-empty strings.")
                elif type(value) is not bool and (not isinstance(value, str) or not value):
                    raise ValueError("uData site catalog filters require strings, booleans, or string tuples.")
            object.__setattr__(self, "filters", _freeze_mapping(self.filters, "udata.site_catalog.filters"))

    def query_params(self) -> list[tuple[str, str]]:
        """Encode catalog parameters in stable order with repeated filter keys."""
        params = [("page", str(self.page)), ("page_size", str(self.page_size))]
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
                params.append((key, cast(str, value)))
        return params


@dataclass(frozen=True, slots=True)
class SiteDocument:
    """A bounded root document or redirect without retaining response bytes."""

    endpoint: str
    media_type: str
    status_code: int
    size_bytes: int
    sha256: str
    metadata: Mapping[str, object] = field(default_factory=dict)
    data: Mapping[str, object] | None = None
    location: str | None = None
    format: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.endpoint, str) or not self.endpoint:
            raise ValueError("uData site documents require a non-empty endpoint.")
        if not isinstance(self.media_type, str) or not self.media_type:
            raise ValueError("uData site documents require a media type.")
        if type(self.status_code) is not int or not 200 <= self.status_code <= 399:
            raise ValueError("uData site document status codes must be successful or redirect statuses.")
        if type(self.size_bytes) is not int or self.size_bytes < 0:
            raise ValueError("uData site document sizes must be non-negative integers.")
        if _SHA256.fullmatch(self.sha256) is None:
            raise ValueError("uData site documents require a SHA-256 digest.")
        object.__setattr__(self, "metadata", _freeze_mapping(self.metadata, "udata.site_document.metadata"))
        if self.data is not None:
            object.__setattr__(self, "data", _freeze_mapping(self.data, "udata.site_document.data"))
        if self.location is not None and (not isinstance(self.location, str) or not self.location):
            raise ValueError("uData site document locations must be non-empty strings when supplied.")

    @property
    def payload(self) -> Mapping[str, object]:
        """Return JSON content for context documents or bounded metadata otherwise."""
        return self.data if self.data is not None else self.metadata

    def to_dict(self) -> dict[str, object]:
        """Return a versioned JSON-safe document envelope."""
        return {
            "schema_version": 1,
            "kind": "udata_site_document",
            "endpoint": self.endpoint,
            "media_type": self.media_type,
            "status_code": self.status_code,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "payload": _thaw_json(self.payload),
            "location": self.location,
            "format": self.format,
        }


SiteExport = SiteDocument
SiteRedirect = SiteDocument
SiteJsonLdContext = SiteDocument
SiteRdfQuery = SiteCatalogQuery
SiteExportQuery = SiteCatalogQuery


@dataclass(frozen=True, slots=True)
class SiteMutationResult:
    """A site PATCH outcome with a redacted receipt and optional returned profile."""

    receipt: MutationReceipt
    profile: SiteProfile | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.receipt, MutationReceipt):
            raise ValueError("uData site mutations require a shared mutation receipt.")
        if self.profile is not None and not isinstance(self.profile, SiteProfile):
            raise ValueError("uData site mutation profiles require SiteProfile.")

    @property
    def record(self) -> SiteProfile | None:
        """Return the returned site profile using the common mutation-result name."""
        return self.profile

    def to_dict(self) -> dict[str, object]:
        """Return a versioned JSON-safe mutation result."""
        return {
            "schema_version": 1,
            "kind": "udata_site_mutation_result",
            "receipt": self.receipt.to_dict(),
            "profile": self.profile.to_dict() if self.profile is not None else None,
        }


SitePatchResult = SiteMutationResult
SiteMutationOutcome = MutationReceipt

__all__ = [
    "ROOT_OPERATION",
    "SITE_RESOURCE_KIND",
    "UNSET",
    "SiteCatalogQuery",
    "SiteDocument",
    "SiteExport",
    "SiteExportQuery",
    "SiteJsonLdContext",
    "SiteMutationOutcome",
    "SiteMutationResult",
    "SitePatchInput",
    "SitePatchResult",
    "SiteProfile",
    "SiteRedirect",
    "SiteRdfQuery",
]
