"""Immutable uData site-profile values and root operation inputs."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import cast
from urllib.parse import urlsplit

from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.models import _freeze_json, _thaw_json
from datasluice.domain.catalog.receipts import MutationReceipt

ROOT_OPERATION = "udata/api-v1.root-and-effective-profile-probe"
SET_SITE_OPERATION = "udata/api-v1.set_site"
SITE_RESOURCE_KIND = ResourceKind("site")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_VERSION = re.compile(r"^[0-9]+[.][0-9]+[.][0-9]+$")
_SITE_STRING_FIELDS = frozenset({"id", "title", "version"})
_SITE_LIST_FIELDS = frozenset({"keywords", "datasets_blocs", "reuses_blocs", "dataservices_blocs"})
_SITE_MAPPING_FIELDS = frozenset({"configs", "themes", "settings", "metrics"})
_SITE_FIELDS = _SITE_STRING_FIELDS | _SITE_LIST_FIELDS | _SITE_MAPPING_FIELDS | frozenset({"feed_size"})
_PATCH_LIST_FIELDS = frozenset({"keywords", "datasets_blocs", "reuses_blocs", "dataservices_blocs"})
_PATCH_MAPPING_FIELDS = frozenset({"configs", "themes", "settings"})
_DATASET_CATALOG_FILTERS = frozenset(
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
_DATASET_BOOLEAN_FILTERS = frozenset({"featured", "archived", "deleted", "private"})
_ORGANIZATION_CSV_SORTS = frozenset({"name", "reuses", "datasets", "followers", "views", "created", "last_modified"})
_DATASET_CSV_FILTERS = frozenset(
    {
        "tag",
        "badge",
        "organization",
        "organization_name",
        "organization_badge",
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
_DATASET_CSV_REPEATABLE_FILTERS = frozenset({"tag", "granularity", "format", "schema"})
_DATASET_CSV_BOOLEAN_FILTERS = frozenset({"featured"})
_DATASET_CSV_LAST_UPDATE_RANGES = frozenset({"last_30_days", "last_12_months", "last_3_years"})
_ACCESS_TYPES = frozenset({"open", "open_with_account", "restricted"})
_FORMAT_FAMILIES = frozenset({"tabular", "machine_readable", "geographical", "documents", "other"})
_PRODUCER_TYPES = frozenset({"public-service", "association", "company", "local-authority", "user", "not-specified"})
_OBJECT_ID_FILTERS = frozenset({"organization", "owner", "geozone", "topic", "dataservice", "reuse"})
_DATASET_CSV_OBJECT_ID_FILTERS = frozenset({"organization", "owner", "geozone", "topic"})
_REUSE_CSV_SORTS = frozenset({"title", "created", "datasets", "followers", "views"})
_REUSE_CSV_FILTERS = frozenset({"dataset", "dataservice", "tag", "organization_badge"})
_DATASERVICE_CSV_SORTS = frozenset({"title", "created", "last_modified", "followers", "views"})
_DATASERVICE_CSV_FILTERS = frozenset({"tag", "contact_point", "dataset", "organization_badge", "topic", "reuse"})
_CONTROLLED_ORIGIN = "http://127.0.0.1:5640"
_CONTROLLED_SOURCE_COMMIT = "0546582058d84706812a1c37387576efc4e5ad1f"
_CONTROLLED_COMPOSE_SHA256 = "f34538ffeab0de25dd5a8c0ce3984b2f2e6d56356fe3f095dbc593f8fdec23c7"
_CONTROLLED_DOCKERFILE_SHA256 = "6c21f02c3a287f1c1a2b42db392e767a484792bb763827a65bce5fcdd0d97e3b"
_CONTROLLED_IMAGE_DIGESTS = (
    "mongo@sha256:d3d7c7fbbbb18f61baac3f8d13f0834c28a0e000cae444691def321d568abe47",
    "redis@sha256:28bd5e15c3674c48a472a3dd475ba446d0a3cd876e7addb988b5840a286b2256",
    "elasticsearch/elasticsearch@sha256:5496dd095a610571a02c362cd5f60ddd29a2cac5225d52f953241a5189871356",
    "minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e",
    "axllent/mailpit@sha256:fa9d90f91a042f92cc28cf6dc4c75c6d57ac693b2737cdd30a6bfd9879838bbf",
)
_ATTESTATION_SEAL = object()


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


def _validate_object_id(value: str, field_name: str) -> None:
    if re.fullmatch(r"[0-9a-fA-F]{24}", value) is None:
        raise ValueError(f"uData {field_name} filters require a valid object identifier.")


@dataclass(frozen=True, slots=True, init=False)
class ControlledStackAttestation:
    """Evidence identity required before a uData site mutation can dispatch."""

    origin: str = field(repr=False)
    source_commit: str
    compose_sha256: str
    dockerfile_sha256: str
    image_digests: tuple[str, ...]
    nonce_sha256: str = field(repr=False)
    site_id: str

    def __init__(
        self,
        *,
        origin: str,
        source_commit: str,
        compose_sha256: str,
        dockerfile_sha256: str,
        image_digests: tuple[str, ...],
        nonce_sha256: str,
        site_id: str,
        _seal: object | None = None,
    ) -> None:
        if _seal is not _ATTESTATION_SEAL:
            raise TypeError("Controlled uData attestations must come from the verified evidence constructor.")
        object.__setattr__(self, "origin", origin)
        object.__setattr__(self, "source_commit", source_commit)
        object.__setattr__(self, "compose_sha256", compose_sha256)
        object.__setattr__(self, "dockerfile_sha256", dockerfile_sha256)
        object.__setattr__(self, "image_digests", image_digests)
        object.__setattr__(self, "nonce_sha256", nonce_sha256)
        object.__setattr__(self, "site_id", site_id)
        self._validate()

    def _validate(self) -> None:
        if self.origin != _CONTROLLED_ORIGIN:
            raise ValueError("Controlled uData evidence must use the approved loopback origin.")
        if _COMMIT.fullmatch(self.source_commit) is None or self.source_commit != _CONTROLLED_SOURCE_COMMIT:
            raise ValueError("Controlled uData evidence must use the pinned source commit.")
        if self.compose_sha256 != _CONTROLLED_COMPOSE_SHA256:
            raise ValueError("Controlled uData evidence must use the approved compose identity.")
        if self.dockerfile_sha256 != _CONTROLLED_DOCKERFILE_SHA256:
            raise ValueError("Controlled uData evidence must use the approved image build identity.")
        if not isinstance(self.image_digests, tuple) or self.image_digests != _CONTROLLED_IMAGE_DIGESTS:
            raise ValueError("Controlled uData evidence must use the approved dependency image identities.")
        if _SHA256.fullmatch(self.nonce_sha256) is None:
            raise ValueError("Controlled uData evidence must contain a nonce digest.")
        if not isinstance(self.site_id, str) or not self.site_id:
            raise ValueError("Controlled uData evidence must identify the target site.")
        parsed = urlsplit(self.origin)
        if (
            parsed.scheme != "http"
            or parsed.hostname != "127.0.0.1"
            or parsed.port != 5640
            or parsed.username
            or parsed.password
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Controlled uData evidence must identify the approved loopback transport target.")

    @classmethod
    def _from_verified_values(
        cls,
        *,
        origin: str,
        source_commit: str,
        compose_sha256: str,
        dockerfile_sha256: str,
        image_digests: tuple[str, ...],
        nonce: str,
        site_id: str,
    ) -> ControlledStackAttestation:
        """Create an attestation while retaining only the nonce digest."""
        if not isinstance(nonce, str) or not nonce:
            raise ValueError("Controlled uData evidence requires a non-empty stack nonce.")
        return cls(
            origin=origin,
            source_commit=source_commit,
            compose_sha256=compose_sha256,
            dockerfile_sha256=dockerfile_sha256,
            image_digests=image_digests,
            nonce_sha256=hashlib.sha256(nonce.encode()).hexdigest(),
            site_id=site_id,
            _seal=_ATTESTATION_SEAL,
        )

    @property
    def evidence_digest(self) -> str:
        """Return a stable digest of the non-secret controlled-stack identity."""
        value = "|".join(
            (
                self.source_commit,
                self.compose_sha256,
                self.dockerfile_sha256,
                *self.image_digests,
                self.nonce_sha256,
                self.site_id,
            )
        )
        return hashlib.sha256(value.encode()).hexdigest()

    def matches(self, *, origin: str, site_id: str) -> bool:
        """Return whether this evidence binds the current transport and decoded site."""
        return self.origin == origin == _CONTROLLED_ORIGIN and self.site_id == site_id


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

    title: str | _UnsetValue = UNSET
    keywords: tuple[str, ...] | None | _UnsetValue = UNSET
    feed_size: int | _UnsetValue = UNSET
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
class SiteDatasetCatalogQuery:
    """Exact dataset-filter schema shared only by the RDF and dataset CSV routes."""

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
                if key not in _DATASET_CATALOG_FILTERS:
                    raise ValueError(f"Unknown uData site dataset-catalog filter: {key}.")
                if isinstance(value, tuple):
                    if key != "tag":
                        raise ValueError("Only the uData dataset-catalog tag filter is repeatable.")
                    if not value or not all(isinstance(item, str) and item for item in value):
                        raise ValueError("uData site dataset-catalog repeated tags require non-empty strings.")
                elif type(value) is bool:
                    if key not in _DATASET_BOOLEAN_FILTERS:
                        raise ValueError(f"uData site dataset-catalog filter {key!r} is not boolean.")
                elif not isinstance(value, str) or not value:
                    raise ValueError("uData site dataset-catalog scalar filters require non-empty strings.")
                else:
                    if key in _OBJECT_ID_FILTERS:
                        _validate_object_id(value, key)
                    if key == "access_type" and value not in _ACCESS_TYPES:
                        raise ValueError("uData site dataset-catalog access_type is not documented.")
            object.__setattr__(self, "filters", _freeze_mapping(self.filters, "udata.site_dataset_catalog.filters"))

    def query_params(self) -> list[tuple[str, str]]:
        """Encode the pinned dataset-catalog parser fields in stable order."""
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
class SiteDatasetCsvQuery:
    """Exact DatasetSearch query schema used by datasets and resources CSV routes."""

    q: str | None = None
    sort: str | None = None
    page: int = 1
    page_size: int = 20
    filters: Mapping[str, str | bool | tuple[str, ...]] | None = None

    def __post_init__(self) -> None:
        if type(self.page) is not int or self.page < 1:
            raise ValueError("uData dataset CSV page must be a positive integer.")
        if type(self.page_size) is not int or self.page_size < 1:
            raise ValueError("uData dataset CSV page_size must be a positive integer.")
        if self.q is not None and (not isinstance(self.q, str) or not self.q):
            raise ValueError("uData dataset CSV q must be a non-empty string when supplied.")
        if self.sort is not None:
            sort_key = self.sort[1:] if self.sort.startswith("-") else self.sort
            if not self.sort or sort_key not in {"created", "last_update", "reuses", "followers", "views"}:
                raise ValueError("uData dataset CSV sort must be a documented value.")
        if self.filters is not None:
            if not isinstance(self.filters, Mapping):
                raise ValueError("uData dataset CSV filters must be a mapping.")
            for key, value in self.filters.items():
                if key not in _DATASET_CSV_FILTERS:
                    raise ValueError(f"Unknown uData dataset CSV filter: {key}.")
                if key == "last_update_range" and (
                    not isinstance(value, str) or value not in _DATASET_CSV_LAST_UPDATE_RANGES
                ):
                    raise ValueError("uData dataset CSV last_update_range must be a documented range choice.")
                if isinstance(value, tuple):
                    if (
                        key not in _DATASET_CSV_REPEATABLE_FILTERS
                        or not value
                        or not all(isinstance(item, str) and item for item in value)
                    ):
                        raise ValueError("Only documented dataset CSV list filters may repeat non-empty strings.")
                elif type(value) is bool:
                    if key not in _DATASET_CSV_BOOLEAN_FILTERS:
                        raise ValueError(f"uData dataset CSV filter {key!r} is not boolean.")
                elif not isinstance(value, str) or not value:
                    raise ValueError("uData dataset CSV scalar filters require non-empty strings.")
                else:
                    if key in _DATASET_CSV_OBJECT_ID_FILTERS:
                        _validate_object_id(value, key)
                    if key == "access_type" and value not in _ACCESS_TYPES:
                        raise ValueError("uData dataset CSV access_type is not documented.")
                    if key == "format_family" and value not in _FORMAT_FAMILIES:
                        raise ValueError("uData dataset CSV format_family is not documented.")
                    if key == "producer_type" and value not in _PRODUCER_TYPES:
                        raise ValueError("uData dataset CSV producer_type is not documented.")
            object.__setattr__(self, "filters", _freeze_mapping(self.filters, "udata.site_dataset_csv.filters"))

    def query_params(self) -> list[tuple[str, str]]:
        """Encode the pinned DatasetSearch parser fields in stable order."""
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
class SiteOrganizationCsvQuery:
    """Exact organization CSV query schema accepted by the pinned organization parser."""

    q: str | None = None
    badge: str | None = None
    name: str | None = None
    business_number_id: str | None = None

    def __post_init__(self) -> None:
        for field_name in ("q", "badge", "name", "business_number_id"):
            value = getattr(self, field_name)
            if value is not None and (not isinstance(value, str) or not value):
                raise ValueError(f"uData organization CSV {field_name} must be a non-empty string when supplied.")

    def query_params(self) -> list[tuple[str, str]]:
        """Encode the pinned organization parser fields in stable order."""
        params: list[tuple[str, str]] = []
        for key in ("q", "badge", "name", "business_number_id"):
            value = getattr(self, key)
            if value is not None:
                params.append((key, value))
        return params


@dataclass(frozen=True, slots=True)
class SiteReuseCsvQuery:
    """Exact generated Reuse index query schema used by the reuse CSV route."""

    q: str | None = None
    sort: str | None = None
    filters: Mapping[str, str | tuple[str, ...]] | None = None

    def __post_init__(self) -> None:
        if self.q is not None and (not isinstance(self.q, str) or not self.q):
            raise ValueError("uData reuse CSV q must be a non-empty string when supplied.")
        if self.sort is not None:
            sort_key = self.sort[1:] if self.sort.startswith("-") else self.sort
            if not self.sort or sort_key not in _REUSE_CSV_SORTS:
                raise ValueError("uData reuse CSV sort must be a documented value.")
        if self.filters is not None:
            if not isinstance(self.filters, Mapping):
                raise ValueError("uData reuse CSV filters must be a mapping.")
            for key, value in self.filters.items():
                if key not in _REUSE_CSV_FILTERS:
                    raise ValueError(f"Unknown uData reuse CSV filter: {key}.")
                if isinstance(value, tuple):
                    if key != "tag" or not value or not all(isinstance(item, str) and item for item in value):
                        raise ValueError("Only the uData reuse CSV tag filter may repeat non-empty strings.")
                elif not isinstance(value, str) or not value:
                    raise ValueError("uData reuse CSV filters require non-empty strings.")
                elif key in {"dataset", "dataservice"}:
                    _validate_object_id(value, key)
            object.__setattr__(self, "filters", _freeze_mapping(self.filters, "udata.site_reuse_csv.filters"))

    def query_params(self) -> list[tuple[str, str]]:
        """Encode the generated Reuse index parser fields in stable order."""
        params: list[tuple[str, str]] = []
        if self.q is not None:
            params.append(("q", self.q))
        if self.sort is not None:
            params.append(("sort", self.sort))
        for key, value in sorted((self.filters or {}).items()):
            if isinstance(value, tuple):
                params.extend((key, item) for item in value)
            else:
                params.append((key, value))
        return params


@dataclass(frozen=True, slots=True)
class SiteDataserviceCsvQuery:
    """Exact generated Dataservice index query schema used by its CSV route."""

    q: str | None = None
    sort: str | None = None
    filters: Mapping[str, str | tuple[str, ...]] | None = None

    def __post_init__(self) -> None:
        if self.q is not None and (not isinstance(self.q, str) or not self.q):
            raise ValueError("uData dataservice CSV q must be a non-empty string when supplied.")
        if self.sort is not None:
            sort_key = self.sort[1:] if self.sort.startswith("-") else self.sort
            if not self.sort or sort_key not in _DATASERVICE_CSV_SORTS:
                raise ValueError("uData dataservice CSV sort must be a documented value.")
        if self.filters is not None:
            if not isinstance(self.filters, Mapping):
                raise ValueError("uData dataservice CSV filters must be a mapping.")
            for key, value in self.filters.items():
                if key not in _DATASERVICE_CSV_FILTERS:
                    raise ValueError(f"Unknown uData dataservice CSV filter: {key}.")
                if isinstance(value, tuple):
                    if key != "tag" or not value or not all(isinstance(item, str) and item for item in value):
                        raise ValueError("Only the uData dataservice CSV tag filter may repeat non-empty strings.")
                elif not isinstance(value, str) or not value:
                    raise ValueError("uData dataservice CSV filters require non-empty strings.")
                elif key in {"contact_point", "dataset", "topic", "reuse"}:
                    _validate_object_id(value, key)
            object.__setattr__(self, "filters", _freeze_mapping(self.filters, "udata.site_dataservice_csv.filters"))

    def query_params(self) -> list[tuple[str, str]]:
        """Encode the generated Dataservice index parser fields in stable order."""
        params: list[tuple[str, str]] = []
        if self.q is not None:
            params.append(("q", self.q))
        if self.sort is not None:
            params.append(("sort", self.sort))
        for key, value in sorted((self.filters or {}).items()):
            if isinstance(value, tuple):
                params.extend((key, item) for item in value)
            else:
                params.append((key, value))
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
SiteCatalogQuery = SiteDatasetCatalogQuery
SiteRdfQuery = SiteDatasetCatalogQuery
SiteExportQuery = SiteDatasetCatalogQuery


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
    "ControlledStackAttestation",
    "ROOT_OPERATION",
    "SET_SITE_OPERATION",
    "SITE_RESOURCE_KIND",
    "UNSET",
    "SiteCatalogQuery",
    "SiteDatasetCatalogQuery",
    "SiteDatasetCsvQuery",
    "SiteDocument",
    "SiteExport",
    "SiteExportQuery",
    "SiteJsonLdContext",
    "SiteMutationOutcome",
    "SiteMutationResult",
    "SiteOrganizationCsvQuery",
    "SitePatchInput",
    "SitePatchResult",
    "SiteProfile",
    "SiteRedirect",
    "SiteRdfQuery",
    "SiteReuseCsvQuery",
    "SiteDataserviceCsvQuery",
]
