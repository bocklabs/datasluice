"""Exact spatial zone and coverage request builders and bounded decoders for uData 17.6."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from urllib.parse import urlencode

from datasluice.connectors.catalog.udata.models.spatial import (
    SpatialDatasetQuery,
    SpatialSuggestQuery,
    segment,
)
from datasluice.domain.catalog.models import MappingRecord
from datasluice.errors.catalog import CatalogValidationError

PLATFORM = "udata"
SUGGEST_ZONES_OPERATION = "udata/api-v1.suggest-zones"
SPATIAL_ZONES_OPERATION = "udata/api-v1.spatial-zones"
SPATIAL_ZONE_DATASETS_OPERATION = "udata/api-v1.spatial-zone-datasets"
SPATIAL_ZONE_OPERATION = "udata/api-v1.spatial-zone"
SPATIAL_LEVELS_OPERATION = "udata/api-v1.spatial-levels"
SPATIAL_GRANULARITIES_OPERATION = "udata/api-v1.spatial-granularities"
SPATIAL_COVERAGE_OPERATION = "udata/api-v1.spatial-coverage"
_SPATIAL_PATH = "/api/1/spatial"
_PINNED_SCHEMA_ACTION = "Verify the response against the pinned uData 17.6 schema."

type Request = tuple[str, str, dict[str, str], object | None]


def _request(path: str) -> Request:
    return "GET", path, {}, None


def _query(pairs: list[tuple[str, str]]) -> str:
    return urlencode(pairs)


def _zone_list(ids: Iterable[str]) -> str:
    """Encode the stock comma-separated zone identifier list as one path segment list."""
    encoded = [segment(zone_id, SPATIAL_ZONES_OPERATION) for zone_id in ids]
    if not encoded:
        raise CatalogValidationError(
            "A uData spatial zone list must carry one or more zone identifiers.",
            operation=SPATIAL_ZONES_OPERATION,
            platform=PLATFORM,
            safe_action="Pass at least one prior typed zone identifier.",
        )
    return ",".join(encoded)


def suggest_zones_request(query: SpatialSuggestQuery) -> Request:
    return _request(f"{_SPATIAL_PATH}/zones/suggest/?{_query(query.query_params())}")


def spatial_zones_request(ids: Iterable[str]) -> Request:
    return _request(f"{_SPATIAL_PATH}/zones/{_zone_list(ids)}/")


def spatial_zone_datasets_request(zone_id: str, query: SpatialDatasetQuery) -> Request:
    zone = segment(zone_id, SPATIAL_ZONE_DATASETS_OPERATION)
    return _request(f"{_SPATIAL_PATH}/zone/{zone}/datasets/?{_query(query.query_params())}")


def spatial_zone_request(zone_id: str) -> Request:
    return _request(f"{_SPATIAL_PATH}/zone/{segment(zone_id, SPATIAL_ZONE_OPERATION)}/")


def spatial_levels_request() -> Request:
    return _request(f"{_SPATIAL_PATH}/levels/")


def spatial_granularities_request() -> Request:
    return _request(f"{_SPATIAL_PATH}/granularities/")


def spatial_coverage_request(level: str) -> Request:
    return _request(f"{_SPATIAL_PATH}/coverage/{segment(level, SPATIAL_COVERAGE_OPERATION)}/")


def parse_mapping(payload: object, operation: str) -> MappingRecord:
    if not isinstance(payload, Mapping):
        raise CatalogValidationError(
            "The uData spatial response must be an object.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return MappingRecord(payload)


def parse_mapping_sequence(payload: object, operation: str) -> tuple[MappingRecord, ...]:
    if not isinstance(payload, list) or not all(isinstance(item, Mapping) for item in payload):
        raise CatalogValidationError(
            "The uData spatial response must be a list of objects.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return tuple(MappingRecord(item) for item in payload)


def parse_feature_collection(payload: object, operation: str) -> MappingRecord:
    """Retain the stock GeoJSON FeatureCollection envelope with its native members."""
    collection = parse_mapping(payload, operation)
    features = collection.payload.get("features")
    if not isinstance(features, tuple) or not all(isinstance(feature, Mapping) for feature in features):
        raise CatalogValidationError(
            "The uData spatial response must be a GeoJSON FeatureCollection.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return collection
