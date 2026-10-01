"""Dual-mode uData spatial zone and coverage service."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datasluice.connectors.catalog.udata.models.spatial import (
    SpatialDatasetQuery,
    SpatialSuggestQuery,
)
from datasluice.connectors.catalog.udata.wire import spatial as wire
from datasluice.domain.catalog.models import MappingRecord
from datasluice.errors.catalog import NativeCatalogError

from .taxonomies import AsyncCatalogService, SyncCatalogService

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient


class SyncSpatialService(SyncCatalogService):
    """Typed synchronous methods for the assigned spatial family."""

    def __init__(self, client: SyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    def suggest_zones(self, query: SpatialSuggestQuery) -> tuple[MappingRecord, ...]:
        return self._read(wire.suggest_zones_request(query), wire.SUGGEST_ZONES_OPERATION, wire.parse_mapping_sequence)

    def spatial_zones(self, ids: tuple[str, ...]) -> MappingRecord:
        return self._read(wire.spatial_zones_request(ids), wire.SPATIAL_ZONES_OPERATION, wire.parse_feature_collection)

    def spatial_zone_datasets(
        self, zone_id: str, query: SpatialDatasetQuery | None = None
    ) -> tuple[MappingRecord, ...]:
        return self._read(
            wire.spatial_zone_datasets_request(zone_id, query or SpatialDatasetQuery()),
            wire.SPATIAL_ZONE_DATASETS_OPERATION,
            wire.parse_mapping_sequence,
        )

    def spatial_zone(self, zone_id: str) -> MappingRecord:
        return self._read(wire.spatial_zone_request(zone_id), wire.SPATIAL_ZONE_OPERATION, wire.parse_mapping)

    def spatial_levels(self) -> tuple[MappingRecord, ...]:
        return self._read(wire.spatial_levels_request(), wire.SPATIAL_LEVELS_OPERATION, wire.parse_mapping_sequence)

    def spatial_granularities(self) -> tuple[MappingRecord, ...]:
        return self._read(
            wire.spatial_granularities_request(),
            wire.SPATIAL_GRANULARITIES_OPERATION,
            wire.parse_mapping_sequence,
        )

    def spatial_coverage(self, level: str) -> MappingRecord:
        return self._read(
            wire.spatial_coverage_request(level), wire.SPATIAL_COVERAGE_OPERATION, wire.parse_feature_collection
        )


class AsyncSpatialService(AsyncCatalogService):
    """Typed asynchronous methods for the assigned spatial family."""

    def __init__(self, client: AsyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    async def suggest_zones(self, query: SpatialSuggestQuery) -> tuple[MappingRecord, ...]:
        return await self._read(
            wire.suggest_zones_request(query), wire.SUGGEST_ZONES_OPERATION, wire.parse_mapping_sequence
        )

    async def spatial_zones(self, ids: tuple[str, ...]) -> MappingRecord:
        return await self._read(
            wire.spatial_zones_request(ids), wire.SPATIAL_ZONES_OPERATION, wire.parse_feature_collection
        )

    async def spatial_zone_datasets(
        self, zone_id: str, query: SpatialDatasetQuery | None = None
    ) -> tuple[MappingRecord, ...]:
        return await self._read(
            wire.spatial_zone_datasets_request(zone_id, query or SpatialDatasetQuery()),
            wire.SPATIAL_ZONE_DATASETS_OPERATION,
            wire.parse_mapping_sequence,
        )

    async def spatial_zone(self, zone_id: str) -> MappingRecord:
        return await self._read(wire.spatial_zone_request(zone_id), wire.SPATIAL_ZONE_OPERATION, wire.parse_mapping)

    async def spatial_levels(self) -> tuple[MappingRecord, ...]:
        return await self._read(
            wire.spatial_levels_request(), wire.SPATIAL_LEVELS_OPERATION, wire.parse_mapping_sequence
        )

    async def spatial_granularities(self) -> tuple[MappingRecord, ...]:
        return await self._read(
            wire.spatial_granularities_request(),
            wire.SPATIAL_GRANULARITIES_OPERATION,
            wire.parse_mapping_sequence,
        )

    async def spatial_coverage(self, level: str) -> MappingRecord:
        return await self._read(
            wire.spatial_coverage_request(level), wire.SPATIAL_COVERAGE_OPERATION, wire.parse_feature_collection
        )
