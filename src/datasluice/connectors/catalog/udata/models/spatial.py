"""Immutable uData spatial zone and coverage queries."""

from __future__ import annotations

from dataclasses import dataclass

from datasluice.connectors.catalog.udata.models._segment import path_segment


def segment(value: str, operation: str) -> str:
    return path_segment(value, operation, "uData spatial")


def _positive_int(value: object, label: str) -> None:
    if type(value) is not int or value < 1:
        raise ValueError(f"uData spatial {label} must be a positive integer.")


@dataclass(frozen=True, slots=True)
class SpatialSuggestQuery:
    """Stock zone-suggestion query: required ``q`` and the upstream ``size`` default."""

    q: str
    size: int = 10

    def __post_init__(self) -> None:
        if not isinstance(self.q, str) or not self.q:
            raise ValueError("uData spatial suggest q must be a non-empty string.")
        _positive_int(self.size, "suggest size")

    def query_params(self) -> list[tuple[str, str]]:
        return [("q", self.q), ("size", str(self.size))]


@dataclass(frozen=True, slots=True)
class SpatialDatasetQuery:
    """Stock zone-datasets query carrying the upstream ``size`` default only."""

    size: int = 25

    def __post_init__(self) -> None:
        _positive_int(self.size, "dataset size")

    def query_params(self) -> list[tuple[str, str]]:
        return [("size", str(self.size))]
