"""Structured errors for the catalog connector contract."""

from datasluice.errors.catalog import (
    CatalogConflictError,
    CatalogError,
    CatalogNotFoundError,
    CatalogRateLimitError,
    CatalogUnavailableError,
    CatalogValidationError,
    ForbiddenError,
    NativeCatalogError,
    UnauthenticatedError,
    UnsupportedCapabilityError,
    map_catalog_error,
    raise_mapped_catalog_error,
)

__all__ = [
    "CatalogConflictError",
    "CatalogError",
    "CatalogNotFoundError",
    "CatalogRateLimitError",
    "CatalogUnavailableError",
    "CatalogValidationError",
    "ForbiddenError",
    "NativeCatalogError",
    "UnauthenticatedError",
    "UnsupportedCapabilityError",
    "map_catalog_error",
    "raise_mapped_catalog_error",
]
