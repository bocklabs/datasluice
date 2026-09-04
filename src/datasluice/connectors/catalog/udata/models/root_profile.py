"""Connector-facing re-exports for the domain-owned uData site contract."""

from __future__ import annotations

from datasluice.domain.catalog.udata import (
    ROOT_OPERATION,
    SET_SITE_OPERATION,
    SITE_RESOURCE_KIND,
    SiteCatalogQuery,
    SiteDataserviceCsvQuery,
    SiteDatasetCatalogQuery,
    SiteDatasetCsvQuery,
    SiteDocument,
    SiteMutationResult,
    SiteOrganizationCsvQuery,
    SitePatchInput,
    SiteProfile,
    SiteReuseCsvQuery,
)

__all__ = [
    "ROOT_OPERATION",
    "SET_SITE_OPERATION",
    "SITE_RESOURCE_KIND",
    "SiteCatalogQuery",
    "SiteDataserviceCsvQuery",
    "SiteDatasetCatalogQuery",
    "SiteDatasetCsvQuery",
    "SiteDocument",
    "SiteMutationResult",
    "SiteOrganizationCsvQuery",
    "SitePatchInput",
    "SiteProfile",
    "SiteReuseCsvQuery",
]
