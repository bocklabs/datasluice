"""Tests for the exact retained public surface of the top-level datasluice package."""

from __future__ import annotations

import re

import pytest

import datasluice

_ROOT_EXPORTS = {
    "__version__",
    "DataSluice",
    "OpenedResource",
    "DirectResourceLocator",
    "resource_locator_from_dict",
    "Dataset",
    "Resource",
    "Organization",
    "License",
    "Query",
    "SearchResult",
    "Artifact",
    "ArtifactProvenance",
    "CredentialScope",
    "DetectionResult",
    "Digest",
    "HttpDownload",
    "LocalFile",
    "ObjectStorage",
    "QueryAccess",
    "ResourceAccess",
    "Schema",
    "StreamAccess",
    "SyncState",
    "CatalogId",
    "CatalogPlatform",
    "ResourceKind",
    "DatasetRecord",
    "NativeRecord",
    "ResultEnvelope",
    "DataSluiceError",
    "CatalogError",
    "NativeCatalogError",
    "UnsupportedCapabilityError",
    "UnauthenticatedError",
    "ForbiddenError",
    "CatalogUnavailableError",
    "StateStoreError",
    "SyncStateConflictError",
    "DownloadError",
    "ChecksumMismatchError",
    "FormatError",
    "ConfigError",
    "ResourceResolutionError",
    "OpenedResourceConsumedError",
}

_RETIRED_ROOT_SYMBOLS = (
    "DataSluiceSession",
    "PluginManager",
    "AdapterNotFoundError",
    "PortalError",
    "NotFoundError",
    "CatalogResourceLocator",
    "ResourceLocator",
    "search",
    "portal",
    "detect",
    "discover",
    "registry",
)


def test_version() -> None:
    assert re.match(r"^\d+\.\d+\.\d+", datasluice.__version__)


def test_root_exports_exactly_the_canonical_surface() -> None:
    assert set(datasluice.__all__) == _ROOT_EXPORTS


@pytest.mark.parametrize("export", sorted(_ROOT_EXPORTS))
def test_every_declared_root_export_is_importable(export: str) -> None:
    assert getattr(datasluice, export) is not None


@pytest.mark.parametrize("retired", _RETIRED_ROOT_SYMBOLS)
def test_retired_connector_and_runtime_symbols_are_absent_from_the_root(retired: str) -> None:
    assert retired not in datasluice.__all__
    assert not hasattr(datasluice, retired)
    with pytest.raises(ImportError):
        exec(f"from datasluice import {retired}", {})
