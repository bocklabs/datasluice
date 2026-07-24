"""Portal-agnostic domain models for DataSluice."""

from datasluice.domain.access import HttpDownload, LocalFile, ObjectStorage, QueryAccess, ResourceAccess, StreamAccess
from datasluice.domain.artifact import Artifact
from datasluice.domain.capabilities import CatalogCapabilities
from datasluice.domain.credentials import CredentialScope
from datasluice.domain.dataset import Dataset
from datasluice.domain.detection import DetectionResult
from datasluice.domain.license import License
from datasluice.domain.organization import Organization
from datasluice.domain.query import Query
from datasluice.domain.resource import Resource
from datasluice.domain.result import SearchResult
from datasluice.domain.schema import Schema
from datasluice.domain.sync_state import SyncState

__all__ = [
    "Artifact",
    "CatalogCapabilities",
    "CredentialScope",
    "Dataset",
    "DetectionResult",
    "HttpDownload",
    "License",
    "LocalFile",
    "ObjectStorage",
    "Organization",
    "Query",
    "QueryAccess",
    "Resource",
    "ResourceAccess",
    "Schema",
    "SearchResult",
    "StreamAccess",
    "SyncState",
]
