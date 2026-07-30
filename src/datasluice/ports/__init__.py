"""Port Protocol interfaces for DataSluice — unstable boundary contracts."""

from datasluice.ports.cache import CachePort
from datasluice.ports.catalog import CatalogPort, OrganizationCatalog, SearchableCatalog
from datasluice.ports.credentials import CredentialProvider
from datasluice.ports.detector import PortalDetector
from datasluice.ports.resource_reader import ResourceReader
from datasluice.ports.state_store import StateStore
from datasluice.ports.storage import StoragePort
from datasluice.ports.transport import ConditionalFetchResult, ConditionalTransport, StreamingTransport, Transport

__all__ = [
    "CachePort",
    "CatalogPort",
    "ConditionalFetchResult",
    "ConditionalTransport",
    "CredentialProvider",
    "OrganizationCatalog",
    "PortalDetector",
    "ResourceReader",
    "SearchableCatalog",
    "StateStore",
    "StoragePort",
    "StreamingTransport",
    "Transport",
]
