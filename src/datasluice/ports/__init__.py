"""Port Protocol interfaces for DataSluice — unstable boundary contracts."""

from datasluice.ports.cache import CachePort
from datasluice.ports.catalog import CatalogPort, OrganizationCatalog, SearchableCatalog
from datasluice.ports.credentials import CredentialProvider
from datasluice.ports.detector import PortalDetector
from datasluice.ports.resource_reader import CheckpointableResourceReader, ResourceReader, ResponseAwareReader
from datasluice.ports.state_store import AtomicStateStore, StateStore
from datasluice.ports.storage import StoragePort
from datasluice.ports.transport import ConditionalFetchResult, ConditionalTransport, StreamingTransport, Transport

__all__ = [
    "AtomicStateStore",
    "CachePort",
    "CatalogPort",
    "CheckpointableResourceReader",
    "ConditionalFetchResult",
    "ConditionalTransport",
    "CredentialProvider",
    "OrganizationCatalog",
    "PortalDetector",
    "ResourceReader",
    "ResponseAwareReader",
    "SearchableCatalog",
    "StateStore",
    "StoragePort",
    "StreamingTransport",
    "Transport",
]
