"""DataSluice — one Python interface for open-data discovery, extraction,
format normalization, and pipeline integration.
"""

from datasluice._version import __version__
from datasluice.client import DataSluice
from datasluice.domain import Dataset, License, Organization, Query, Resource, SearchResult
from datasluice.exceptions import (
    AdapterError,
    AdapterNotFoundError,
    AuthenticationError,
    ChecksumMismatchError,
    ConfigError,
    DataSluiceError,
    DownloadError,
    FormatError,
    NotFoundError,
    PortalDetectionError,
    PortalError,
    RateLimitError,
)

__all__ = [
    "__version__",
    "DataSluice",
    # Domain models
    "Dataset",
    "Resource",
    "Organization",
    "License",
    "Query",
    "SearchResult",
    # Exceptions
    "DataSluiceError",
    "PortalError",
    "AdapterError",
    "AdapterNotFoundError",
    "PortalDetectionError",
    "AuthenticationError",
    "RateLimitError",
    "NotFoundError",
    "DownloadError",
    "ChecksumMismatchError",
    "FormatError",
    "ConfigError",
]
