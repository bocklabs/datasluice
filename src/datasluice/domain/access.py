"""ResourceAccess sum-type describing how a resource is reached."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any


@dataclass(frozen=True, kw_only=True)
class ResourceAccess:
    """Base descriptor for how a resource is accessed.

    Subclasses discriminate on ``kind`` so Phase 4 match-dispatch (DATA-04)
    can route to the correct reader without complex isinstance chains.

    Attributes:
        kind: Discriminator string identifying the access variant.
        extra: Portal-native access fields not captured above.
    """

    kind: str
    extra: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.extra, MappingProxyType):
            object.__setattr__(self, "extra", MappingProxyType(dict(self.extra)))


@dataclass(frozen=True, kw_only=True)
class HttpDownload(ResourceAccess):
    """Resource fetched over HTTP(S).

    Attributes:
        url: Absolute URL to download.
        method: HTTP method (default ``"GET"``).
        kind: Discriminator, always ``"http_download"``.
    """

    url: str
    method: str = "GET"
    kind: str = "http_download"


@dataclass(frozen=True, kw_only=True)
class ObjectStorage(ResourceAccess):
    """Resource stored in object storage (S3, GCS, Azure Blob).

    Attributes:
        uri: Object URI (e.g. ``s3://bucket/key``).
        kind: Discriminator, always ``"object_storage"``.
    """

    uri: str
    kind: str = "object_storage"


@dataclass(frozen=True, kw_only=True)
class QueryAccess(ResourceAccess):
    """Resource accessed via a query endpoint (SQL/SoQL/datastore).

    Attributes:
        endpoint: Query endpoint URL.
        query_language: Query language identifier (empty when unspecified).
        kind: Discriminator, always ``"query"``.
    """

    endpoint: str
    query_language: str = ""
    kind: str = "query"


@dataclass(frozen=True, kw_only=True)
class StreamAccess(ResourceAccess):
    """Resource consumed as a streaming endpoint.

    Attributes:
        url: Stream URL.
        kind: Discriminator, always ``"stream"``.
    """

    url: str
    kind: str = "stream"


@dataclass(frozen=True, kw_only=True)
class LocalFile(ResourceAccess):
    """Resource available on the local filesystem.

    Attributes:
        path: Local filesystem path.
        kind: Discriminator, always ``"local_file"``.
    """

    path: str
    kind: str = "local_file"
