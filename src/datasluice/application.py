"""Public application facade, locators, and opened-resource lifecycle."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

from datasluice._uri import sanitize_uri
from datasluice.data.access import DataPlaneResourceReader
from datasluice.domain import DetectionResult, HttpDownload, LocalFile, Query, Resource, SearchResult
from datasluice.domain.artifact import _freeze_extensions
from datasluice.exceptions import DataSluiceError, ResourceResolutionError, StreamClosedError
from datasluice.runtime.session import DataSluiceSession

_DIRECT_LOCATOR_KEYS = frozenset({"schema_version", "kind", "uri", "format", "media_type", "extensions"})
_CATALOG_LOCATOR_KEYS = frozenset({"schema_version", "kind", "portal_url", "dataset_id", "resource_id", "extensions"})


def _contract_error(path: str) -> DataSluiceError:
    return DataSluiceError(f"Invalid schema-v1 resource locator at {path}")


def _validate_uri(value: object, path: str) -> str:
    if not isinstance(value, str) or not value:
        raise _contract_error(path)
    try:
        parts = urlsplit(value)
    except ValueError as exc:
        raise _contract_error(path) from exc
    if parts.username is not None or parts.password is not None:
        raise _contract_error(path)
    return value


def _object_dict(value: object, path: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise _contract_error(path)
    result: dict[str, object] = {}
    for key, nested in value.items():
        if not isinstance(key, str):
            raise _contract_error(path)
        result[key] = nested
    return result


@dataclass(frozen=True)
class DirectResourceLocator:
    """A validated, serializable direct resource reference."""

    uri: str
    format: str | None = None
    media_type: str | None = None
    extensions: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_uri(self.uri, "uri")
        if self.format is not None and not isinstance(self.format, str):
            raise _contract_error("format")
        if self.media_type is not None and not isinstance(self.media_type, str):
            raise _contract_error("media_type")
        object.__setattr__(self, "extensions", _freeze_extensions(self.extensions))

    def to_dict(self) -> dict[str, object]:
        """Return a fresh, secret-free locator envelope."""
        from datasluice.domain.artifact import _thaw_json

        return {
            "schema_version": 1,
            "kind": "direct",
            "uri": sanitize_uri(self.uri),
            "format": self.format,
            "media_type": self.media_type,
            "extensions": _thaw_json(self.extensions),
        }

    @classmethod
    def from_dict(cls, value: object) -> DirectResourceLocator:
        """Decode one strict direct locator envelope."""
        data = _object_dict(value, "direct")
        if set(data) != _DIRECT_LOCATOR_KEYS:
            raise _contract_error("direct")
        if data["schema_version"] != 1 or type(data["schema_version"]) is not int or data["kind"] != "direct":
            raise _contract_error("direct")
        uri = data["uri"]
        format_name = data["format"]
        media_type = data["media_type"]
        extensions = data["extensions"]
        if (
            not isinstance(uri, str)
            or format_name is not None
            and not isinstance(format_name, str)
            or media_type is not None
            and not isinstance(media_type, str)
        ):
            raise _contract_error("direct")
        return cls(
            uri=uri,
            format=format_name,
            media_type=media_type,
            extensions=_object_dict(extensions, "extensions"),
        )


@dataclass(frozen=True)
class CatalogResourceLocator:
    """A validated, serializable catalog resource reference."""

    portal_url: str
    dataset_id: str
    resource_id: str
    extensions: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_uri(self.portal_url, "portal_url")
        if not isinstance(self.dataset_id, str) or not self.dataset_id:
            raise _contract_error("dataset_id")
        if not isinstance(self.resource_id, str) or not self.resource_id:
            raise _contract_error("resource_id")
        object.__setattr__(self, "extensions", _freeze_extensions(self.extensions))

    def to_dict(self) -> dict[str, object]:
        """Return a fresh, secret-free locator envelope."""
        from datasluice.domain.artifact import _thaw_json

        return {
            "schema_version": 1,
            "kind": "catalog",
            "portal_url": sanitize_uri(self.portal_url),
            "dataset_id": self.dataset_id,
            "resource_id": self.resource_id,
            "extensions": _thaw_json(self.extensions),
        }

    @classmethod
    def from_dict(cls, value: object) -> CatalogResourceLocator:
        """Decode one strict catalog locator envelope."""
        data = _object_dict(value, "catalog")
        if set(data) != _CATALOG_LOCATOR_KEYS:
            raise _contract_error("catalog")
        if data["schema_version"] != 1 or type(data["schema_version"]) is not int or data["kind"] != "catalog":
            raise _contract_error("catalog")
        portal_url = data["portal_url"]
        dataset_id = data["dataset_id"]
        resource_id = data["resource_id"]
        extensions = data["extensions"]
        if not isinstance(portal_url, str) or not isinstance(dataset_id, str) or not isinstance(resource_id, str):
            raise _contract_error("catalog")
        return cls(
            portal_url=portal_url,
            dataset_id=dataset_id,
            resource_id=resource_id,
            extensions=_object_dict(extensions, "extensions"),
        )


type ResourceLocator = DirectResourceLocator | CatalogResourceLocator


def resource_locator_from_dict(value: object) -> ResourceLocator:
    """Decode one strict, tagged ResourceLocator envelope."""
    data = _object_dict(value, "locator")
    kind = data.get("kind")
    if kind == "direct":
        return DirectResourceLocator.from_dict(data)
    if kind == "catalog":
        return CatalogResourceLocator.from_dict(data)
    raise _contract_error("kind")


def search_datasets(
    session: Any,
    url: str,
    query: str | Query | None = None,
    *,
    portal_type: str | None = None,
    **kwargs: Any,
) -> SearchResult:
    """Search one portal through injected session dependencies."""
    selected_query = query if isinstance(query, Query) else Query(text=query, **kwargs)
    return session.portal(url, portal_type=portal_type).search(selected_query)


def detect_portal(url: str, *, transport: Any, plugin_manager: Any) -> DetectionResult:
    """Detect a portal through caller-supplied infrastructure."""
    from datasluice.discovery import detect

    return detect(url, transport=transport, plugin_manager=plugin_manager)


def read_stream(resource: Resource, *, reader: Any) -> Any:
    """Open one resource through the injected data-plane reader."""
    return reader.open(resource)


def run_transform_pipeline(stream: Any, pipeline: Any) -> Any:
    """Apply a reusable transform pipeline to an existing stream."""
    return pipeline.run(stream)


def open_resource(resource: Resource, *, source_locator: ResourceLocator, reader: Any) -> OpenedResource:
    """Wrap one resolved resource for lazy, single-use consumption."""
    return OpenedResource(resource, source_locator=source_locator, reader=reader)


def materialize(
    resource: Resource,
    *,
    destination_uri: str,
    source_locator: ResourceLocator,
    reader: Any | None = None,
    stream: Any | None = None,
    mode: str = "parquet",
    transforms: tuple[str, ...] = (),
) -> Any:
    """Materialize one resource into its canonical Artifact record."""
    from datasluice.sync.materialize import materialize_artifact

    return materialize_artifact(
        resource,
        destination_uri=destination_uri,
        source_locator=source_locator,
        reader=reader,
        stream=stream,
        mode=mode,
        transforms=transforms,
    )


def _sanitized_resource_selectors(resources: list[Resource]) -> str:
    selectors = sorted({sanitize_uri(resource.id) for resource in resources})
    return ", ".join(selectors) or "(none)"


def _resolve_catalog_resource(session: Any, locator: CatalogResourceLocator) -> Resource:
    dataset = session.portal(locator.portal_url).get_dataset(locator.dataset_id)
    matches = [resource for resource in dataset.resources if resource.id == locator.resource_id]
    selectors = _sanitized_resource_selectors(dataset.resources)
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise ResourceResolutionError(
            f"Resource selector {sanitize_uri(locator.resource_id)!r} was not found in dataset "
            f"{sanitize_uri(locator.dataset_id)!r}. Valid selectors: {selectors}"
        )
    raise ResourceResolutionError(
        f"Resource selector {sanitize_uri(locator.resource_id)!r} is ambiguous in dataset "
        f"{sanitize_uri(locator.dataset_id)!r}. Valid selectors: {selectors}"
    )


def _resolve_direct_resource(locator: DirectResourceLocator) -> Resource:
    parts = urlsplit(locator.uri)
    identity_source = str(locator.to_dict()["uri"])
    resource_id = hashlib.sha256(identity_source.encode()).hexdigest()
    if parts.scheme == "file":
        access = LocalFile(path=unquote(parts.path))
    elif parts.scheme:
        access = HttpDownload(url=locator.uri)
    else:
        access = LocalFile(path=locator.uri)
    return Resource(
        id=resource_id,
        name=Path(unquote(parts.path or locator.uri)).name or None,
        url=locator.uri,
        format=locator.format,
        media_type=locator.media_type,
        access=access,
    )


class _ApplicationServices:
    """Private coordinator for application operations over one composition substrate."""

    def __init__(self, session: Any, reader: Any) -> None:
        self._session = session
        self._reader = reader

    def search(
        self,
        url: str,
        query: str | Query | None = None,
        *,
        portal_type: str | None = None,
        **kwargs: Any,
    ) -> SearchResult:
        """Search through the injected composition substrate."""
        return search_datasets(self._session, url, query, portal_type=portal_type, **kwargs)

    def detect(self, url: str) -> DetectionResult:
        """Run injected portal detection without facade-specific mapping."""
        return detect_portal(url, transport=self._session._transport, plugin_manager=self._session.plugins)

    def resolve(self, locator: ResourceLocator) -> Resource:
        """Resolve one public locator to the canonical Resource model."""
        if isinstance(locator, DirectResourceLocator):
            return _resolve_direct_resource(locator)
        return _resolve_catalog_resource(self._session, locator)

    def open(self, resource: Resource | ResourceLocator) -> OpenedResource:
        """Build one lazy opened-resource wrapper."""
        if isinstance(resource, Resource):
            resolved = resource
            source_locator = _locator_from_resource(resource)
        else:
            resolved = self.resolve(resource)
            source_locator = resource
        return open_resource(resolved, source_locator=source_locator, reader=self._reader)

    def materialize(
        self,
        resource: Resource | ResourceLocator,
        destination_uri: str,
        *,
        mode: str = "parquet",
    ) -> Any:
        """Materialize one resource through the application operation."""
        opened = self.open(resource)
        return opened.materialize(destination_uri, mode=mode)


class Portal:
    """Stable application wrapper for a portal URL."""

    def __init__(self, services: _ApplicationServices, url: str, portal_type: str | None = None) -> None:
        self._services = services
        self._url = url
        self._portal_type = portal_type

    def search(self, query: str | Query | None = None, **kwargs: Any) -> SearchResult:
        """Search through the facade without exposing a connector."""
        return self._services.search(self._url, query, portal_type=self._portal_type, **kwargs)


class DataSluice:
    """Canonical public facade for discovery, resource access, and materialization."""

    def __init__(
        self,
        *,
        session: Any | None = None,
        reader: Any | None = None,
        **session_kwargs: Any,
    ) -> None:
        if session is not None and session_kwargs:
            raise DataSluiceError("session= cannot be combined with session configuration")
        self._session = session if session is not None else DataSluiceSession(**session_kwargs)
        self._reader = reader if reader is not None else DataPlaneResourceReader(transport=self._session._transport)
        self._services = _ApplicationServices(self._session, self._reader)
        self._closed = False

    def portal(self, url: str, portal_type: str | None = None) -> Portal:
        """Return a stable Portal wrapper for *url*."""
        self._ensure_open()
        return Portal(self._services, url, portal_type)

    def search(self, url: str, query: str | Query | None = None, **kwargs: Any) -> SearchResult:
        """Search one portal through the session substrate."""
        self._ensure_open()
        return self._services.search(url, query, **kwargs)

    def detect(self, url: str) -> DetectionResult:
        """Detect a portal through the session's injected infrastructure."""
        self._ensure_open()
        return self._services.detect(url)

    def resolve(self, locator: ResourceLocator) -> Resource:
        """Resolve one public locator into the canonical Resource model."""
        self._ensure_open()
        return self._services.resolve(locator)

    def open(self, resource: Resource | ResourceLocator) -> OpenedResource:
        """Return a lazy, single-use OpenedResource wrapper."""
        self._ensure_open()
        return self._services.open(resource)

    def materialize(
        self,
        resource: Resource | ResourceLocator,
        destination_uri: str,
        *,
        mode: str = "parquet",
    ) -> Any:
        """Materialize one Resource or ResourceLocator into an Artifact."""
        return self._services.materialize(resource, destination_uri, mode=mode)

    def close(self) -> None:
        """Close this facade and any resource wrappers it owns."""
        self._closed = True

    def __enter__(self) -> DataSluice:
        self._ensure_open()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise StreamClosedError("DataSluice is closed")


class OpenedResource:
    """Lazy, single-use application wrapper over a Resource reader."""

    def __init__(self, resource: Resource, *, source_locator: ResourceLocator, reader: Any) -> None:
        self._resource = resource
        self._source_locator = source_locator
        self._reader = reader
        self._pipeline: Any | None = None
        self._stream: Any | None = None
        self._consumed = False
        self._closed = False

    @property
    def is_open(self) -> bool:
        """Whether the underlying data stream is currently open."""
        return self._stream is not None and not self._closed

    def transform(self, pipeline: Any) -> OpenedResource:
        """Attach one transform pipeline without opening the resource."""
        self._ensure_available()
        self._pipeline = pipeline
        return self

    def iter_batches(self) -> Iterator[Any]:
        """Iterate batches once, closing every stream when iteration finishes."""
        self._ensure_available()
        return self._iter_batches()

    def __iter__(self) -> Iterator[Any]:
        return self.iter_batches()

    def to_arrow(self) -> Any:
        """Consume this resource into an Arrow Table."""
        from datasluice.integrations.arrow import to_arrow

        return self._consume(to_arrow)

    def to_pandas(self) -> Any:
        """Consume this resource into a pandas DataFrame."""
        from datasluice.integrations.pandas import to_pandas

        return self._consume(to_pandas)

    def to_polars(self) -> Any:
        """Consume this resource into a polars DataFrame."""
        from datasluice.integrations.polars import to_polars

        return self._consume(to_polars)

    def to_duckdb(self, **kwargs: Any) -> Any:
        """Consume this resource into a DuckDB relation."""
        from datasluice.integrations.duckdb import to_duckdb

        return self._consume(lambda stream: to_duckdb(stream, **kwargs))

    def materialize(self, destination_uri: str, *, mode: str = "parquet") -> Any:
        """Materialize this resource once and return its Artifact envelope."""
        self._ensure_available()
        self._consumed = True
        transforms = () if self._pipeline is None else tuple(type(step).__name__ for step in self._pipeline.steps)
        try:
            if self._pipeline is None:
                return materialize(
                    self._resource,
                    destination_uri=destination_uri,
                    source_locator=self._source_locator,
                    reader=self._reader,
                    mode=mode,
                )
            raw_stream = self._reader.open(self._resource)
            self._stream = raw_stream
            stream = run_transform_pipeline(raw_stream, self._pipeline)
            return materialize(
                self._resource,
                destination_uri=destination_uri,
                source_locator=self._source_locator,
                stream=stream,
                mode=mode,
                transforms=transforms,
            )
        finally:
            self._finish(self._stream, None)

    def close(self) -> None:
        """Close an opened stream or prevent future consumption."""
        if self._closed:
            return
        self._closed = True
        if self._stream is not None:
            self._stream.close()
            self._stream = None

    def __enter__(self) -> OpenedResource:
        self._ensure_available()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _iter_batches(self) -> Iterator[Any]:
        raw_stream, stream = self._begin()
        try:
            yield from stream.iter_batches()
        finally:
            self._finish(raw_stream, stream)

    def _consume(self, operation: Callable[[Any], Any]) -> Any:
        raw_stream, stream = self._begin()
        try:
            return operation(stream)
        finally:
            self._finish(raw_stream, stream)

    def _begin(self) -> tuple[Any, Any]:
        self._ensure_available()
        self._consumed = True
        try:
            raw_stream = read_stream(self._resource, reader=self._reader)
            self._stream = raw_stream
            stream = raw_stream if self._pipeline is None else run_transform_pipeline(raw_stream, self._pipeline)
            return raw_stream, stream
        except BaseException:
            self._closed = True
            raise

    def _finish(self, raw_stream: Any | None, stream: Any | None) -> None:
        self._stream = None
        self._closed = True
        if stream is not None and stream is not raw_stream:
            stream.close()
        if raw_stream is not None:
            raw_stream.close()

    def _ensure_available(self) -> None:
        if self._closed or self._consumed:
            raise StreamClosedError("OpenedResource has already been consumed or closed")


def _locator_from_resource(resource: Resource) -> DirectResourceLocator:
    access = resource.access
    uri = resource.url or getattr(access, "url", None) or getattr(access, "uri", None)
    if uri is None and isinstance(access, LocalFile):
        uri = Path(access.path).resolve().as_uri()
    if not isinstance(uri, str) or not uri:
        raise DataSluiceError("Resource has no serializable direct locator")
    return DirectResourceLocator(uri=uri, format=resource.format, media_type=resource.media_type)
