"""Public application facade, locators, and opened-resource lifecycle."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

from datasluice._uri import sanitize_uri
from datasluice.contracts.catalog.protocols import CatalogConnectorContext
from datasluice.data.access import DataPlaneResourceReader
from datasluice.domain import HttpDownload, LocalFile, ObjectStorage, Resource
from datasluice.domain.artifact import _freeze_extensions
from datasluice.domain.catalog.auth import CredentialResolver
from datasluice.domain.catalog.profiles import DeclaredCapabilityProfile, EffectiveCapabilityProfile
from datasluice.exceptions import (
    DataSluiceError,
    OpenedResourceConsumedError,
    StreamClosedError,
)
from datasluice.runtime.clients import AsyncCatalogClient, SyncCatalogClient
from datasluice.runtime.session import DataSluiceSession

_DIRECT_LOCATOR_KEYS = frozenset({"schema_version", "kind", "uri", "format", "media_type", "extensions"})


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


def resource_locator_from_dict(value: object) -> DirectResourceLocator:
    """Decode one strict, tagged ResourceLocator envelope."""
    data = _object_dict(value, "locator")
    kind = data.get("kind")
    if kind == "direct":
        return DirectResourceLocator.from_dict(data)
    raise _contract_error("kind")


type ResourceLocator = DirectResourceLocator


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


_OBJECT_STORAGE_SCHEMES = frozenset({"s3", "gs", "gcs", "az", "azure", "abfs"})


def _resolve_direct_resource(locator: DirectResourceLocator) -> Resource:
    parts = urlsplit(locator.uri)
    identity_source = str(locator.to_dict()["uri"])
    resource_id = hashlib.sha256(identity_source.encode()).hexdigest()
    if parts.scheme == "file":
        access = LocalFile(path=unquote(parts.path))
    elif parts.scheme in _OBJECT_STORAGE_SCHEMES:
        access = ObjectStorage(uri=locator.uri)
    elif parts.scheme in ("http", "https"):
        access = HttpDownload(url=locator.uri)
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

    def resolve(self, locator: DirectResourceLocator) -> Resource:
        """Resolve one public locator to the canonical Resource model."""
        return _resolve_direct_resource(locator)

    def open(self, resource: Resource | DirectResourceLocator) -> OpenedResource:
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
        resource: Resource | DirectResourceLocator,
        destination_uri: str,
        *,
        mode: str = "parquet",
    ) -> Any:
        """Materialize one resource through the application operation."""
        opened = self.open(resource)
        return opened.materialize(destination_uri, mode=mode)

    def download_many(self, resources: list[Resource], destination: str) -> list[dict[str, object]]:
        """Raw bulk-copy multiple resources into a destination directory."""
        from datasluice.io.downloader import Downloader

        downloader = Downloader(self._session._transport)
        paths = downloader.download_many(resources, destination)
        results: list[dict[str, object]] = []
        for resource, path in zip(resources, paths, strict=False):
            try:
                size = path.stat().st_size
            except OSError:
                size = None
            results.append({"resource_id": resource.id, "path": str(path), "size": size})
        return results


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
        self._owns_session_dependencies = session is None
        self._owns_reader = reader is None
        self._session = session if session is not None else DataSluiceSession(**session_kwargs)
        self._reader = reader if reader is not None else DataPlaneResourceReader(transport=self._session._transport)
        self._services = _ApplicationServices(self._session, self._reader)
        self._owned_closeables = self._collect_owned_closeables(session_kwargs)
        self._closed = False

    def open_catalog[T](self, factory: Callable[[CatalogConnectorContext], T], context: CatalogConnectorContext) -> T:
        """Return one explicit caller-selected canonical catalog connector."""
        self._ensure_open()
        return self._session.open_catalog(factory, context)

    @property
    def credentials(self) -> CredentialResolver:
        """Return the session's explicit-only credential resolver."""
        self._ensure_open()
        return self._session.credentials

    def sync_client(self, profile: DeclaredCapabilityProfile | EffectiveCapabilityProfile) -> SyncCatalogClient:
        """Create one synchronous catalog client through the session runtime."""
        self._ensure_open()
        return self._session.sync_client(profile)

    def async_client(self, profile: DeclaredCapabilityProfile | EffectiveCapabilityProfile) -> AsyncCatalogClient:
        """Create one asynchronous catalog client through the session runtime."""
        self._ensure_open()
        return self._session.async_client(profile)

    def resolve(self, locator: DirectResourceLocator) -> Resource:
        """Resolve one public locator into the canonical Resource model."""
        self._ensure_open()
        return self._services.resolve(locator)

    def open(self, resource: Resource | DirectResourceLocator) -> OpenedResource:
        """Return a lazy, single-use OpenedResource wrapper."""
        self._ensure_open()
        return self._services.open(resource)

    def materialize(
        self,
        resource: Resource | DirectResourceLocator,
        destination_uri: str,
        *,
        mode: str = "parquet",
    ) -> Any:
        """Materialize one Resource or ResourceLocator into an Artifact."""
        self._ensure_open()
        return self._services.materialize(resource, destination_uri, mode=mode)

    def download_many(self, resources: list[Resource], destination: str) -> list[dict[str, object]]:
        """Raw bulk-copy resources into a destination directory."""
        self._ensure_open()
        return self._services.download_many(resources, destination)

    def close(self) -> None:
        """Close this facade and any resource wrappers it owns."""
        if self._closed:
            return
        self._closed = True
        first_error: BaseException | None = None
        for closeable in self._owned_closeables:
            try:
                closeable.close()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    def __enter__(self) -> DataSluice:
        self._ensure_open()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise StreamClosedError("DataSluice is closed")

    def _collect_owned_closeables(self, session_kwargs: Mapping[str, Any]) -> tuple[Any, ...]:
        candidates: list[Any] = []
        if self._owns_reader:
            candidates.append(self._reader)
        if self._owns_session_dependencies:
            if session_kwargs.get("transport") is None:
                candidates.append(self._session._transport)
            if session_kwargs.get("cache") is None:
                candidates.append(self._session._cache)
            if session_kwargs.get("storage") is None:
                candidates.append(self._session.storage)
            if session_kwargs.get("state_store") is None:
                candidates.append(self._session.state_store)
            if session_kwargs.get("plugins") is None:
                candidates.append(self._session.plugins)
        closeables: list[Any] = []
        seen: set[int] = set()
        for candidate in candidates:
            if candidate is None or not hasattr(candidate, "close") or id(candidate) in seen:
                continue
            seen.add(id(candidate))
            closeables.append(candidate)
        return tuple(closeables)


class OpenedResource:
    """Lazy, single-use application wrapper over a Resource reader."""

    def __init__(self, resource: Resource, *, source_locator: ResourceLocator, reader: Any) -> None:
        self._resource = resource
        self._source_locator = source_locator
        self._reader = reader
        self._pipeline: Any | None = None
        self._raw_stream: Any | None = None
        self._transformed_stream: Any | None = None
        self._consumed = False
        self._closed = False
        self._manual_iteration = False

    @property
    def is_open(self) -> bool:
        """Whether the underlying data stream is currently open."""
        return self._raw_stream is not None and not self._closed

    def transform(self, pipeline: Any) -> OpenedResource:
        """Attach one transform pipeline without opening the resource."""
        self._ensure_available()
        self._pipeline = pipeline
        return self

    def iter_batches(self) -> Iterator[Any]:
        """Iterate batches once, closing every stream when iteration finishes."""
        self._ensure_available()
        if not self._manual_iteration:
            raise OpenedResourceConsumedError("Manual batch iteration requires an OpenedResource context manager")
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
        transforms = () if self._pipeline is None else tuple(type(step).__name__ for step in self._pipeline.steps)
        return self._consume(
            lambda stream: materialize(
                self._resource,
                destination_uri=destination_uri,
                source_locator=self._source_locator,
                stream=stream,
                mode=mode,
                transforms=transforms,
            )
        )

    def close(self) -> None:
        """Close an opened stream or prevent future consumption."""
        if self._closed:
            return
        self._finish(self._raw_stream, self._transformed_stream)

    def __enter__(self) -> OpenedResource:
        self._ensure_available()
        self._manual_iteration = True
        return self

    def __exit__(self, *exc: Any) -> None:
        try:
            self.close()
        finally:
            self._manual_iteration = False

    def _iter_batches(self) -> Iterator[Any]:
        raw_stream, stream = self._begin()
        try:
            yield from stream.iter_batches()
        except BaseException:
            self._finish_after_failure(raw_stream, stream)
            raise
        else:
            self._finish(raw_stream, stream)

    def _consume(self, operation: Callable[[Any], Any]) -> Any:
        raw_stream, stream = self._begin()
        try:
            result = operation(stream)
        except BaseException:
            self._finish_after_failure(raw_stream, stream)
            raise
        else:
            self._finish(raw_stream, stream)
            return result

    def _begin(self) -> tuple[Any, Any]:
        self._ensure_available()
        self._consumed = True
        raw_stream: Any | None = None
        stream: Any | None = None
        try:
            raw_stream = read_stream(self._resource, reader=self._reader)
            self._raw_stream = raw_stream
            stream = raw_stream if self._pipeline is None else run_transform_pipeline(raw_stream, self._pipeline)
            self._transformed_stream = stream
            return raw_stream, stream
        except BaseException:
            self._finish_after_failure(raw_stream, stream)
            raise

    def _finish(self, raw_stream: Any | None, stream: Any | None) -> None:
        self._raw_stream = None
        self._transformed_stream = None
        self._closed = True
        first_error: BaseException | None = None
        for candidate in (stream, raw_stream):
            if candidate is None or candidate is raw_stream and stream is raw_stream:
                continue
            try:
                candidate.close()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        if raw_stream is not None and stream is raw_stream:
            try:
                raw_stream.close()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    def _finish_after_failure(self, raw_stream: Any | None, stream: Any | None) -> None:
        try:
            self._finish(raw_stream, stream)
        except BaseException:
            pass

    def _ensure_available(self) -> None:
        if self._closed or self._consumed:
            raise OpenedResourceConsumedError("OpenedResource has already been consumed or closed")


def _locator_from_resource(resource: Resource) -> DirectResourceLocator:
    access = resource.access
    uri = resource.url or getattr(access, "url", None) or getattr(access, "uri", None)
    if uri is None and isinstance(access, LocalFile):
        uri = Path(access.path).resolve().as_uri()
    if not isinstance(uri, str) or not uri:
        raise DataSluiceError("Resource has no serializable direct locator")
    return DirectResourceLocator(uri=uri, format=resource.format, media_type=resource.media_type)
