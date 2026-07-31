"""Concrete ``ResourceReader`` implementation with access-kind dispatch (DATA-04, DATA-05, D-P4-06/07/09/15).

The :class:`DataPlaneResourceReader` resolves how a resource is reached, acquires
a byte source, transparently decompresses it, dispatches to the right format
reader, and wraps the resulting ``RecordBatch`` iterator in a
:class:`datasluice.data.batch_stream.BatchStream`.

Dispatch by ``resource.access.kind``:

* ``http_download`` (default when ``resource.access`` is None — every resource has
  a URL today): probe ``isinstance(transport, StreamingTransport)`` per the
  Phase 3 capability-check pattern (D-P3-06). Streaming path wraps
  ``StreamResponse`` in :class:`IterableBytesIO` (non-seekable). Buffered path
  (urllib ``HttpClient``) reads the full body into :class:`io.BytesIO` and logs
  a WARNING recommending ``pip install datasluice[http]`` (D-P4-15).
* ``object_storage``: ``open_filesystem(uri).open(path)`` returning a seekable
  BinaryIO.
* ``local_file``: ``open(path, 'rb')``.
* ``query``: raises :class:`UnsupportedAccessError` (Phase 5 adds query readers
  for CKAN datastore and Socrata SoQL).
* ``stream``: raises :class:`UnsupportedAccessError` (out of scope).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from datasluice.data._byte_source import IterableBytesIO
from datasluice.data.batch_stream import BatchCursor, BatchStream, ParquetRowGroupPosition
from datasluice.data.compression import apply_compression
from datasluice.data.readers import get_reader
from datasluice.exceptions import DataSluiceError, UnsupportedAccessError
from datasluice.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Iterator

    from datasluice.domain import Resource

logger = get_logger("data.access")


_DEFAULT_BATCH_SIZE = 65536
_BATCH_LIFECYCLE_READY = True


class _StreamClosingBytesIO(IterableBytesIO):
    """IterableBytesIO that releases the StreamResponse + transport context on close."""

    def __init__(self, byte_iter: Iterator[bytes], response: Any, stream_cm: Any) -> None:
        super().__init__(byte_iter)
        self._response = response
        self._stream_cm = stream_cm
        self._stream_closed = False

    def close(self) -> None:
        if self._stream_closed:
            return
        self._stream_closed = True
        try:
            super().close()
        finally:
            if hasattr(self._response, "close"):
                self._response.close()
            self._stream_cm.__exit__(None, None, None)


class DataPlaneResourceReader:
    """Concrete ``ResourceReader`` implementing access-kind dispatch (DATA-04).

    Args:
        transport: A transport satisfying the :class:`~datasluice.ports.Transport`
            Protocol (typically :class:`HttpxTransport`). ``None`` is permitted
            for callers that only read local files / object storage (no HTTP
            access kind will be exercised).
        batch_size: Default row-count hint for the underlying format reader
            (default 65536 per D-P4-14).
    """

    def __init__(self, transport: Any | None = None, *, batch_size: int = _DEFAULT_BATCH_SIZE) -> None:
        self.transport = transport
        self.default_batch_size = batch_size

    def _resolve_access(self, resource: Resource) -> Any:
        """Return the resource's access descriptor, defaulting to HttpDownload (D-P4-06)."""

        from datasluice.domain import HttpDownload

        if resource.access is not None:
            return resource.access
        url = resource.url
        if url is None:
            raise UnsupportedAccessError(
                f"Resource {resource.id!r} has no access descriptor and no url; cannot default to HttpDownload"
            )
        return HttpDownload(url=url)

    def open(self, resource: Resource, *, batch_size: int | None = None) -> BatchStream:
        """Open *resource* as a :class:`BatchStream` of Arrow ``RecordBatch``.

        Dispatches on ``resource.access.kind`` (defaulting to
        ``http_download``) to acquire a binary byte source, transparently
        decompresses it via :func:`apply_compression`, dispatches to the
        appropriate format reader (via :func:`get_reader`), and wraps the
        resulting iterator in :class:`BatchStream`.

        Args:
            resource: The resource to open.
            batch_size: Optional row-count hint overriding ``self.default_batch_size``.

        Returns:
            A :class:`BatchStream` ready for iteration.

        Raises:
            UnsupportedAccessError: If the access kind has no implementation
                (``query``, ``stream``) or no transport was supplied for HTTP.
        """

        effective_batch_size = batch_size if batch_size is not None else self.default_batch_size
        _validate_batch_size(effective_batch_size)
        access = self._resolve_access(resource)

        kind = access.kind
        if kind == "http_download":
            source, content_encoding = self._open_http_download(access)
        elif kind == "object_storage":
            source, content_encoding = self._open_object_storage(access)
        elif kind == "local_file":
            source, content_encoding = self._open_local_file(access)
        elif kind == "query":
            raise UnsupportedAccessError(
                "Access kind 'query' is not implemented in Phase 4; Phase 5 adds query readers "
                f"(CKAN datastore, Socrata SoQL) for endpoint {getattr(access, 'endpoint', '<unknown>')!r}"
            )
        elif kind == "stream":
            raise UnsupportedAccessError(
                "Access kind 'stream' is out of scope — no target portal supports live streaming endpoints"
            )
        else:
            raise UnsupportedAccessError(f"Unknown access kind {kind!r} on resource {resource.id!r}")

        if (resource.format or "").upper() == "PARQUET" and kind in ("local_file", "object_storage"):
            if content_encoding is not None:
                source.close()
                raise UnsupportedAccessError(
                    f"Checkpointable Parquet resource {resource.id!r} must not be transport-compressed"
                )
            from datasluice.data.compression import _detect_format

            magic = source.read(6)
            source.seek(0)
            if _detect_format(magic, None) != "none":
                source.close()
                raise UnsupportedAccessError(f"Checkpointable Parquet resource {resource.id!r} must not be compressed")
            return self._build_parquet_cursor_stream(source, start_row_group_index=0, start_batch_index=0)
        decompressed = apply_compression(source, content_encoding)
        return self._build_batch_stream(resource, decompressed, effective_batch_size)

    def open_response(
        self,
        resource: Resource,
        stream_cm: Any,
        *,
        headers: Any,
        batch_size: int | None = None,
    ) -> BatchStream:
        """Open an already-fetched streaming response through the data plane."""
        effective_batch_size = batch_size if batch_size is not None else self.default_batch_size
        _validate_batch_size(effective_batch_size)
        response = stream_cm.__enter__()
        entered = True
        source: Any | None = None
        try:
            response_headers = dict(headers) if headers is not None else {}
            content_encoding = _content_encoding_from_headers(response_headers)
            source = _StreamClosingBytesIO(iter(response), response, stream_cm)
            decompressed = apply_compression(source, content_encoding)
            return self._build_batch_stream(resource, decompressed, effective_batch_size)
        except BaseException:
            if source is not None:
                source.close()
            elif entered:
                stream_cm.__exit__(None, None, None)
            raise

    def open_from_cursor(
        self,
        resource: Resource,
        cursor: BatchCursor,
        *,
        batch_size: int | None = None,
    ) -> BatchStream:
        """Open a seekable Parquet resource from an exact row-group cursor."""
        effective_batch_size = batch_size if batch_size is not None else self.default_batch_size
        _validate_batch_size(effective_batch_size)
        if not isinstance(cursor, BatchCursor) or not isinstance(cursor.position, ParquetRowGroupPosition):
            raise UnsupportedAccessError(
                f"Continuation for resource {resource.id!r} requires a ParquetRowGroupPosition cursor"
            )
        if (resource.format or "").upper() != "PARQUET":
            raise UnsupportedAccessError(f"Continuation for resource {resource.id!r} supports only PARQUET resources")
        access = self._resolve_access(resource)
        if access.kind == "local_file":
            source, content_encoding = self._open_local_file(access)
        elif access.kind == "object_storage":
            source, content_encoding = self._open_object_storage(access)
        else:
            raise UnsupportedAccessError(
                f"Continuation for resource {resource.id!r} at row group "
                f"{cursor.position.row_group_index} requires local_file or object_storage access; "
                f"{access.kind!r} cannot resume without restarting at byte zero"
            )
        if content_encoding is not None:
            source.close()
            raise UnsupportedAccessError(
                f"Continuation for compressed Parquet resource {resource.id!r} is not supported"
            )
        return self._build_parquet_cursor_stream(
            source,
            start_row_group_index=cursor.position.row_group_index,
            start_batch_index=cursor.next_batch_index,
        )

    def _build_parquet_cursor_stream(
        self,
        source: Any,
        *,
        start_row_group_index: int,
        start_batch_index: int,
    ) -> BatchStream:
        from datasluice.data.readers.parquet import ParquetReader

        try:
            pairs = ParquetReader().read_batches_from_row_group(source, start_row_group_index=start_row_group_index)
            first_pair = next(pairs, None)
        except BaseException:
            _close_source(source)
            raise
        if first_pair is None:
            import pyarrow as pa

            schema = pa.schema([])
            pair_iter: Iterator[Any] = iter(())
        else:
            schema = first_pair[1].schema
            pair_iter = _chain(first_pair, pairs)
        try:
            return BatchStream(
                pair_iter,
                schema,
                start_batch_index=start_batch_index,
                start_row_group_index=start_row_group_index,
                indexed=True,
                closeables=(source,),
            )
        except BaseException:
            _close_source(source)
            raise

    def _build_batch_stream(
        self,
        resource: Resource,
        source: Any,
        batch_size: int,
    ) -> BatchStream:
        """Dispatch to the format reader and wrap output in BatchStream."""

        try:
            format_name = resource.format or "CSV"
            reader = get_reader(format_name)
            batches = reader.read_batches(source, batch_size=batch_size)
            first_batch = next(batches, None)
            if first_batch is None:
                import pyarrow as pa

                schema = pa.schema([])
                batch_iter: Iterator[Any] = iter(())
            else:
                schema = first_batch.schema
                batch_iter = _chain(first_batch, batches)
            return BatchStream(batch_iter, schema, closeables=(source,))
        except BaseException:
            _close_source(source)
            raise

    def _open_http_download(self, access: Any) -> tuple[Any, str | None]:
        """HttpDownload: stream via StreamingTransport or buffer via urllib fallback (D-P4-15)."""

        if self.transport is None:
            raise UnsupportedAccessError(
                f"HttpDownload for {getattr(access, 'url', '<unknown>')!r} requires a transport; "
                "pass transport=HttpxTransport() or install datasluice[http]"
            )

        from datasluice.ports import StreamingTransport

        url = access.url

        if isinstance(self.transport, StreamingTransport):
            stream_cm = self.transport.stream(url)
            response = stream_cm.__enter__()
            try:
                headers = dict(response.headers) if hasattr(response, "headers") else {}
                content_encoding = _content_encoding_from_headers(headers)
                source = _StreamClosingBytesIO(iter(response), response, stream_cm)
                return source, content_encoding
            except BaseException:
                stream_cm.__exit__(None, None, None)
                raise

        logger.warning(
            "Transport %s does not satisfy StreamingTransport; buffering HTTP body in memory. "
            "Install datasluice[http] (httpx) for bounded-memory streaming reads.",
            type(self.transport).__name__,
        )
        body = self.transport.download(url)
        import io

        return io.BytesIO(body), None

    def _open_object_storage(self, access: Any) -> tuple[Any, str | None]:
        """ObjectStorage: open via open_filesystem().open() (D-P4-07)."""

        from datasluice.io.filesystem import open_filesystem

        uri = access.uri
        fs = open_filesystem(uri)
        path = _strip_scheme(uri, fs)
        return fs.open(path, "rb"), None

    def _open_local_file(self, access: Any) -> tuple[Any, str | None]:
        """LocalFile: open(path, 'rb')."""

        return open(access.path, "rb"), None


def _chain(first: Any, rest: Iterator[Any]) -> Iterator[Any]:
    yield first
    yield from rest


def _validate_batch_size(batch_size: Any) -> None:
    if type(batch_size) is not int or batch_size <= 0:
        raise DataSluiceError(f"batch_size must be a positive integer, got {batch_size!r}")


def _close_source(source: Any) -> None:
    close = getattr(source, "close", None)
    if not callable(close):
        return
    try:
        close()
    except Exception:
        logger.warning("Failed to close byte source after data-plane construction failure")


def _content_encoding_from_headers(headers: dict[str, str]) -> str | None:
    """Extract a lowercased Content-Encoding value from response headers."""

    for key, value in headers.items():
        if key.lower() == "content-encoding":
            return value.lower()
    return None


def _strip_scheme(uri: str, fs: Any) -> str:
    """Strip the fsspec storage scheme from *uri* to produce the path component."""

    protocol = getattr(fs, "protocol", None)
    if isinstance(protocol, str):
        prefix = f"{protocol}://"
        if uri.startswith(prefix):
            return uri[len(prefix) :]
        prefix_single = f"{protocol}:"
        if uri.startswith(prefix_single):
            return uri[len(prefix_single) :].lstrip("/")
    if "://" in uri:
        return uri.split("://", 1)[1].lstrip("/")
    return uri
