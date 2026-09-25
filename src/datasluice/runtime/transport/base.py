"""Small transport records shared by the catalog runtime."""

from __future__ import annotations

import math
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol

from datasluice.domain.catalog.redaction import redact_string

SENSITIVE_REDIRECT_HEADERS = frozenset(
    {"authorization", "cookie", "proxy-authorization", "x-api-key", "x-auth-token", "x-app-token"}
)

_HOP_BODYLESS_HEADERS = frozenset({"content-length", "content-type", "transfer-encoding"})
_POST_TO_GET_STATUSES = frozenset({301, 302})


class RedirectPolicy(StrEnum):
    """Declare whether a transport may follow an HTTP redirect."""

    FOLLOW = "follow"
    NO_FOLLOW = "no-follow"


class UploadStream(Protocol):
    """A closeable byte stream used for one multipart part."""

    def read(self, size: int = -1) -> bytes: ...

    def close(self) -> None: ...


def strip_sensitive_redirect_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Return a copy without credential-bearing headers."""
    return {key: value for key, value in headers.items() if key.lower() not in SENSITIVE_REDIRECT_HEADERS}


def drop_body_transfer_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Return a copy without body-describing headers, for bodyless redirect hops."""
    return {key: value for key, value in headers.items() if key.lower() not in _HOP_BODYLESS_HEADERS}


@dataclass(frozen=True, slots=True)
class UploadPart:
    """One immutable multipart form part buffered for upload.

    Part data is excluded from ``repr`` so debugging or logging a part can
    never surface uploaded file contents; only field name, file name,
    content type, and the data byte length render.
    """

    field_name: str
    data: bytes | UploadStream = field(repr=False)
    file_name: str | None = None
    content_type: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.field_name, str) or not self.field_name:
            raise ValueError("Upload part field names must be non-empty strings.")
        if not isinstance(self.data, bytes) and not (
            callable(getattr(self.data, "read", None)) and callable(getattr(self.data, "close", None))
        ):
            raise ValueError("Upload part data must be bytes or a closeable byte stream.")
        if self.file_name is not None and (not isinstance(self.file_name, str) or not self.file_name):
            raise ValueError("Upload part file names must be non-empty strings when supplied.")
        if self.content_type is not None and (not isinstance(self.content_type, str) or not self.content_type):
            raise ValueError("Upload part content types must be non-empty strings when supplied.")

    def __repr__(self) -> str:
        size = len(self.data) if isinstance(self.data, bytes) else "stream"
        return (
            f"{type(self).__name__}(field_name={self.field_name!r}, file_name={self.file_name!r}, "
            f"content_type={self.content_type!r}, data=<{size} masked bytes>)"
        )


def redirect_method_and_body(
    method: str,
    status: int,
    body: bytes | None,
    files: tuple[UploadPart, ...] = (),
) -> tuple[str, bytes | None, tuple[UploadPart, ...]]:
    """Rewrite the request method, body, and multipart parts for one redirect hop per RFC 9110.

    A ``303`` always downgrades to a bodyless, fileless ``GET``; a
    ``301``/``302`` historically converts ``POST`` to a bodyless, fileless
    ``GET``; ``307`` and ``308`` preserve the original method, body, and parts
    untouched.
    """
    if status == 303:
        return "GET", None, ()
    if status in _POST_TO_GET_STATUSES and method.upper() == "POST":
        return "GET", None, ()
    return method, body, files


def _validate_request_payload(body: bytes | None, files: tuple[UploadPart, ...] | list[UploadPart]) -> None:
    if body is not None and not isinstance(body, bytes):
        raise ValueError("Runtime request bodies must be bytes when supplied.")
    if not isinstance(files, (tuple, list)):
        raise ValueError("Runtime request multipart parts must be a tuple or list of UploadPart instances.")
    if not all(isinstance(part, UploadPart) for part in files):
        raise ValueError("Runtime request multipart parts must be UploadPart instances.")
    if body is not None and files:
        raise ValueError("Runtime requests cannot carry a byte body and multipart parts together.")


def _validate_response_limit(max_response_bytes: int | None) -> None:
    if max_response_bytes is not None and (type(max_response_bytes) is not int or max_response_bytes < 1):
        raise ValueError("Runtime request response limits must be positive integers when supplied.")


def _freeze_request_headers(headers: Mapping[str, str]) -> Mapping[str, str]:
    if headers is None or not isinstance(headers, Mapping):
        raise ValueError("Runtime request headers must be a mapping of string names to string values.")
    values = dict(headers)
    if not all(isinstance(key, str) and key and isinstance(value, str) for key, value in values.items()):
        raise ValueError("Runtime request headers must contain non-empty string names and string values.")
    return MappingProxyType(values)


@dataclass(frozen=True, slots=True)
class RuntimeRequest:
    """Immutable HTTP request supplied by a catalog client.

    Headers, bodies, and multipart parts are excluded from ``repr`` so
    accidental logging or debugging of requests cannot expose credentials or
    uploaded file contents, and the URL renders through the shared redaction
    helper. ``__hash__`` projects over the hashable ``method`` and ``url``
    fields only; equality remains full-field via the generated ``eq`` and
    still compares headers, body, and parts.
    """

    method: str
    url: str
    headers: Mapping[str, str] = field(default_factory=dict, repr=False)
    body: bytes | None = field(default=None, repr=False)
    files: tuple[UploadPart, ...] = field(default=(), repr=False)
    redirect_policy: RedirectPolicy = RedirectPolicy.FOLLOW
    max_response_bytes: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.method, str) or not self.method:
            raise ValueError("Runtime request methods must be non-empty strings.")
        if not isinstance(self.url, str) or not self.url:
            raise ValueError("Runtime request URLs must be non-empty strings.")
        _validate_request_payload(self.body, self.files)
        if not isinstance(self.redirect_policy, RedirectPolicy):
            raise ValueError("Runtime request redirect policies must use RedirectPolicy.")
        if self.redirect_policy is RedirectPolicy.FOLLOW and any(
            not isinstance(part.data, bytes) for part in self.files
        ):
            raise ValueError("Multipart parts carrying one-shot streams require RedirectPolicy.NO_FOLLOW.")
        _validate_response_limit(self.max_response_bytes)
        object.__setattr__(self, "headers", _freeze_request_headers(self.headers))
        object.__setattr__(self, "files", tuple(self.files))

    def __repr__(self) -> str:
        return f"{type(self).__name__}(method={self.method!r}, url={redact_string(self.url)!r})"

    def __hash__(self) -> int:
        return hash((self.method, self.url))


@dataclass(frozen=True, slots=True)
class RuntimeResponse:
    """Fully buffered HTTP response returned by a catalog transport.

    The custom ``repr`` masks the body (rendering only its byte length) and
    omits headers, so debugging cannot expose credential-bearing payloads.
    ``__hash__`` projects over the hashable ``status_code`` and ``retry_after``
    fields; equality remains full-field via the generated ``eq`` and still
    compares headers and body.
    """

    status_code: int
    headers: Mapping[str, str] = field(repr=False)
    body: bytes
    retry_after: float | None = None

    def __post_init__(self) -> None:
        if type(self.status_code) is not int or not 100 <= self.status_code <= 599:
            raise ValueError("Runtime response status codes must be valid HTTP status codes.")
        if self.headers is None or not isinstance(self.headers, Mapping):
            raise ValueError("Runtime response headers must be a mapping of string names to string values.")
        if not all(isinstance(key, str) and key and isinstance(value, str) for key, value in self.headers.items()):
            raise ValueError("Runtime response headers must contain non-empty string names and string values.")
        if not isinstance(self.body, bytes):
            raise ValueError("Runtime response bodies must be bytes.")
        if self.retry_after is not None and (
            type(self.retry_after) not in (int, float) or not math.isfinite(self.retry_after) or self.retry_after < 0
        ):
            raise ValueError("Runtime response Retry-After must be a finite non-negative number.")
        object.__setattr__(self, "headers", MappingProxyType(dict(self.headers)))
        if self.retry_after is not None:
            object.__setattr__(self, "retry_after", float(self.retry_after))

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(status_code={self.status_code!r}, "
            f"body=<{len(self.body)} masked bytes>, retry_after={self.retry_after!r})"
        )

    def __hash__(self) -> int:
        return hash((self.status_code, self.retry_after))


@dataclass(slots=True)
class RuntimeStreamResponse:
    """One closeable synchronous response whose bytes are yielded incrementally."""

    status_code: int
    headers: Mapping[str, str]
    chunks: Iterator[bytes] = field(repr=False)
    close_callback: Callable[[], None] = field(repr=False)
    retry_after: float | None = None
    failure_callback: Callable[[BaseException], None] | None = field(default=None, repr=False)
    completion_callback: Callable[[], None] | None = field(default=None, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        if type(self.status_code) is not int or not 100 <= self.status_code <= 599:
            raise ValueError("Runtime stream response status codes must be valid HTTP status codes.")
        if self.headers is None or not isinstance(self.headers, Mapping):
            raise ValueError("Runtime stream response headers must be a mapping of string names to string values.")
        headers = dict(self.headers)
        if not all(isinstance(key, str) and key and isinstance(value, str) for key, value in headers.items()):
            raise ValueError("Runtime stream response headers must contain non-empty string names and string values.")
        if not callable(self.close_callback):
            raise ValueError("Runtime stream responses require a close callback.")
        if self.retry_after is not None and (
            type(self.retry_after) not in (int, float) or not math.isfinite(self.retry_after) or self.retry_after < 0
        ):
            raise ValueError("Runtime stream response Retry-After must be a finite non-negative number.")
        self.headers = MappingProxyType(headers)
        if self.retry_after is not None:
            self.retry_after = float(self.retry_after)

    def __iter__(self) -> Iterator[bytes]:
        for chunk in self.chunks:
            if not isinstance(chunk, bytes):
                raise TransportFailure("The catalog transport yielded a non-byte stream chunk.")
            if chunk:
                yield chunk

    def close(self) -> None:
        """Release the stream resource exactly once."""
        if not self._closed:
            self._closed = True
            self.close_callback()

    def fail(self, error: BaseException) -> None:
        """Report a failure discovered while consuming the stream."""
        if self.failure_callback is not None:
            self.failure_callback(error)

    def complete(self) -> None:
        """Report that the caller consumed and validated the complete stream."""
        if self.completion_callback is not None:
            self.completion_callback()


@dataclass(slots=True)
class AsyncRuntimeStreamResponse:
    """One closeable asynchronous response whose bytes are yielded incrementally."""

    status_code: int
    headers: Mapping[str, str]
    chunks: AsyncIterator[bytes] = field(repr=False)
    close_callback: Callable[[], Awaitable[None] | None] = field(repr=False)
    retry_after: float | None = None
    failure_callback: Callable[[BaseException], Awaitable[None] | None] | None = field(default=None, repr=False)
    completion_callback: Callable[[], Awaitable[None] | None] | None = field(default=None, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        if type(self.status_code) is not int or not 100 <= self.status_code <= 599:
            raise ValueError("Async runtime stream response status codes must be valid HTTP status codes.")
        if self.headers is None or not isinstance(self.headers, Mapping):
            raise ValueError(
                "Async runtime stream response headers must be a mapping of string names to string values."
            )
        headers = dict(self.headers)
        if not all(isinstance(key, str) and key and isinstance(value, str) for key, value in headers.items()):
            raise ValueError(
                "Async runtime stream response headers must contain non-empty string names and string values."
            )
        if not callable(self.close_callback):
            raise ValueError("Async runtime stream responses require a close callback.")
        if self.retry_after is not None and (
            type(self.retry_after) not in (int, float) or not math.isfinite(self.retry_after) or self.retry_after < 0
        ):
            raise ValueError("Async runtime stream response Retry-After must be a finite non-negative number.")
        self.headers = MappingProxyType(headers)
        if self.retry_after is not None:
            self.retry_after = float(self.retry_after)

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for chunk in self.chunks:
            if not isinstance(chunk, bytes):
                raise TransportFailure("The catalog transport yielded a non-byte stream chunk.")
            if chunk:
                yield chunk

    async def aclose(self) -> None:
        """Release the asynchronous stream resource exactly once."""
        if not self._closed:
            self._closed = True
            result = self.close_callback()
            if result is not None:
                await result

    async def fail(self, error: BaseException) -> None:
        """Report a failure discovered while consuming the stream."""
        if self.failure_callback is not None:
            result = self.failure_callback(error)
            if result is not None:
                await result

    async def complete(self) -> None:
        """Report that the caller consumed and validated the complete stream."""
        if self.completion_callback is not None:
            result = self.completion_callback()
            if result is not None:
                await result


class TransportFailure(RuntimeError):
    """A connectivity failure distinct from an HTTP status outcome."""


class CatalogTransport(Protocol):
    """Synchronous runtime transport port."""

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        """Send one request and return its fully buffered response."""

    def close(self) -> None:
        """Release transport resources."""


class StreamingCatalogTransport(CatalogTransport, Protocol):
    """Synchronous transport port that can expose a response body incrementally."""

    def send_stream(self, request: RuntimeRequest) -> RuntimeStreamResponse:
        """Send one request without pre-buffering its response body."""
