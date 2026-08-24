"""Small transport records shared by the catalog runtime."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol

from datasluice.domain.catalog.redaction import redact_string

SENSITIVE_REDIRECT_HEADERS = frozenset(
    {"authorization", "cookie", "proxy-authorization", "x-api-key", "x-auth-token", "x-app-token"}
)

_HOP_BODYLESS_HEADERS = frozenset({"content-length", "content-type", "transfer-encoding"})
_POST_TO_GET_STATUSES = frozenset({301, 302})


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
    data: bytes = field(repr=False)
    file_name: str | None = None
    content_type: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.field_name, str) or not self.field_name:
            raise ValueError("Upload part field names must be non-empty strings.")
        if not isinstance(self.data, bytes):
            raise ValueError("Upload part data must be bytes.")
        if self.file_name is not None and (not isinstance(self.file_name, str) or not self.file_name):
            raise ValueError("Upload part file names must be non-empty strings when supplied.")
        if self.content_type is not None and (not isinstance(self.content_type, str) or not self.content_type):
            raise ValueError("Upload part content types must be non-empty strings when supplied.")

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(field_name={self.field_name!r}, file_name={self.file_name!r}, "
            f"content_type={self.content_type!r}, data=<{len(self.data)} masked bytes>)"
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

    def __post_init__(self) -> None:
        if not isinstance(self.method, str) or not self.method:
            raise ValueError("Runtime request methods must be non-empty strings.")
        if not isinstance(self.url, str) or not self.url:
            raise ValueError("Runtime request URLs must be non-empty strings.")
        if self.body is not None and not isinstance(self.body, bytes):
            raise ValueError("Runtime request bodies must be bytes when supplied.")
        if not isinstance(self.files, (tuple, list)):
            raise ValueError("Runtime request multipart parts must be a tuple or list of UploadPart instances.")
        if not all(isinstance(part, UploadPart) for part in self.files):
            raise ValueError("Runtime request multipart parts must be UploadPart instances.")
        if self.body is not None and self.files:
            raise ValueError("Runtime requests cannot carry a byte body and multipart parts together.")
        if self.headers is None or not isinstance(self.headers, Mapping):
            raise ValueError("Runtime request headers must be a mapping of string names to string values.")
        headers = dict(self.headers)
        if not all(isinstance(key, str) and key and isinstance(value, str) for key, value in headers.items()):
            raise ValueError("Runtime request headers must contain non-empty string names and string values.")
        object.__setattr__(self, "headers", MappingProxyType(headers))
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


class TransportFailure(RuntimeError):
    """A connectivity failure distinct from an HTTP status outcome."""


class CatalogTransport(Protocol):
    """Synchronous runtime transport port."""

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        """Send one request and return its fully buffered response."""

    def close(self) -> None:
        """Release transport resources."""
