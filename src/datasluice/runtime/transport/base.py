"""Small transport records shared by the catalog runtime."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol

SENSITIVE_REDIRECT_HEADERS = frozenset({"authorization", "cookie", "proxy-authorization", "x-api-key", "x-auth-token"})

_HOP_BODYLESS_HEADERS = frozenset({"content-length", "content-type", "transfer-encoding"})
_POST_TO_GET_STATUSES = frozenset({301, 302})


def strip_sensitive_redirect_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Return a copy without credential-bearing headers."""
    return {key: value for key, value in headers.items() if key.lower() not in SENSITIVE_REDIRECT_HEADERS}


def drop_body_transfer_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Return a copy without body-describing headers, for bodyless redirect hops."""
    return {key: value for key, value in headers.items() if key.lower() not in _HOP_BODYLESS_HEADERS}


def redirect_method_and_body(method: str, status: int, body: bytes | None) -> tuple[str, bytes | None]:
    """Rewrite the request method and body for one redirect hop per RFC 9110.

    A ``303`` always downgrades to a bodyless ``GET``; a ``301``/``302``
    historically converts ``POST`` to a bodyless ``GET``; ``307`` and ``308``
    preserve the original method and body untouched.
    """
    if status == 303:
        return "GET", None
    if status in _POST_TO_GET_STATUSES and method.upper() == "POST":
        return "GET", None
    return method, body


@dataclass(frozen=True, slots=True)
class RuntimeRequest:
    """Immutable HTTP request supplied by a catalog client.

    Headers and bodies are excluded from ``repr`` so accidental logging or
    debugging of requests cannot expose credentials.
    """

    method: str
    url: str
    headers: Mapping[str, str] = field(default_factory=dict, repr=False)
    body: bytes | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.method, str) or not self.method:
            raise ValueError("Runtime request methods must be non-empty strings.")
        if not isinstance(self.url, str) or not self.url:
            raise ValueError("Runtime request URLs must be non-empty strings.")
        if self.body is not None and not isinstance(self.body, bytes):
            raise ValueError("Runtime request bodies must be bytes when supplied.")
        if self.headers is None or not isinstance(self.headers, Mapping):
            raise ValueError("Runtime request headers must be a mapping of string names to string values.")
        headers = dict(self.headers)
        if not all(isinstance(key, str) and key and isinstance(value, str) for key, value in headers.items()):
            raise ValueError("Runtime request headers must contain non-empty string names and string values.")
        object.__setattr__(self, "headers", MappingProxyType(headers))


@dataclass(frozen=True, slots=True)
class RuntimeResponse:
    """Fully buffered HTTP response returned by a catalog transport."""

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


class TransportFailure(RuntimeError):
    """A connectivity failure distinct from an HTTP status outcome."""


class CatalogTransport(Protocol):
    """Synchronous runtime transport port."""

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        """Send one request and return its fully buffered response."""

    def close(self) -> None:
        """Release transport resources."""
