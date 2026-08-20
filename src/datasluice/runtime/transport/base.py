"""Small transport records shared by the catalog runtime."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol


@dataclass(frozen=True, slots=True)
class RuntimeRequest:
    """Immutable HTTP request supplied by a catalog client."""

    method: str
    url: str
    headers: Mapping[str, str] = field(default_factory=dict)
    body: bytes | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.method, str) or not self.method:
            raise ValueError("Runtime request methods must be non-empty strings.")
        if not isinstance(self.url, str) or not self.url:
            raise ValueError("Runtime request URLs must be non-empty strings.")
        if self.body is not None and not isinstance(self.body, bytes):
            raise ValueError("Runtime request bodies must be bytes when supplied.")
        headers = dict(self.headers)
        if not all(isinstance(key, str) and key and isinstance(value, str) for key, value in headers.items()):
            raise ValueError("Runtime request headers must contain non-empty string names and string values.")
        object.__setattr__(self, "headers", MappingProxyType(headers))


@dataclass(frozen=True, slots=True)
class RuntimeResponse:
    """Fully buffered HTTP response returned by a catalog transport."""

    status_code: int
    headers: Mapping[str, str]
    body: bytes
    retry_after: float | None = None

    def __post_init__(self) -> None:
        if type(self.status_code) is not int or not 100 <= self.status_code <= 599:
            raise ValueError("Runtime response status codes must be valid HTTP status codes.")
        if not isinstance(self.body, bytes):
            raise ValueError("Runtime response bodies must be bytes.")
        if self.retry_after is not None and (not isinstance(self.retry_after, int | float) or self.retry_after < 0):
            raise ValueError("Runtime response Retry-After must be non-negative.")
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
