"""Credential provider port Protocol."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from datasluice.auth.base import BaseAuth


@runtime_checkable
class CredentialProvider(Protocol):
    """Boundary protocol resolving authentication strategies per host."""

    def resolve(self, host: str | None = None) -> BaseAuth: ...
