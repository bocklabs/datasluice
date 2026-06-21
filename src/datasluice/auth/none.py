"""No-op authentication strategy for public portals."""

from __future__ import annotations

from typing import Any

from datasluice.auth.base import BaseAuth


class NoAuth(BaseAuth):
    """Authentication strategy that adds no credentials.

    Suitable for portals that are fully open and require no API key.
    """

    def apply(
        self, headers: dict[str, str], params: dict[str, Any] | None = None
    ) -> tuple[dict[str, str], dict[str, Any]]:
        return dict(headers), dict(params or {})
