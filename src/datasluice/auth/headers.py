"""Custom-headers authentication strategy.

Useful for portals that expect a non-standard header for credentials.
"""

from __future__ import annotations

from typing import Any

from datasluice.auth.base import BaseAuth


class HeadersAuth(BaseAuth):
    """Authenticate requests using arbitrary static headers.

    Args:
        headers: A mapping of header names to values.
    """

    def __init__(self, headers: dict[str, str]) -> None:
        self._headers = dict(headers)

    def apply(
        self, headers: dict[str, str], params: dict[str, Any] | None = None
    ) -> tuple[dict[str, str], dict[str, Any]]:
        merged = dict(headers)
        merged.update(self._headers)
        return merged, dict(params or {})
