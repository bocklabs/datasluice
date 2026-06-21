"""Bearer-token (OAuth 2.0 / JWT) authentication strategy."""

from __future__ import annotations

from typing import Any

from datasluice.auth.base import BaseAuth


class BearerAuth(BaseAuth):
    """Authenticate requests using a bearer token in the ``Authorization`` header.

    Args:
        token: The bearer token (JWT or opaque token).
        scheme: Authentication scheme (default ``"Bearer"``).
    """

    def __init__(self, token: str, *, scheme: str = "Bearer") -> None:
        self.token = token
        self.scheme = scheme

    def apply(
        self, headers: dict[str, str], params: dict[str, Any] | None = None
    ) -> tuple[dict[str, str], dict[str, Any]]:
        headers = dict(headers)
        headers["Authorization"] = f"{self.scheme} {self.token}"
        return headers, dict(params or {})
