"""HTTP Basic authentication strategy."""

from __future__ import annotations

from base64 import b64encode
from typing import Any

from datasluice.auth.base import BaseAuth


class BasicAuth(BaseAuth):
    """Authenticate requests using HTTP Basic credentials.

    Args:
        username: Basic-auth username.
        password: Basic-auth password.
    """

    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = password

    def __repr__(self) -> str:
        return f"<BasicAuth username={self.username!r} password=***>"

    def apply(
        self, headers: dict[str, str], params: dict[str, Any] | None = None
    ) -> tuple[dict[str, str], dict[str, Any]]:
        headers = dict(headers)
        token = b64encode(f"{self.username}:{self.password}".encode()).decode("ascii")
        headers["Authorization"] = f"Basic {token}"
        return headers, dict(params or {})
