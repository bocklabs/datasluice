"""API-key authentication strategy.

Supports passing the key via a header (default ``X-Api-Key``) or as a
query parameter.
"""

from __future__ import annotations

from typing import Any

from datasluice.auth.base import BaseAuth


class APIKeyAuth(BaseAuth):
    """Authenticate requests using an API key.

    Args:
        api_key: The API key value.
        header_name: Name of the header to carry the key
            (default ``"X-Api-Key"``).
        param_name: If set, the key is sent as a query parameter with this
            name instead of (or in addition to) a header.
        in_header: Whether to add the key to headers (default ``True``).
        in_query: Whether to add the key to query params (default ``False``).
    """

    def __init__(
        self,
        api_key: str,
        *,
        header_name: str = "X-Api-Key",
        param_name: str | None = None,
        in_header: bool = True,
        in_query: bool = False,
    ) -> None:
        self.api_key = api_key
        self.header_name = header_name
        self.param_name = param_name
        self.in_header = in_header
        self.in_query = in_query

    def __repr__(self) -> str:
        return (
            f"<APIKeyAuth header_name={self.header_name!r} param_name={self.param_name!r} "
            f"in_header={self.in_header!r} in_query={self.in_query!r} api_key=***>"
        )

    def apply(
        self, headers: dict[str, str], params: dict[str, Any] | None = None
    ) -> tuple[dict[str, str], dict[str, Any]]:
        headers = dict(headers)
        params = dict(params or {})
        if self.in_header:
            headers[self.header_name] = self.api_key
        if self.in_query and self.param_name:
            params[self.param_name] = self.api_key
        return headers, params
