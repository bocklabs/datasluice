"""Connector construction context carrying injected infra ports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datasluice.auth import BaseAuth
    from datasluice.ports import Transport


@dataclass(frozen=True)
class ConnectorContext:
    """Context passed to ``create_*_connector(ctx)`` factory functions.

    Carries the infra seams plugins need without depending on concrete classes,
    so third-party connectors receive transport/auth via dependency injection
    rather than reaching for module-level globals.

    Attributes:
        base_url: Root URL of the portal instance.
        transport: HTTP transport satisfying the :class:`Transport` port.
        auth: Optional authentication strategy (``None`` means no auth).
        page_size: Default page size hint for paginated catalog calls.
    """

    base_url: str
    transport: Transport
    auth: BaseAuth | None = None
    page_size: int = 100
