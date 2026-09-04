"""uData live-client construction boundary."""

from __future__ import annotations

from typing import NoReturn

from datasluice.connectors.catalog.udata.clients import (
    AsyncUDataClient,
    SyncUDataClient,
    create_async_client,
    create_sync_client,
)
from datasluice.connectors.catalog.udata.settings import UDataClientSettings
from datasluice.runtime.extras import require_extra

__all__ = [
    "AsyncUDataClient",
    "SyncUDataClient",
    "UDataClientSettings",
    "create_async_client",
    "create_live_client",
    "create_sync_client",
]


def create_live_client() -> NoReturn:
    """Report the retired no-argument uData live seam after the extra gate.

    Raises:
        ImportError: If the uData connector extra is unavailable.
        NotImplementedError: Always, because live construction requires
            explicit ``UDataClientSettings`` through the typed factories.
    """
    require_extra("udata")
    raise NotImplementedError(
        "The no-argument uData live seam is retired; construct the client with "
        "create_sync_client(UDataClientSettings(...)) or create_async_client(UDataClientSettings(...))."
    )
