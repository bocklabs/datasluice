"""Typed platform-native catalog Protocol families."""

from datasluice.contracts.catalog.native.ckan import AsyncCKANServices, SyncCKANServices
from datasluice.contracts.catalog.native.socrata import AsyncSocrataServices, SyncSocrataServices
from datasluice.contracts.catalog.native.udata import AsyncUDataServices, SyncUDataServices

__all__ = [
    "AsyncCKANServices",
    "AsyncSocrataServices",
    "AsyncUDataServices",
    "SyncCKANServices",
    "SyncSocrataServices",
    "SyncUDataServices",
]
