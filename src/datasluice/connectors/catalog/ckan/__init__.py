"""Canonical public CKAN connector contract."""

from datasluice.connectors.catalog.ckan.clients import create_async_client, create_sync_client
from datasluice.connectors.catalog.ckan.connector import CKANConnector
from datasluice.connectors.catalog.ckan.factory import create_ckan_connector
from datasluice.connectors.catalog.ckan.settings import CKANClientSettings

__all__ = [
    "CKANClientSettings",
    "CKANConnector",
    "create_async_client",
    "create_ckan_connector",
    "create_sync_client",
]
