"""Canonical public CKAN connector contract."""

from datasluice.connectors.catalog.ckan.connector import CKANConnector
from datasluice.connectors.catalog.ckan.factory import create_ckan_connector

__all__ = ["CKANConnector", "create_ckan_connector"]
