"""Canonical public CKAN connector contract."""

from datasluice.connectors.catalog.ckan.adapter import CKANAdapter
from datasluice.connectors.catalog.ckan.factory import create_ckan_connector

__all__ = ["CKANAdapter", "create_ckan_connector"]
