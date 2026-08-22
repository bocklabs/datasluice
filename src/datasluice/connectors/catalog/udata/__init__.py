"""Canonical public uData connector contract."""

from datasluice.connectors.catalog.udata.connector import UDataConnector
from datasluice.connectors.catalog.udata.factory import create_udata_connector

__all__ = ["UDataConnector", "create_udata_connector"]
