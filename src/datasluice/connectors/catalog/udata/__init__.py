"""Canonical public uData connector contract."""

from datasluice.connectors.catalog.udata.adapter import UDataAdapter
from datasluice.connectors.catalog.udata.factory import create_udata_connector

__all__ = ["UDataAdapter", "create_udata_connector"]
