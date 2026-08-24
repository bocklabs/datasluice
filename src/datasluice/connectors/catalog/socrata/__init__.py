"""Canonical public Socrata connector contract."""

from datasluice.connectors.catalog.socrata.connector import SocrataConnector
from datasluice.connectors.catalog.socrata.factory import create_socrata_connector

__all__ = ["SocrataConnector", "create_socrata_connector"]
