"""Canonical public Socrata connector contract."""

from datasluice.connectors.catalog.socrata.adapter import SocrataAdapter
from datasluice.connectors.catalog.socrata.factory import create_socrata_connector

__all__ = ["SocrataAdapter", "create_socrata_connector"]
