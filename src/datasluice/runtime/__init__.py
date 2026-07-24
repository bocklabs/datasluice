"""Composition root and plugin machinery for DataSluice."""

from __future__ import annotations

from datasluice.runtime.context import ConnectorContext
from datasluice.runtime.plugin_manager import PluginFailure, PluginManager

__all__ = ["PluginManager", "PluginFailure", "ConnectorContext"]
