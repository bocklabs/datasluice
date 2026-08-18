"""Composition root and plugin machinery for DataSluice."""

from __future__ import annotations

from datasluice.runtime.defaults import create_default_transport
from datasluice.runtime.plugin_manager import PluginFailure, PluginManager
from datasluice.runtime.session import DataSluiceSession

__all__ = ["DataSluiceSession", "PluginManager", "PluginFailure", "create_default_transport"]
