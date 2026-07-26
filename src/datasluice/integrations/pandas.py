"""Pandas integration (awaiting Phase 6 rebuild over the shared BatchStream).

The v0.1.0 ``resource_to_dataframe`` / ``dataset_to_dataframes`` helpers
have been removed per D-P4-18: they relied on the deleted
``datasluice.formats`` read path. Phase 6 rebuilds them over the shared
:class:`datasluice.data.BatchStream`, producing DataFrames via
zero-copy Arrow interop.

This module is intentionally kept as an importable placeholder so that
existing ``import datasluice.integrations.pandas`` calls do not break
import-time; calling the (removed) helpers raises a clear ``AttributeError``.
"""
