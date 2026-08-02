"""Unit tests for integration module imports (no heavy deps required)."""

from __future__ import annotations

import importlib

import pytest


@pytest.mark.parametrize("module_name", ["pandas", "dlt", "duckdb"])
def test_integration_modules_importable(module_name: str) -> None:
    """Integration modules should be importable without their optional deps."""
    module = importlib.import_module(f"datasluice.integrations.{module_name}")
    assert module is not None
