"""Public API stability tests for ``datasluice.contracts`` (D-P5-11 one-way lock).

``run_contract_suite`` is the third-party connector-author contract surface:
its import paths and signature are asserted here so any regression fails CI
immediately.

Uses the same ``importlib`` indirection as ``test_builtin_conformance.py`` so
the RED commit skips cleanly under the full-suite pre-commit hook.
"""

from __future__ import annotations

import importlib
import inspect

import pytest

try:
    _contracts = importlib.import_module("datasluice.contracts")
except ImportError:
    pytest.skip(
        "datasluice.contracts package pending (RED → GREEN within task 05-04-1)",
        allow_module_level=True,
    )

run_contract_suite = _contracts.run_contract_suite


def test_run_contract_suite_importable_from_package_root() -> None:
    """QUAL-01: the suite is importable from the public package root."""
    assert callable(run_contract_suite)
    assert run_contract_suite.__name__ == "run_contract_suite"


def test_run_contract_suite_reexported_from_checks_module() -> None:
    """D-P5-11: the package root re-exports the checks-module function."""
    checks_module = importlib.import_module("datasluice.contracts.checks")
    assert checks_module.run_contract_suite is run_contract_suite


def test_run_contract_suite_signature_is_stable() -> None:
    """D-P5-11 one-way lock: ``(connector_factory, fixture_set, *, base_url, transport)``."""
    params = inspect.signature(run_contract_suite).parameters
    assert list(params) == ["connector_factory", "fixture_set", "base_url", "transport"]
    assert params["connector_factory"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert params["fixture_set"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert params["base_url"].kind is inspect.Parameter.KEYWORD_ONLY
    assert params["transport"].kind is inspect.Parameter.KEYWORD_ONLY
