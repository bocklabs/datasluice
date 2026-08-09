"""Public API stability tests for ``datasluice.contracts``.

``run_contract_suite`` is the third-party connector-author contract surface:
its import paths and signature are asserted here so any regression fails CI
immediately.
"""

from __future__ import annotations

import inspect

from datasluice.contracts import run_contract_suite


def test_run_contract_suite_importable_from_package_root() -> None:
    """The suite is importable from the public package root."""
    assert callable(run_contract_suite)
    assert run_contract_suite.__name__ == "run_contract_suite"


def test_run_contract_suite_reexported_from_checks_module() -> None:
    """The package root re-exports the checks-module function."""
    from datasluice.contracts.checks import run_contract_suite as from_checks

    assert from_checks is run_contract_suite


def test_run_contract_suite_signature_is_stable() -> None:
    """one-way lock: ``(connector_factory, fixture_set, *, base_url, transport)``."""
    params = inspect.signature(run_contract_suite).parameters
    assert list(params) == ["connector_factory", "fixture_set", "base_url", "transport"]
    assert params["connector_factory"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert params["fixture_set"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert params["base_url"].kind is inspect.Parameter.KEYWORD_ONLY
    assert params["transport"].kind is inspect.Parameter.KEYWORD_ONLY
