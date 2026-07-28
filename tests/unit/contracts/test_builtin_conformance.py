"""Built-in connector conformance tests (QUAL-01/02/03, D-P5-13).

Runs the public :mod:`datasluice.contracts` suite parametrized over
``(connector_factory, fixture_set)`` pairs against hand-authored fixtures
served over real localhost sockets — no transport mocking, no network egress.
Runs in the DEFAULT pytest suite (no marker, no opt-in).

The module is resolved via ``importlib.import_module`` (rather than a static
``import``) so the RED commit can land under this repo's full-suite pre-commit
hook: until the implementation in the GREEN step ships, the whole module skips
cleanly instead of erroring at collection (Phase 3 convention).
"""

from __future__ import annotations

import importlib
from collections.abc import Callable

import pytest

from datasluice.connectors.base import BaseAdapter
from datasluice.connectors.ckan.factory import create_ckan_connector
from datasluice.runtime.context import ConnectorContext
from datasluice.transport import HttpClient

try:
    _contracts = importlib.import_module("datasluice.contracts")
except ImportError:
    pytest.skip(
        "datasluice.contracts package pending (RED → GREEN within task 05-04-1)",
        allow_module_level=True,
    )

run_contract_suite: Callable[..., None] = _contracts.run_contract_suite


@pytest.mark.parametrize(
    ("portal_name", "factory"),
    [
        pytest.param("ckan", create_ckan_connector, id="ckan"),
    ],
)
def test_builtin_connector_conforms(
    portal_name: str, factory: Callable[[ConnectorContext], BaseAdapter], request: pytest.FixtureRequest
) -> None:
    """Each built-in connector passes the shared conformance suite."""
    _server, base, fixture_set = request.getfixturevalue(f"{portal_name}_server")
    run_contract_suite(factory, fixture_set, base_url=base, transport=HttpClient())
