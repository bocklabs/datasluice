"""Built-in connector conformance tests (QUAL-01/02/03, D-P5-13).

Runs the public :mod:`datasluice.contracts` suite parametrized over
``(connector_factory, fixture_set)`` pairs against hand-authored fixtures
served over real localhost sockets — no transport mocking, no network egress.
Runs in the DEFAULT pytest suite (no marker, no opt-in).
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from datasluice.connectors.base import BaseAdapter
from datasluice.connectors.ckan.factory import create_ckan_connector
from datasluice.connectors.datagouv.factory import create_datagouv_connector
from datasluice.connectors.socrata.factory import create_socrata_connector
from datasluice.contracts import run_contract_suite
from datasluice.runtime.context import ConnectorContext
from datasluice.transport import HttpClient


@pytest.mark.parametrize(
    ("portal_name", "factory"),
    [
        pytest.param("ckan", create_ckan_connector, id="ckan"),
        pytest.param("datagouv", create_datagouv_connector, id="datagouv"),
        pytest.param("socrata", create_socrata_connector, id="socrata"),
    ],
)
def test_builtin_connector_conforms(
    portal_name: str, factory: Callable[[ConnectorContext], BaseAdapter], request: pytest.FixtureRequest
) -> None:
    """Each built-in connector passes the shared conformance suite."""
    _server, base, fixture_set = request.getfixturevalue(f"{portal_name}_server")
    run_contract_suite(factory, fixture_set, base_url=base, transport=HttpClient())
