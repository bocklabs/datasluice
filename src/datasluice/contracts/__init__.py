"""Public conformance suite for catalog connectors (QUAL-01).

Built-in connectors run this suite in the DEFAULT CI pytest run (D-P5-13).
Third-party connector authors import and parametrize it against their own
fixtures — see :func:`run_contract_suite` for the full fixture-serving
contract.

Extension on-ramp (replaces the removed ``CustomAdapter``, D-P5-22): satisfy
the :mod:`datasluice.ports` capability Protocols, register a
``datasluice.connectors`` entry-point, drop fixtures under
``tests/fixtures/<yourportal>/``, and run the suite.

Example::

    from datasluice.contracts import run_contract_suite

    run_contract_suite(my_connector_factory, my_fixture_set, base_url=server_url, transport=my_transport)
"""

from datasluice.contracts.checks import run_contract_suite

__all__ = ["run_contract_suite"]
