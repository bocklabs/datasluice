"""Connector conformance checks (D-P5-14).

:func:`run_contract_suite` executes the 8-check matrix against a connector
instance built from *connector_factory* and pointed at a fixture-serving HTTP
server at *base_url* over *transport*:

1. The connector publishes a :class:`~datasluice.domain.CatalogCapabilities`
   ClassVar (D-P5-23).
2. ``isinstance(connector, SearchableCatalog)`` holds (D-08).
3. ``get_dataset`` returns a :class:`~datasluice.domain.Dataset` whose
   ``resources`` is a list.
4. ``search`` returns a :class:`~datasluice.domain.SearchResult` page.
5. Dataset IDs are stable across repeated ``get_dataset`` calls.
6. Pagination yields no duplicate dataset IDs across pages.
7. An unsupported filter field raises
   :class:`~datasluice.exceptions.UnsupportedQueryFieldError` pre-flight.
8. Resources carry access descriptors (``Resource.access is not None``).

Checks 3-6 and 8 cover the QUAL-02 catalog-data half; checks 1, 2, and 7 cover
the capability-Protocol half. All checks run against hand-authored fixtures
served over real localhost sockets (D-P5-12) — no transport mocking.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from datasluice.connectors.base import BaseAdapter
from datasluice.domain import CatalogCapabilities
from datasluice.ports import Transport
from datasluice.runtime.context import ConnectorContext


def run_contract_suite(
    connector_factory: Callable[[ConnectorContext], BaseAdapter],
    fixture_set: Mapping[str, Any],
    *,
    base_url: str,
    transport: Transport,
) -> None:
    """Run all 8 conformance checks (D-P5-14) against a fixture-served connector.

    Args:
        connector_factory: A ``create_*_connector(ctx)`` callable, the same
            shape registered under the ``datasluice.connectors`` entry-point
            group.
        fixture_set: Parsed fixture payloads keyed by fixture name. MUST
            include a ``"dataset_id"`` entry naming a dataset that
            ``get_dataset`` can fetch from the served fixtures.
        base_url: Root URL of the fixture-serving HTTP server (the connector
            is built pointing at this URL).
        transport: A caller-provided :class:`~datasluice.ports.Transport`
            implementation (e.g. ``datasluice.transport.HttpClient()``).

    Raises:
        AssertionError: On the first failing conformance check.

    Third-party on-ramp (D-P5-11): satisfy the capability Protocols, register a
    ``datasluice.connectors`` entry-point, drop fixtures under
    ``tests/fixtures/<yourportal>/``, serve them over localhost sockets, and
    call this function. The signature is a one-way-locked public contract.
    """
    ctx = ConnectorContext(base_url=base_url, transport=transport, auth=None, page_size=10)
    connector = connector_factory(ctx)
    _check_publishes_catalog_capabilities(connector)
    _check_isinstance_searchable_catalog(connector)
    _check_get_dataset_returns_dataset_with_resources(connector, fixture_set)
    _check_search_returns_page(connector)
    _check_dataset_ids_stable(connector, fixture_set)
    _check_pagination_no_duplicates(connector)
    _check_unsupported_filter_reported(connector)
    _check_resources_have_access_descriptors(connector)


def _check_publishes_catalog_capabilities(connector: BaseAdapter) -> None:
    """Check 1: the connector publishes a ``CatalogCapabilities`` ClassVar (D-P5-23)."""
    capabilities = getattr(type(connector), "capabilities", None)
    assert isinstance(capabilities, CatalogCapabilities), (
        f"{type(connector).__name__} must publish a `capabilities: ClassVar[CatalogCapabilities]`; "
        f"found {capabilities!r}"
    )


def _check_isinstance_searchable_catalog(connector: BaseAdapter) -> None:
    """Check 2 (task 05-04-3 fills in)."""
    ...


def _check_get_dataset_returns_dataset_with_resources(connector: BaseAdapter, fixture_set: Mapping[str, Any]) -> None:
    """Check 3 (task 05-04-3 fills in)."""
    ...


def _check_search_returns_page(connector: BaseAdapter) -> None:
    """Check 4 (task 05-04-3 fills in)."""
    ...


def _check_dataset_ids_stable(connector: BaseAdapter, fixture_set: Mapping[str, Any]) -> None:
    """Check 5 (task 05-04-3 fills in)."""
    ...


def _check_pagination_no_duplicates(connector: BaseAdapter) -> None:
    """Check 6 (task 05-04-3 fills in)."""
    ...


def _check_unsupported_filter_reported(connector: BaseAdapter, fixture_set: Mapping[str, Any] | None = None) -> None:
    """Check 7 (task 05-04-3 fills in)."""
    ...


def _check_resources_have_access_descriptors(connector: BaseAdapter) -> None:
    """Check 8 (task 05-04-3 fills in)."""
    ...
