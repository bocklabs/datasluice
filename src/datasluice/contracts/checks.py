"""Connector conformance checks (D-P5-14).

:func:`run_contract_suite` executes the 8-check matrix against a connector
instance built from *connector_factory* and pointed at a fixture-serving HTTP
server at *base_url* over *transport*:

1. The connector publishes a :class:`~datasluice.domain.CatalogCapabilities`
   ClassVar (D-P5-23).
2. ``isinstance(connector, SearchableCatalog)`` holds (D-08).
3. ``get_dataset`` returns a :class:`~datasluice.domain.Dataset` whose
   ``resources`` is a list (QUAL-02).
4. ``search`` returns a :class:`~datasluice.domain.SearchResult` page with
   ``datasets`` non-empty and ``total`` at least the page length (QUAL-02).
5. Dataset IDs are stable across repeated ``get_dataset`` calls (QUAL-02).
6. Pagination yields no duplicate dataset IDs across pages (QUAL-02).
7. An unsupported filter field raises
   :class:`~datasluice.exceptions.UnsupportedQueryFieldError` pre-flight
   (QUAL-02, ARCH-08). When the connector supports every ``Query`` filter
   field (e.g. CKAN), the check instead asserts the capabilities enumerate
   the full filter-field set.
8. Resources carry access descriptors — ``Resource.access is not None`` for
   at least one resource per searched dataset (QUAL-02, D-P5-02).

Checks 3-6 and 8 cover the QUAL-02 catalog-data half; checks 1, 2, and 7 cover
the capability-Protocol half. All checks run against hand-authored fixtures
served over real localhost sockets (D-P5-12) — no transport mocking.

Fixture-serving contract (third-party authors): the suite issues transport
calls in a FIXED order — ``get_dataset(dataset_id)`` once (check 3), then
``search(limit=10)`` (check 4), then ``get_dataset(dataset_id)`` twice more
(check 5), then ``search(limit=2, offset=0)`` and ``search(limit=2, offset=2)``
(check 6), and finally ``search(limit=10)`` once more (check 8). Check 7 raises
pre-flight and never touches the transport. The two check-6 pages MUST return
disjoint dataset IDs; a fixture set serving 4+ datasets supports two disjoint
2-item pages.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from datasluice.connectors.base import BaseAdapter
from datasluice.domain import CatalogCapabilities, Dataset, Query, SearchResult
from datasluice.exceptions import UnsupportedQueryFieldError
from datasluice.ports import SearchableCatalog, Transport
from datasluice.runtime.context import ConnectorContext

_QUERY_FILTER_FIELDS: tuple[str, ...] = ("text", "tags", "organizations", "groups", "res_format", "license_id", "sort")


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
        AssertionError: On the first failing conformance check. The message
            names the failing check and connector so parametrized pytest runs
            surface the exact invariant that broke.

    Third-party on-ramp (D-P5-11): satisfy the capability Protocols, register a
    ``datasluice.connectors`` entry-point, drop fixtures under
    ``tests/fixtures/<yourportal>/``, serve them over localhost sockets per
    the call-order contract in the module docstring, and call this function.
    The signature is a one-way-locked public contract.
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
    """Check 2: the connector satisfies the ``SearchableCatalog`` Protocol (D-08)."""
    assert isinstance(connector, SearchableCatalog), (
        f"{type(connector).__name__} must satisfy the SearchableCatalog Protocol (D-08 structural subtyping)"
    )


def _known_dataset_id(fixture_set: Mapping[str, Any]) -> str:
    """Extract the ``"dataset_id"`` entry the suite requires in *fixture_set*."""
    dataset_id = fixture_set.get("dataset_id")
    assert isinstance(dataset_id, str) and dataset_id, (
        "fixture_set must include a non-empty 'dataset_id' entry naming a dataset "
        "that get_dataset can fetch from the served fixtures"
    )
    return dataset_id


def _check_get_dataset_returns_dataset_with_resources(connector: BaseAdapter, fixture_set: Mapping[str, Any]) -> None:
    """Check 3: ``get_dataset`` returns a ``Dataset`` whose ``resources`` is a list."""
    dataset = connector.get_dataset(_known_dataset_id(fixture_set))
    assert isinstance(dataset, Dataset), (
        f"get_dataset must return a Dataset; {type(connector).__name__} returned {type(dataset).__name__}"
    )
    assert isinstance(dataset.resources, list), (
        f"get_dataset must populate Dataset.resources as a list (None forbidden); "
        f"{type(connector).__name__} returned {dataset.resources!r}"
    )


def _check_search_returns_page(connector: BaseAdapter) -> None:
    """Check 4 (QUAL-02): ``search`` returns a populated ``SearchResult`` page."""
    result = connector.search(Query(limit=10))
    assert isinstance(result, SearchResult), (
        f"search must return a SearchResult; {type(connector).__name__} returned {type(result).__name__}"
    )
    assert isinstance(result.datasets, list) and len(result.datasets) >= 1, (
        f"search must return at least one dataset against the served fixtures; {type(connector).__name__} returned "
        f"{len(result.datasets) if isinstance(result.datasets, list) else result.datasets!r}"
    )
    assert isinstance(result.total, int) and result.total >= len(result.datasets), (
        f"SearchResult.total must be an int >= len(datasets); {type(connector).__name__} returned {result.total!r}"
    )


def _check_dataset_ids_stable(connector: BaseAdapter, fixture_set: Mapping[str, Any]) -> None:
    """Check 5 (QUAL-02): repeated ``get_dataset`` calls return the requested ID."""
    dataset_id = _known_dataset_id(fixture_set)
    first = connector.get_dataset(dataset_id)
    second = connector.get_dataset(dataset_id)
    assert first.id == dataset_id, (
        f"get_dataset({dataset_id!r}) returned id {first.id!r} on first call "
        f"({type(connector).__name__}) — dataset IDs must be the portal-native ID, not regenerated"
    )
    assert second.id == dataset_id, (
        f"get_dataset({dataset_id!r}) returned id {second.id!r} on second call "
        f"({type(connector).__name__}) — dataset IDs must be stable across calls"
    )


def _check_pagination_no_duplicates(connector: BaseAdapter) -> None:
    """Check 6 (QUAL-02): two consecutive pages share no dataset IDs."""
    page_one = connector.search(Query(limit=2, offset=0))
    page_two = connector.search(Query(limit=2, offset=2))
    ids_one = {dataset.id for dataset in page_one.datasets}
    ids_two = {dataset.id for dataset in page_two.datasets}
    assert ids_one, f"search(offset=0) returned no datasets for {type(connector).__name__}"
    assert ids_two, (
        f"search(offset=2) returned no datasets for {type(connector).__name__} — "
        f"fixtures must serve at least 3 datasets for pagination to be meaningful"
    )
    duplicates = ids_one & ids_two
    assert not duplicates, (
        f"pagination must not repeat dataset IDs across pages; {type(connector).__name__} repeated {sorted(duplicates)}"
    )


def _probe_query(field: str) -> Query:
    """Build a ``Query`` that sets exactly one filter *field* to a probe value."""
    if field == "text":
        return Query(text="conformance-probe")
    if field == "tags":
        return Query(tags=["conformance-probe"])
    if field == "organizations":
        return Query(organizations=["conformance-probe"])
    if field == "groups":
        return Query(groups=["conformance-probe"])
    if field == "res_format":
        return Query(res_format="CSV")
    if field == "license_id":
        return Query(license_id="cc-by")
    return Query(sort="title_string asc")


def _check_unsupported_filter_reported(connector: BaseAdapter, fixture_set: Mapping[str, Any] | None = None) -> None:
    """Check 7 (QUAL-02, ARCH-08): unsupported filter fields raise pre-flight.

    The unsupported field is derived from the connector's own
    ``CatalogCapabilities`` — the first ``Query`` filter field absent from
    ``supported_query_fields``. Connectors supporting every filter field
    (e.g. CKAN) skip the raise-path and assert the full enumeration instead.
    """
    del fixture_set
    capabilities = getattr(type(connector), "capabilities", None)
    assert isinstance(capabilities, CatalogCapabilities), (
        f"{type(connector).__name__} must publish a `capabilities: ClassVar[CatalogCapabilities]`"
    )
    supported = set(capabilities.supported_query_fields)
    unsupported = [field for field in _QUERY_FILTER_FIELDS if field not in supported]
    if not unsupported:
        assert set(_QUERY_FILTER_FIELDS) <= supported, (
            f"{type(connector).__name__} rejects no Query filter field, so its capabilities must enumerate all "
            f"{len(_QUERY_FILTER_FIELDS)}; missing: {sorted(set(_QUERY_FILTER_FIELDS) - supported)}"
        )
        return
    field = unsupported[0]
    try:
        connector.search(_probe_query(field))
    except UnsupportedQueryFieldError as exc:
        assert exc.field == field, (
            f"{type(connector).__name__} must report the unsupported field {field!r}; "
            f"UnsupportedQueryFieldError reported {exc.field!r}"
        )
        return
    raise AssertionError(
        f"search with unsupported field {field!r} must raise UnsupportedQueryFieldError pre-flight "
        f"({type(connector).__name__}); no exception was raised"
    )


def _check_resources_have_access_descriptors(connector: BaseAdapter) -> None:
    """Check 8 (QUAL-02, D-P5-02): searched resources carry access descriptors."""
    result = connector.search(Query(limit=10))
    assert result.datasets, f"search returned no datasets for {type(connector).__name__}"
    for dataset in result.datasets:
        assert any(resource.access is not None for resource in dataset.resources), (
            f"dataset {dataset.id!r} from {type(connector).__name__} has no resource with an access descriptor — "
            f"mappers must populate Resource.access (HttpDownload / QueryAccess) per D-P5-02"
        )
