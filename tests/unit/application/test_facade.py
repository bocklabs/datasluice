"""Application facade tests for direct data-plane work and explicit catalog composition.

The facade exposes direct resource access through caller-owned readers and
canonical catalog construction through an explicit
``Callable[[CatalogConnectorContext], T]`` factory handoff only.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import cast

import pytest

import datasluice.application as application_module
from datasluice.application import DataSluice, DirectResourceLocator, OpenedResource
from datasluice.contracts.catalog.protocols import (
    AsyncCatalogOperationExecutor,
    CatalogConnectorContext,
    SyncCatalogOperationExecutor,
)
from datasluice.exceptions import StreamClosedError
from datasluice.runtime.session import DataSluiceSession


class _Transport:
    """Structural transport double for constructing real sessions."""

    def request(self, url: str, **kwargs: object) -> bytes:
        return b""

    def get_json(self, url: str, **kwargs: object) -> dict[str, object]:
        return {}

    def download(self, url: str, **kwargs: object) -> bytes:
        return b""


class _SyncExecutor:
    """Structural sync executor double for canonical context construction."""

    def execute(self, operation: object, guard: object) -> object:
        return object()

    def close(self) -> None:
        return None


class _AsyncExecutor:
    """Structural async executor double for canonical context construction."""

    async def execute(self, operation: object, guard: object) -> object:
        return object()

    async def aclose(self) -> None:
        return None


def _catalog_context() -> CatalogConnectorContext:
    """Build one canonical context from structural executor doubles."""
    return CatalogConnectorContext(
        sync_executor=cast(SyncCatalogOperationExecutor, _SyncExecutor()),
        async_executor=cast(AsyncCatalogOperationExecutor, _AsyncExecutor()),
    )


class _MinimalSession:
    """Injected session double exposing only the canonical catalog handoff."""

    _transport = object()

    def open_catalog[T](self, factory: object, context: CatalogConnectorContext) -> T:
        return cast("T", cast("Callable[[CatalogConnectorContext], object]", factory)(context))


class _BatchStream:
    """Deterministic batch stream double closed exactly once by consumption."""

    def __init__(self, batches: list[bytes]) -> None:
        self._batches = batches
        self.closed = 0

    def iter_batches(self) -> Iterator[bytes]:
        """Return the deterministic batch iterator."""
        return iter(self._batches)

    def close(self) -> None:
        self.closed += 1


class _Reader:
    """Injected reader double recording every resolved resource it opens."""

    def __init__(self, batches: list[bytes]) -> None:
        self._batches = batches
        self.opened: list[object] = []

    def open(self, resource: object) -> _BatchStream:
        self.opened.append(resource)
        return _BatchStream(self._batches)


class _CallerOwnedConnector:
    """Connector double whose lifecycle stays with the caller."""

    def __init__(self) -> None:
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1


def test_facade_open_catalog_uses_only_explicit_factory_and_context() -> None:
    """The facade hands the caller-selected factory one exact canonical context."""
    context = _catalog_context()
    connector = _CallerOwnedConnector()
    calls: list[CatalogConnectorContext] = []

    def factory(received: CatalogConnectorContext) -> _CallerOwnedConnector:
        calls.append(received)
        return connector

    data_sluice = DataSluice(session=DataSluiceSession(transport=_Transport()))

    assert data_sluice.open_catalog(factory, context) is connector
    assert calls == [context]


def test_facade_close_never_closes_caller_owned_catalog_connectors() -> None:
    """Connectors built through open_catalog keep caller-owned lifecycles."""
    connector = _CallerOwnedConnector()
    data_sluice = DataSluice(session=DataSluiceSession(transport=_Transport()))

    data_sluice.open_catalog(lambda received: connector, _catalog_context())
    data_sluice.close()
    data_sluice.close()

    assert connector.close_calls == 0


def test_facade_rejects_portal_shaped_catalog_contexts() -> None:
    """A context double carrying portal identity is rejected before dispatch."""

    class _PortalShapedContext:
        portal_type = "ckan"
        base_url = "https://data.example.gov"

    data_sluice = DataSluice(session=DataSluiceSession(transport=_Transport()))
    portal_context = cast(CatalogConnectorContext, _PortalShapedContext())
    with pytest.raises(TypeError, match="CatalogConnectorContext"):
        data_sluice.open_catalog(lambda received: received, portal_context)


def test_facade_resolves_direct_locator_to_canonical_resource() -> None:
    """Direct data-plane resolution turns a locator into the canonical Resource."""
    data_sluice = DataSluice(session=_MinimalSession(), reader=_Reader([]))
    locator = DirectResourceLocator(uri="file:///data/example.csv", format="csv", media_type="text/csv")

    resource = data_sluice.resolve(locator)

    assert resource.url == "file:///data/example.csv"
    assert resource.format == "csv"
    assert resource.media_type == "text/csv"


def test_facade_opens_direct_locator_through_the_injected_reader() -> None:
    """Direct data-plane access streams through the caller-injected reader."""
    reader = _Reader([b"batch-one"])
    data_sluice = DataSluice(session=_MinimalSession(), reader=reader)

    with data_sluice.open(DirectResourceLocator(uri="file:///data/example.csv")) as opened:
        batches = list(opened)

    assert batches == [b"batch-one"]
    assert len(reader.opened) == 1
    assert reader.opened[0].url == "file:///data/example.csv"  # ty: ignore[unresolved-attribute]


def test_facade_open_returns_lazy_single_use_resource_wrapper() -> None:
    """The facade returns the lazy wrapper without eagerly opening a stream."""
    reader = _Reader([b"batch-one"])
    data_sluice = DataSluice(session=_MinimalSession(), reader=reader)

    opened = data_sluice.open(DirectResourceLocator(uri="file:///data/example.csv"))

    assert isinstance(opened, OpenedResource)
    assert reader.opened == []
    opened.close()


def test_closed_facade_rejects_catalog_and_data_plane_work() -> None:
    """A closed facade refuses both catalog composition and direct data-plane work."""
    data_sluice = DataSluice(session=_MinimalSession(), reader=_Reader([]))
    data_sluice.close()

    with pytest.raises(StreamClosedError):
        data_sluice.open_catalog(lambda received: received, _catalog_context())
    with pytest.raises(StreamClosedError):
        data_sluice.open(DirectResourceLocator(uri="file:///data/example.csv"))


def test_facade_exposes_no_portal_surface() -> None:
    """The application module and facade carry no portal, search, or discovery members."""
    assert not hasattr(DataSluice, "portal")
    assert not hasattr(DataSluice, "search")
    assert not hasattr(DataSluice, "detect")
    for retired_name in ("detect_portal", "search_datasets", "CatalogResourceLocator"):
        assert not hasattr(application_module, retired_name)


class _CloseSpy:
    """Close-counting double that optionally fails on close."""

    def __init__(self, error: BaseException | None = None) -> None:
        self._error = error
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1
        if self._error is not None:
            raise self._error


class _OwnedSession:
    """Session double whose data-plane dependencies are all closeable."""

    def __init__(self) -> None:
        self._transport = _CloseSpy(RuntimeError("transport close failed"))
        self._cache = _CloseSpy()
        self.storage = _CloseSpy()
        self.state_store = _CloseSpy()
        self.plugins = _CloseSpy()


def test_facade_closes_each_owned_dependency_once_and_preserves_first_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Facade-created dependencies are all closed, even when one close fails."""
    session = _OwnedSession()
    reader = _CloseSpy()
    monkeypatch.setattr(application_module, "DataSluiceSession", lambda **kwargs: session)
    monkeypatch.setattr(application_module, "DataPlaneResourceReader", lambda **kwargs: reader)
    data_sluice = application_module.DataSluice()

    with pytest.raises(RuntimeError, match="transport close failed"):
        data_sluice.close()
    data_sluice.close()

    assert reader.close_calls == 1
    assert session._transport.close_calls == 1
    assert session._cache.close_calls == 1
    assert session.storage.close_calls == 1
    assert session.state_store.close_calls == 1
    assert session.plugins.close_calls == 1


def test_facade_leaves_injected_dependencies_open(monkeypatch: pytest.MonkeyPatch) -> None:
    """Caller-provided session and reader dependencies remain borrowed."""
    session = _OwnedSession()
    reader = _CloseSpy()
    data_sluice = application_module.DataSluice(session=session, reader=reader)

    data_sluice.close()

    assert reader.close_calls == 0
    assert session._transport.close_calls == 0
    assert session._cache.close_calls == 0
    assert session.storage.close_calls == 0
    assert session.state_store.close_calls == 0
    assert session.plugins.close_calls == 0
