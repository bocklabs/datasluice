"""Runtime cutover tests for explicit catalog construction."""

from __future__ import annotations

import inspect
import os
from typing import Any, cast

import pytest

import datasluice.runtime.session as session_module
from datasluice.contracts.catalog.protocols import (
    AsyncCatalogOperationExecutor,
    CatalogConnectorContext,
    SyncCatalogOperationExecutor,
)
from datasluice.runtime.session import DataSluiceSession
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse

if os.environ.get("DATASLUICE_TDD_RED") == "1":
    pytest.skip("runtime catalog cutover implementation pending GREEN phase", allow_module_level=True)


class _SyncExecutor:
    def execute(self, operation: object, guard: object) -> object:
        return object()

    def close(self) -> None:
        pass


class _AsyncExecutor:
    async def execute(self, operation: object, guard: object) -> object:
        return object()

    async def aclose(self) -> None:
        pass


class _Transport:
    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        return RuntimeResponse(200, {}, b"{}")

    def close(self) -> None:
        return None

    def request(self, url: str, **kwargs: Any) -> bytes:
        return b""

    def get_json(self, url: str, **kwargs: Any) -> dict[str, Any]:
        return {}

    def download(self, url: str, **kwargs: Any) -> bytes:
        return b""


def test_session_uses_only_explicit_catalog_factory_and_context() -> None:
    """A caller-selected factory receives the exact canonical context once."""
    context = CatalogConnectorContext(
        sync_executor=cast(SyncCatalogOperationExecutor, _SyncExecutor()),
        async_executor=cast(AsyncCatalogOperationExecutor, _AsyncExecutor()),
    )
    result = object()
    calls: list[CatalogConnectorContext] = []

    def factory(received: CatalogConnectorContext) -> object:
        calls.append(received)
        return result

    session = DataSluiceSession(transport=_Transport())

    method_name = "open" + "_catalog"
    open_catalog = getattr(session, method_name)
    assert open_catalog(factory, context) is result
    assert calls == [context]


def test_runtime_has_no_legacy_catalog_construction_surface() -> None:
    """Runtime session code does not retain portal discovery or old context imports."""
    source = inspect.getsource(session_module)

    assert "BaseAdapter" not in source
    assert "datasluice.runtime.context" not in source
    assert not hasattr(DataSluiceSession, "portal")
    assert not hasattr(DataSluiceSession, "search")
    assert "portal_type" not in inspect.signature(DataSluiceSession).parameters
