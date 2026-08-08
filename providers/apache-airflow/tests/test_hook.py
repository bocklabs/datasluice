"""Connection-backed DataSluiceHook contract tests."""

from __future__ import annotations

import base64
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import airflow.providers.datasluice.hooks.datasluice as hook_module
import pytest
from airflow.providers.datasluice.hooks.datasluice import DataSluiceHook

from datasluice import (
    Artifact,
    CatalogResourceLocator,
    Dataset,
    DataSluiceError,
    Resource,
    SearchResult,
)

_PORTAL_URL = "https://portal.example.test/catalog?api_key=connection-secret"
_SECRET = "connection-secret"


class _FakeFacade:
    instances: list[_FakeFacade] = []
    search_result = SearchResult()
    materialize_result: Any = None
    search_error: Exception | None = None
    materialize_error: Exception | None = None

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.closed = False
        self.search_calls: list[tuple[str, object, dict[str, object]]] = []
        self.materialize_calls: list[tuple[object, str, str]] = []
        type(self).instances.append(self)

    def __enter__(self) -> _FakeFacade:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self.closed = True

    def search(self, url: str, query: object = None, **kwargs: object) -> SearchResult:
        self.search_calls.append((url, query, kwargs))
        if type(self).search_error is not None:
            raise type(self).search_error
        return type(self).search_result

    def materialize(self, locator: object, destination_uri: str, *, mode: str = "parquet") -> Artifact:
        self.materialize_calls.append((locator, destination_uri, mode))
        if type(self).materialize_error is not None:
            raise type(self).materialize_error
        return type(self).materialize_result


@pytest.fixture
def fake_facade(monkeypatch: pytest.MonkeyPatch) -> type[_FakeFacade]:
    _FakeFacade.instances = []
    _FakeFacade.search_result = SearchResult()
    _FakeFacade.materialize_result = None
    _FakeFacade.search_error = None
    _FakeFacade.materialize_error = None
    monkeypatch.setattr(hook_module, "DataSluice", _FakeFacade)
    return _FakeFacade


def _connection(
    *,
    host: str | None = _PORTAL_URL,
    login: str | None = None,
    password: str | None = None,
    extra: dict[str, object] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        conn_id="datasluice_default",
        conn_type="datasluice",
        host=host,
        login=login,
        password=password,
        extra_dejson=extra or {},
    )


def _patch_connection(monkeypatch: pytest.MonkeyPatch, connection: object) -> list[str]:
    calls: list[str] = []

    def get_connection(self: DataSluiceHook, conn_id: str) -> object:
        calls.append(conn_id)
        if isinstance(connection, Exception):
            raise connection
        return connection

    monkeypatch.setattr(DataSluiceHook, "get_connection", get_connection)
    return calls


def _artifact() -> Artifact:
    digest = "0123456789abcdef" * 4
    inverse = "fedcba9876543210" * 4
    return Artifact.from_dict(
        {
            "schema_version": 1,
            "kind": "artifact",
            "uri": "memory://output/result.parquet",
            "media_type": "application/x-parquet",
            "size": 10,
            "content_digest": {"algorithm": "sha256", "value": digest},
            "blob_digest": {"algorithm": "sha256", "value": inverse},
            "provenance": {
                "source_locator": CatalogResourceLocator(
                    portal_url="https://portal.example.test",
                    dataset_id="dataset-1",
                    resource_id="resource-1",
                ).to_dict(),
                "resource_identity": digest,
                "created_at": "2026-08-01T00:00:00Z",
                "materialization_mode": "parquet",
                "transforms": [],
            },
            "metadata": {},
            "extensions": {},
        }
    )


def test_constructor_defaults_and_delays_connection_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _patch_connection(monkeypatch, _connection())

    hook = DataSluiceHook()
    explicit = DataSluiceHook(airflow_conn_id="custom_datasluice")

    assert hook.airflow_conn_id == "datasluice_default"
    assert explicit.airflow_conn_id == "custom_datasluice"
    assert calls == []
    assert not any(name in hook.__dict__ for name in ("connection", "conn", "_facade", "_data_sluice"))


def test_get_conn_maps_portal_options_and_basic_credentials(
    monkeypatch: pytest.MonkeyPatch, fake_facade: type[_FakeFacade]
) -> None:
    connection = _connection(
        login="portal-user",
        password=_SECRET,
        extra={
            "portal_url": "https://connection.example.test",
            "portal_type": "ckan",
            "auth_type": "basic",
            "timeout": 12.5,
            "retries": 4,
            "rate_limit": 2.0,
            "page_size": 25,
            "cache_dir": "/tmp/datasluice-cache",
            "cache_ttl": 90,
        },
    )
    calls = _patch_connection(monkeypatch, connection)

    facade = DataSluiceHook(airflow_conn_id="configured").get_conn()

    assert calls == ["configured"]
    assert facade.kwargs["timeout"] == 12.5
    assert facade.kwargs["retries"] == 4
    assert facade.kwargs["rate_limit"] == 2.0
    assert facade.kwargs["page_size"] == 25
    assert facade.kwargs["cache_dir"] == "/tmp/datasluice-cache"
    assert facade.kwargs["cache_ttl"] == 90
    auth = facade.kwargs["auth"]
    headers, params = auth.apply({}, {})
    assert headers["Authorization"] == f"Basic {base64.b64encode(f'portal-user:{_SECRET}'.encode()).decode()}"
    assert params == {}
    assert _SECRET not in repr(auth)
    facade.close()
    assert fake_facade.instances[0].closed


@pytest.mark.parametrize(
    ("extra", "login", "password", "header", "param", "expected"),
    [
        (
            {"auth_type": "api_key", "api_key_header": "X-Portal-Key", "api_key_in_query": True},
            None,
            _SECRET,
            "X-Portal-Key",
            "api_key",
            _SECRET,
        ),
        ({"auth_type": "bearer"}, None, _SECRET, "Authorization", None, f"Bearer {_SECRET}"),
        (
            {"auth_type": "basic"},
            "user",
            _SECRET,
            "Authorization",
            None,
            f"Basic {base64.b64encode(f'user:{_SECRET}'.encode()).decode()}",
        ),
    ],
)
def test_supported_connection_credentials(
    monkeypatch: pytest.MonkeyPatch,
    fake_facade: type[_FakeFacade],
    extra: dict[str, object],
    login: str | None,
    password: str | None,
    header: str,
    param: str | None,
    expected: str,
) -> None:
    calls = _patch_connection(monkeypatch, _connection(login=login, password=password, extra=extra))

    facade = DataSluiceHook().get_conn()
    auth = facade.kwargs["auth"]
    headers, params = auth.apply({}, {})

    assert calls == ["datasluice_default"]
    assert headers[header] == expected
    if param is not None:
        assert params[param] == _SECRET
    assert _SECRET not in repr(auth)
    facade.close()


def test_headers_credentials_are_mapped_from_connection_extra(
    monkeypatch: pytest.MonkeyPatch, fake_facade: type[_FakeFacade]
) -> None:
    _patch_connection(
        monkeypatch,
        _connection(
            extra={"auth_type": "headers", "headers": {"X-Portal-Key": _SECRET, "X-Region": "eu"}},
        ),
    )

    facade = DataSluiceHook().get_conn()
    headers, _ = facade.kwargs["auth"].apply({}, {})

    assert headers == {"X-Portal-Key": _SECRET, "X-Region": "eu"}
    assert _SECRET not in repr(facade.kwargs["auth"])
    facade.close()


def test_explicit_non_secret_overrides_win_and_secret_overrides_are_rejected(
    monkeypatch: pytest.MonkeyPatch, fake_facade: type[_FakeFacade]
) -> None:
    result = SearchResult(datasets=[])
    fake_facade.search_result = result
    _patch_connection(
        monkeypatch,
        _connection(
            extra={"portal_url": "https://connection.example.test", "timeout": 10, "page_size": 5},
        ),
    )
    hook = DataSluiceHook(portal_url="https://override.example.test", timeout=99, page_size=20)

    hook.search(query="query")

    facade = fake_facade.instances[0]
    assert facade.search_calls[0][0] == "https://override.example.test"
    assert facade.kwargs["timeout"] == 99
    assert facade.kwargs["page_size"] == 20
    with pytest.raises(ValueError, match="non-secret"):
        DataSluiceHook(api_key=_SECRET)


def test_search_returns_bounded_secret_free_catalog_locators(
    monkeypatch: pytest.MonkeyPatch, fake_facade: type[_FakeFacade]
) -> None:
    fake_facade.search_result = SearchResult(
        datasets=[
            Dataset(
                id="dataset-1",
                resources=[Resource(id="resource-1"), Resource(id="resource-2")],
            ),
            Dataset(id="dataset-2", resources=[Resource(id="resource-3")]),
        ]
    )
    _patch_connection(monkeypatch, _connection())

    result = DataSluiceHook().search(query="open data", max_results=2)

    assert result == [
        CatalogResourceLocator(
            portal_url=_PORTAL_URL,
            dataset_id="dataset-1",
            resource_id="resource-1",
        ).to_dict(),
        CatalogResourceLocator(
            portal_url=_PORTAL_URL,
            dataset_id="dataset-1",
            resource_id="resource-2",
        ).to_dict(),
    ]
    assert len(result) == 2
    assert _SECRET not in json.dumps(result)
    assert fake_facade.instances[0].closed


def test_materialize_accepts_one_locator_and_closes_facade(
    monkeypatch: pytest.MonkeyPatch, fake_facade: type[_FakeFacade]
) -> None:
    artifact = _artifact()
    fake_facade.materialize_result = artifact
    _patch_connection(monkeypatch, _connection())
    locator = CatalogResourceLocator(
        portal_url="https://portal.example.test",
        dataset_id="dataset-1",
        resource_id="resource-1",
    ).to_dict()

    result = DataSluiceHook().materialize(locator, "memory://output/result.parquet")

    assert result is artifact
    materialize_call = fake_facade.instances[0].materialize_calls[0]
    assert isinstance(materialize_call[0], CatalogResourceLocator)
    assert materialize_call[1:] == ("memory://output/result.parquet", "parquet")
    assert fake_facade.instances[0].closed


def test_materialize_rejects_multiple_locators_before_connection_lookup(
    monkeypatch: pytest.MonkeyPatch, fake_facade: type[_FakeFacade]
) -> None:
    calls = _patch_connection(monkeypatch, _connection())

    with pytest.raises(DataSluiceError):
        DataSluiceHook().materialize([], "memory://output/result.parquet")

    assert calls == []
    assert fake_facade.instances == []


def test_task_facades_close_and_redact_delegate_failures(
    monkeypatch: pytest.MonkeyPatch, fake_facade: type[_FakeFacade]
) -> None:
    _patch_connection(monkeypatch, _connection(password=_SECRET, extra={"auth_type": "bearer"}))
    fake_facade.search_error = RuntimeError(f"remote request included {_SECRET}")
    hook = DataSluiceHook()

    with pytest.raises(RuntimeError) as error:
        hook.search("https://portal.example.test", "query")

    assert _SECRET not in str(error.value)
    assert fake_facade.instances[0].closed


def test_missing_connection_does_not_expose_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _patch_connection(monkeypatch, RuntimeError("connection is missing"))

    with pytest.raises(RuntimeError, match="connection is missing"):
        DataSluiceHook(airflow_conn_id="missing").get_conn()

    assert calls == ["missing"]


def test_hook_imports_only_top_level_core_contracts() -> None:
    source = Path(hook_module.__file__).read_text(encoding="utf-8")

    assert "from datasluice import" in source
    assert "from datasluice." not in source
    assert "import datasluice" not in source
    assert "datasluice._" not in source
