"""Provider discovery and metadata contract tests for apache-airflow-providers-datasluice.

Runs inside the wheel-only candidate venv built by ``run_candidate.py`` so every
assertion reflects the installed-wheel experience Airflow will encounter. The
provider declares its runtime hook and operator without registering connections
or claiming platform actions that await Phases 3-5.
"""

from __future__ import annotations

import importlib.metadata as importlib_metadata
import sys
import tomllib
from pathlib import Path

import pytest
import yaml
from packaging.requirements import Requirement

_PROVIDER_PACKAGE = "apache-airflow-providers-datasluice"
_IMPORT_NS = "airflow.providers.datasluice"
_RUNTIME_DECLARATION_KEYS = ("operators", "hooks")
_FORBIDDEN_DECLARATION_KEYS = ("hook-class-names", "connection-types")
_EXECUTION_CLAIM_WORDS = ("discovery", "streaming", "materialization", "materialize", "search operator")
_DEPENDENCY_TABLE_TEXT = 'dependencies = [\n    "datasluice[http]>=0.2,<1",\n    "apache-airflow>=3.2,<4",\n]\n'
_DEPENDENCY_TABLE_LIST = ["datasluice[http]>=0.2,<1", "apache-airflow>=3.2,<4"]

_PROVIDER_PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"
_PROVIDER_PROJECT = tomllib.load(_PROVIDER_PYPROJECT.open("rb"))["project"]
_PROVIDER_VERSION = _PROVIDER_PROJECT["version"]


def _provider_info() -> dict[str, object]:
    from airflow.providers.datasluice.get_provider_info import get_provider_info

    return get_provider_info()


def _provider_yaml() -> dict[str, object]:
    import airflow.providers.datasluice as pkg

    yaml_path = Path(pkg.__file__).parent / "provider.yaml"
    return yaml.safe_load(yaml_path.read_text(encoding="utf-8"))


def _metadata_value(mapping: dict[str, object], key: str, source: str) -> object:
    """Return *key* from *mapping*, failing with a message naming the source and key."""
    if key not in mapping:
        pytest.fail(f"{source} metadata is missing required key {key!r}")
    return mapping[key]


def _descriptions() -> dict[str, object]:
    info = _provider_info()
    data = _provider_yaml()
    return {
        "get-provider-info": _metadata_value(info, "description", "get-provider-info"),
        "provider-yaml": _metadata_value(data, "description", "provider-yaml"),
        "provider-pyproject": _metadata_value(_PROVIDER_PROJECT, "description", "provider-pyproject"),
    }


def test_get_provider_info_returns_locked_identity() -> None:
    """get_provider_info returns the locked package identity and current version."""
    info = _provider_info()
    assert _metadata_value(info, "package-name", "get-provider-info") == _PROVIDER_PACKAGE
    name = _metadata_value(info, "name", "get-provider-info")
    assert isinstance(name, str) and name, f"get-provider-info 'name' must be non-empty text, got {name!r}"
    description = _metadata_value(info, "description", "get-provider-info")
    assert isinstance(description, str) and description, (
        f"get-provider-info 'description' must be non-empty text, got {description!r}"
    )
    assert _metadata_value(info, "versions", "get-provider-info") == [_PROVIDER_VERSION]


def test_yaml_carries_no_pinned_version_and_python_metadata_owns_it() -> None:
    """provider.yaml declares identity only; get_provider_info derives the version."""
    info = _provider_info()
    data = _provider_yaml()
    info_package_name = _metadata_value(info, "package-name", "get-provider-info")
    data_package_name = _metadata_value(data, "package-name", "provider-yaml")
    assert data_package_name == info_package_name == _PROVIDER_PACKAGE, (
        f"'package-name' drifted: get_provider_info={info_package_name!r} vs provider.yaml={data_package_name!r} "
        f"(expected {_PROVIDER_PACKAGE!r})"
    )
    info_name = _metadata_value(info, "name", "get-provider-info")
    data_name = _metadata_value(data, "name", "provider-yaml")
    assert data_name == info_name, f"'name' drifted: get_provider_info={info_name!r} vs provider.yaml={data_name!r}"
    assert "versions" not in data, "provider.yaml must not pin a 'versions' key"
    assert _metadata_value(info, "versions", "get-provider-info") == [_PROVIDER_VERSION]


def test_metadata_declares_runtime_hook_operator_but_no_connection_registration() -> None:
    """Both metadata sources declare runtime modules without connection registration."""
    info = _provider_info()
    data = _provider_yaml()
    for key in _RUNTIME_DECLARATION_KEYS:
        info_value = _metadata_value(info, key, "get-provider-info")
        data_value = _metadata_value(data, key, "provider-yaml")
        assert info_value == data_value, (
            f"{key!r} drifted between sources: get_provider_info={info_value!r} vs provider.yaml={data_value!r}"
        )
        assert info_value, f"get_provider_info declares an empty {key!r}; provider.yaml declares {data_value!r}"
    for key in _FORBIDDEN_DECLARATION_KEYS:
        assert key not in info, f"get_provider_info declares forbidden key {key!r}"
        assert key not in data, f"provider.yaml declares forbidden key {key!r}"


def test_descriptions_make_no_execution_claims() -> None:
    """No provider description claims removed discovery, streaming, or materialization execution."""
    for source, description in _descriptions().items():
        assert isinstance(description, str) and description, f"{source} description must be non-empty text"
        lowered = description.lower()
        for word in _EXECUTION_CLAIM_WORDS:
            assert word not in lowered, f"{source} description claims {word!r}"


def test_dependency_table_is_byte_for_byte_unchanged() -> None:
    """The provider dependency table keeps the exact runtime HTTP floor."""
    text = _PROVIDER_PYPROJECT.read_text(encoding="utf-8")
    assert _DEPENDENCY_TABLE_TEXT in text
    assert _PROVIDER_PROJECT["dependencies"] == _DEPENDENCY_TABLE_LIST


def test_wheel_exposes_exactly_one_provider_entry_point() -> None:
    """The built wheel registers exactly one apache_airflow_provider entry point."""
    eps = importlib_metadata.entry_points(group="apache_airflow_provider")
    datasluice_eps = [entry for entry in eps if entry.dist and entry.dist.name == _PROVIDER_PACKAGE]
    assert len(datasluice_eps) == 1
    assert datasluice_eps[0].name == "provider_info"
    assert datasluice_eps[0].value == f"{_IMPORT_NS}.get_provider_info:get_provider_info"


def test_provider_discovered_by_provider_manager() -> None:
    """Airflow ProviderManager discovers the installed provider entry point."""
    from airflow.providers_manager import ProvidersManager

    manager = ProvidersManager()
    providers = manager.providers
    assert _PROVIDER_PACKAGE in providers
    discovered = providers[_PROVIDER_PACKAGE]
    assert discovered.version == _PROVIDER_VERSION
    assert discovered.data["package-name"] == _PROVIDER_PACKAGE


def test_discovery_import_is_metadata_only() -> None:
    """Loading provider metadata performs no private-core or Connection side effect."""
    import airflow.providers.datasluice.get_provider_info as info_module

    source = Path(info_module.__file__).read_text(encoding="utf-8")
    assert "from datasluice" not in source
    assert "import datasluice" not in source

    pre_keys = {
        key for key in sys.modules if key == "_airflow_connection_db" or key.startswith("airflow.models.connection")
    }
    _provider_info()
    post_keys = {
        key for key in sys.modules if key == "_airflow_connection_db" or key.startswith("airflow.models.connection")
    }
    assert pre_keys == post_keys


def test_import_namespace_resolves() -> None:
    """The provider imports under the locked airflow.providers.datasluice namespace."""
    import airflow.providers.datasluice as pkg

    assert pkg.__name__ == _IMPORT_NS


def test_hook_injects_connection_credential_into_runtime_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """The hook composes a real CKAN live client from the connection credential and origin."""
    from airflow.providers.datasluice.hooks.datasluice import DatasluiceHook

    from datasluice.contracts.catalog.protocols import SyncCatalogClient
    from datasluice.domain.catalog.auth import CKANCredential

    class Connection:
        extra_dejson = {"platform": "ckan", "base_url": "http://127.0.0.1:9001", "api_token": "loopback-token"}

    monkeypatch.setattr(DatasluiceHook, "get_connection", lambda self, _: Connection())
    client = DatasluiceHook(airflow_conn_id="loopback").get_conn()

    assert isinstance(client, SyncCatalogClient)
    assert isinstance(client.credentials, CKANCredential)
    assert client.platform_metadata()["platform"] == "ckan"


def test_provider_distribution_metadata_matches_contract() -> None:
    """Installed distribution metadata preserves the exact identity and version."""
    distribution = importlib_metadata.distribution(_PROVIDER_PACKAGE)
    assert distribution.version == _PROVIDER_VERSION
    requires = {
        requirement.name.lower(): frozenset(str(specifier) for specifier in requirement.specifier)
        for requirement in (Requirement(line) for line in (distribution.requires or []))
    }
    declared = _PROVIDER_PROJECT["dependencies"]
    expected = {
        req.name.lower(): frozenset(str(spec) for spec in req.specifier)
        for req in (Requirement(line) for line in declared)
    }
    assert requires == expected
