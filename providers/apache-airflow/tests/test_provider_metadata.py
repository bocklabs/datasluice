"""Provider discovery and metadata contract tests for apache-airflow-providers-datasluice.

Runs inside the wheel-only candidate venv built by ``run_candidate.py`` so every
assertion reflects the installed-wheel experience Airflow will encounter.
"""

from __future__ import annotations

import importlib.metadata as importlib_metadata
import sys
from pathlib import Path

import yaml
from packaging.requirements import Requirement

_PROVIDER_PACKAGE = "apache-airflow-providers-datasluice"
_IMPORT_NS = "airflow.providers.datasluice"
_HOOK_CLASS = f"{_IMPORT_NS}.hooks.datasluice.DataSluiceHook"
_CONNECTION_TYPE = "datasluice"
_OPERATOR_MODULES = {
    f"{_IMPORT_NS}.operators.search",
    f"{_IMPORT_NS}.operators.materialize",
}


def _provider_info() -> dict[str, object]:
    from airflow.providers.datasluice.get_provider_info import get_provider_info

    return get_provider_info()


def _provider_yaml() -> dict[str, object]:
    import airflow.providers.datasluice as pkg

    yaml_path = Path(pkg.__file__).parent / "provider.yaml"
    return yaml.safe_load(yaml_path.read_text(encoding="utf-8"))


def test_get_provider_info_returns_locked_identity() -> None:
    """get_provider_info returns the locked D-21 package name and stable metadata."""
    info = _provider_info()
    assert info["package-name"] == _PROVIDER_PACKAGE
    assert isinstance(info["name"], str) and info["name"]
    assert isinstance(info["description"], str) and info["description"]
    versions = info.get("versions", [])
    assert isinstance(versions, list) and "0.1.0" in versions


def test_get_provider_info_declares_hook_and_connection_type() -> None:
    """Metadata declares the DataSluiceHook class and datasluice connection type."""
    info = _provider_info()
    hook_names = info.get("hook-class-names")
    assert isinstance(hook_names, list) and _HOOK_CLASS in hook_names
    conn_types = info.get("connection-types")
    assert isinstance(conn_types, list)
    matched = [
        entry for entry in conn_types if isinstance(entry, dict) and entry.get("connection-type") == _CONNECTION_TYPE
    ]
    assert len(matched) == 1
    assert matched[0].get("hook-class-name") == _HOOK_CLASS


def test_get_provider_info_declares_operator_modules() -> None:
    """Metadata declares both operator modules from the provider contract."""
    operators = _provider_info().get("operators", [])
    modules = {module for entry in operators for module in entry.get("python-modules", [])}
    assert modules == _OPERATOR_MODULES


def test_provider_yaml_exists_and_agrees_with_get_provider_info() -> None:
    """provider.yaml and get_provider_info agree on the locked identity and hook."""
    info = _provider_info()
    data = _provider_yaml()
    assert data["package-name"] == info["package-name"]
    assert data["name"] == info["name"]

    yaml_hook_modules = {module for entry in data.get("hooks", []) for module in entry.get("python-modules", [])}
    assert f"{_IMPORT_NS}.hooks.datasluice" in yaml_hook_modules

    yaml_operator_modules = {
        module for entry in data.get("operators", []) for module in entry.get("python-modules", [])
    }
    assert yaml_operator_modules == _OPERATOR_MODULES

    yaml_conn_types = data.get("connection-types", [])
    matched = [entry for entry in yaml_conn_types if entry.get("connection-type") == _CONNECTION_TYPE]
    assert len(matched) == 1 and matched[0]["hook-class-name"] == _HOOK_CLASS

    yaml_versions = data.get("versions", [])
    assert "0.1.0" in yaml_versions


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
    assert discovered.version == "0.1.0"
    assert discovered.data["package-name"] == _PROVIDER_PACKAGE


def test_discovery_import_is_metadata_only() -> None:
    """Loading provider metadata performs no private-core or Connection side effect (D-50)."""
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


def test_provider_distribution_metadata_matches_contract() -> None:
    """Installed distribution metadata preserves the exact D-21/D-29 identity and version."""
    distribution = importlib_metadata.distribution(_PROVIDER_PACKAGE)
    assert distribution.version == "0.1.0"
    requires = {
        requirement.name.lower(): frozenset(str(specifier) for specifier in requirement.specifier)
        for requirement in (Requirement(line) for line in (distribution.requires or []))
    }
    assert requires == {
        "datasluice": frozenset({">=1.0", "<2"}),
        "apache-airflow": frozenset({">=3.2", "<4"}),
    }
