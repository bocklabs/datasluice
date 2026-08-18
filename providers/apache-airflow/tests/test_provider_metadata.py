"""Provider discovery and metadata contract tests for apache-airflow-providers-datasluice.

Runs inside the wheel-only candidate venv built by ``run_candidate.py`` so every
assertion reflects the installed-wheel experience Airflow will encounter. After
the Phase 1 clean break the provider is metadata-only: it declares no hook,
operator, or connection registration.
"""

from __future__ import annotations

import importlib.metadata as importlib_metadata
import sys
import tomllib
from pathlib import Path

import yaml
from packaging.requirements import Requirement

_PROVIDER_PACKAGE = "apache-airflow-providers-datasluice"
_IMPORT_NS = "airflow.providers.datasluice"
_RUNTIME_DECLARATION_KEYS = ("operators", "hooks", "hook-class-names", "connection-types")
_EXECUTION_CLAIM_WORDS = ("discovery", "streaming", "materialization", "materialize", "search operator")
_DEPENDENCY_TABLE_TEXT = 'dependencies = [\n    "datasluice>=0.2,<1",\n    "apache-airflow>=3.2,<4",\n]\n'
_DEPENDENCY_TABLE_LIST = ["datasluice>=0.2,<1", "apache-airflow>=3.2,<4"]

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


def _descriptions() -> dict[str, str]:
    info = _provider_info()
    data = _provider_yaml()
    return {
        "get-provider-info": str(info["description"]),
        "provider-yaml": str(data["description"]),
        "provider-pyproject": str(_PROVIDER_PROJECT["description"]),
    }


def test_get_provider_info_returns_locked_identity() -> None:
    """get_provider_info returns the locked package identity and current version."""
    info = _provider_info()
    assert info["package-name"] == _PROVIDER_PACKAGE
    assert isinstance(info["name"], str) and info["name"]
    assert isinstance(info["description"], str) and info["description"]
    assert info["versions"] == [_PROVIDER_VERSION]


def test_yaml_carries_no_pinned_version_and_python_metadata_owns_it() -> None:
    """provider.yaml declares identity only; get_provider_info derives the version."""
    info = _provider_info()
    data = _provider_yaml()
    assert data["package-name"] == info["package-name"] == _PROVIDER_PACKAGE
    assert data["name"] == info["name"]
    assert "versions" not in data
    assert info["versions"] == [_PROVIDER_VERSION]


def test_metadata_declares_no_hook_operator_or_connection() -> None:
    """Neither metadata source declares a hook, operator, or connection registration."""
    info = _provider_info()
    data = _provider_yaml()
    for key in _RUNTIME_DECLARATION_KEYS:
        assert key not in info, f"get_provider_info still declares {key!r}"
        assert key not in data, f"provider.yaml still declares {key!r}"


def test_descriptions_make_no_execution_claims() -> None:
    """No provider description claims removed discovery, streaming, or materialization execution."""
    for source, description in _descriptions().items():
        lowered = description.lower()
        for word in _EXECUTION_CLAIM_WORDS:
            assert word not in lowered, f"{source} description claims {word!r}"


def test_dependency_table_is_byte_for_byte_unchanged() -> None:
    """The provider dependency table keeps the exact Phase 1 lines with no connector extras."""
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
