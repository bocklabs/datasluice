"""Contract tests for canonical built-in connector metadata."""

from __future__ import annotations

import importlib
import os
import tomllib
from pathlib import Path

from datasluice.discovery.fingerprints import HTML_FINGERPRINTS, PATH_FINGERPRINTS
from datasluice.runtime import plugin_manager

if os.environ.get("DATASLUICE_TDD_RED") == "1":
    import pytest

    pytest.skip("canonical connector metadata implementation pending GREEN phase", allow_module_level=True)


def test_builtin_entry_points_target_only_canonical_factories() -> None:
    """Installed built-ins reserve namespaced IDs for canonical factories."""
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))

    assert project["project"]["entry-points"]["datasluice.connectors"] == {
        "datasluice/ckan": "datasluice.connectors.catalog.ckan.factory:create_ckan_connector",
        "datasluice/udata": "datasluice.connectors.catalog.udata.factory:create_udata_connector",
        "datasluice/socrata": "datasluice.connectors.catalog.socrata.factory:create_socrata_connector",
    }
    assert getattr(plugin_manager, "_BUILTIN_CONNECTOR_IDS", None) == frozenset(
        project["project"]["entry-points"]["datasluice.connectors"]
    )


def test_connector_namespaces_are_import_light_and_non_reexporting() -> None:
    """Root and catalog namespaces do not publish platform APIs."""
    root = importlib.import_module("datasluice.connectors")
    catalog = importlib.import_module("datasluice.connectors.catalog")

    assert getattr(root, "__all__", None) == []
    assert getattr(catalog, "__all__", None) == []


def test_fingerprints_resolve_only_canonical_platform_ids() -> None:
    """Discovery metadata identifies uData without retaining data.gouv IDs."""
    assert set(PATH_FINGERPRINTS.values()) == {"ckan", "udata", "socrata"}
    assert set(HTML_FINGERPRINTS.values()) == {"ckan", "udata", "socrata"}
