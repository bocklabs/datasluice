"""Contract tests for canonical built-in connector metadata."""

from __future__ import annotations

import tomllib
from pathlib import Path


def test_builtin_entry_points_target_only_canonical_factories() -> None:
    """Installed built-ins reserve namespaced IDs for canonical factories."""
    project = tomllib.loads((Path(__file__).resolve().parents[4] / "pyproject.toml").read_text(encoding="utf-8"))

    assert project["project"]["entry-points"]["datasluice.connectors"] == {
        "datasluice/ckan": "datasluice.connectors.catalog.ckan.factory:create_ckan_connector",
        "datasluice/udata": "datasluice.connectors.catalog.udata.factory:create_udata_connector",
        "datasluice/socrata": "datasluice.connectors.catalog.socrata.factory:create_socrata_connector",
    }
