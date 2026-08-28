"""Repository-wide clean-break, coverage-linkage, and compliance audit.

Phase 1 final gate: the whole tracked repository—not any single plan—must
satisfy the locked connector contract. This module proves four things:

1. The canonical platform packages, classes, factories, and namespaced
   entry points are exactly the declared public surface, and neither the
   package root nor the catalog contract suite re-exports platform APIs.
2. Every removed module, symbol, identifier, portal type, runner, runtime
   method, CLI command, fixture tree, documentation page, navigation
   entry, and Airflow provider hook/operator/DAG is absent from all
   tracked production, test, documentation, and provider surfaces except
   the explicit negative-audit allowlist below.
3. Every INTEGRATE row of the official API coverage matrix links uniquely
   to a pinned profile operation, typed native sync and async Protocols,
   deterministic fixture cases, runner report outcomes, and reference
   certification; only the two locked OPT-OUT rows are absent.
4. Public models, errors, lifecycle, auth, safety, extension, and
   certification invariants hold: typed frozen values, import-light
   packages, redacted secret-safe outputs, no raw HTTP surface, no
   implicit activation, no runtime installation, no mutable extension
   bag, no global write flag, no sync-thread async wrapper, and no alias,
   shim, or deprecation wrapper.

Untracked working-tree files (build artifacts, ``graphify-out`` knowledge
graphs, ignored provider ``dist/`` wheels) are outside this audit: only
tracked files shape the installed, documented, or CI-tested surface.

Historical identifiers stay legal only in this module and in the explicit
per-file allowlist, which contains solely negative guards that assert the
same absence this gate enforces.
"""

from __future__ import annotations

import ast
import dataclasses
import importlib
import importlib.metadata
import inspect
import json
import logging
import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SRC_ROOT = REPO_ROOT / "src"
PYPROJECT = REPO_ROOT / "pyproject.toml"
PROFILES_DIR = SRC_ROOT / "datasluice" / "contracts" / "catalog" / "profiles"
COVERAGE_MD = REPO_ROOT / ".planning/phases/01-target-connector-contract/COVERAGE.md"
PROVIDER_ROOT = REPO_ROOT / "providers" / "apache-airflow"
ZENSICAL = REPO_ROOT / "zensical.toml"

PLATFORMS = ("ckan", "udata", "socrata")

CANONICAL_PLATFORM_EXPORTS: dict[str, tuple[str, str]] = {
    "ckan": ("CKANConnector", "create_ckan_connector"),
    "udata": ("UDataConnector", "create_udata_connector"),
    "socrata": ("SocrataConnector", "create_socrata_connector"),
}

CANONICAL_LIVE_CLIENT_EXPORTS: dict[str, tuple[str, ...]] = {
    "ckan": (
        "CKANClientSettings",
        "CKANConnector",
        "create_async_client",
        "create_ckan_connector",
        "create_sync_client",
    ),
    "udata": (
        "UDataClientSettings",
        "UDataConnector",
        "create_async_client",
        "create_sync_client",
        "create_udata_connector",
    ),
}

CONNECTOR_MODULE_PATHS: dict[str, str] = {
    "ckan": "src/datasluice/connectors/catalog/ckan/connector.py",
    "udata": "src/datasluice/connectors/catalog/udata/connector.py",
    "socrata": "src/datasluice/connectors/catalog/socrata/connector.py",
}

EXPECTED_ENTRY_POINTS: dict[str, str] = {
    "datasluice/ckan": "datasluice.connectors.catalog.ckan.factory:create_ckan_connector",
    "datasluice/udata": "datasluice.connectors.catalog.udata.factory:create_udata_connector",
    "datasluice/socrata": "datasluice.connectors.catalog.socrata.factory:create_socrata_connector",
}

REMOVED_MODULES: tuple[str, ...] = (
    "datasluice.adapters",
    "datasluice.connectors.ckan",
    "datasluice.connectors.datagouv",
    "datasluice.connectors.socrata",
    "datasluice.domain.capabilities",
    "datasluice.contracts.checks",
    "datasluice.contracts.fixtures",
    "datasluice.runtime.context",
    "datasluice.cli.search",
    "datasluice.cli.inspect",
    "datasluice.cli.detect",
    "datasluice.cli.download",
)

_RUNTIME_MODULE_PREFIX = "datasluice."

REMOVED_RUNTIME_MODULES: tuple[str, ...] = (
    _RUNTIME_MODULE_PREFIX + "auth",
    _RUNTIME_MODULE_PREFIX + "credentials",
    _RUNTIME_MODULE_PREFIX + "ports.credentials",
    _RUNTIME_MODULE_PREFIX + "ports.transport",
    _RUNTIME_MODULE_PREFIX + "transport",
)

BANNED_IDENTIFIERS: tuple[str, ...] = (
    *REMOVED_MODULES,
    "AdapterError",
    "AdapterNotFoundError",
    "AdapterRegistry",
    "BaseAdapter",
    "CKANAdapter",
    "CatalogResourceLocator",
    "CustomAdapter",
    "DataGouvAdapter",
    "create_datagouv_connector",
    "DataSluiceHook",
    "DataSluiceOperator",
    "DataSluiceSearchOperator",
    "DataSluiceMaterializeOperator",
    "PortalType",
    "SODA2Adapter",
    "SocrataAdapter",
    "UDataAdapter",
    "datagouv",
    "registry.register",
)

NEGATIVE_AUDIT_ALLOWLIST: dict[str, frozenset[str]] = {
    "tests/quality/test_catalog_surface.py": frozenset(BANNED_IDENTIFIERS),
    "tests/quality/test_docs_guard.py": frozenset(
        {
            "AdapterError",
            "AdapterNotFoundError",
            "BaseAdapter",
            "CKANAdapter",
            "CatalogResourceLocator",
            "DataGouvAdapter",
            "create_datagouv_connector",
            "DataSluiceHook",
            "DataSluiceOperator",
            "DataSluiceSearchOperator",
            "DataSluiceMaterializeOperator",
            "SocrataAdapter",
            "UDataAdapter",
            "datagouv",
            "datasluice.adapters",
            "datasluice.connectors.ckan",
            "datasluice.connectors.datagouv",
            "datasluice.connectors.socrata",
            "registry.register",
        }
    ),
    "tests/quality/test_release_routing.py": frozenset({"DataSluiceHook"}),
    "tests/unit/domain/test_purity.py": frozenset({"datasluice.adapters"}),
    "tests/unit/runtime/test_catalog_cutover.py": frozenset({"BaseAdapter", "datasluice.runtime.context"}),
    "tests/unit/application/test_catalog_cutover.py": frozenset({"CatalogResourceLocator"}),
    "tests/unit/application/test_facade.py": frozenset({"CatalogResourceLocator"}),
    "tests/unit/contracts/catalog/test_public_api.py": frozenset(
        {"AdapterError", "AdapterNotFoundError", "CatalogResourceLocator"}
    ),
    "tests/unit/test_package.py": frozenset({"AdapterNotFoundError", "CatalogResourceLocator"}),
    "tests/unit/connectors/catalog/test_udata_public.py": frozenset(
        {"DataGouvAdapter", "create_datagouv_connector", "datagouv"}
    ),
    "tests/unit/connectors/catalog/test_socrata_public.py": frozenset({"SODA2Adapter"}),
    "tests/unit/discovery/test_detection_evidence.py": frozenset({"datagouv"}),
    "tests/unit/discovery/test_discovery.py": frozenset({"datagouv"}),
    "tests/unit/runtime/test_plugin_manager.py": frozenset({"datagouv"}),
    "tests/unit/runtime/test_no_global_state.py": frozenset({"AdapterRegistry", "registry.register"}),
    "tests/unit/test_former_facade_names_removed.py": frozenset(
        {"CKANAdapter", "CustomAdapter", "SocrataAdapter", "UDataAdapter"}
    ),
    "providers/apache-airflow/tests/test_public_boundary.py": frozenset(
        {
            "DataSluiceHook",
            "DataSluiceSearchOperator",
            "DataSluiceMaterializeOperator",
            "hooks.datasluice",
            "operators.search",
            "operators.materialize",
            "operators._xcom",
            "example_datasluice",
        }
    ),
    ".github/workflows/docs.yaml": frozenset({"DataSluiceOperator", "DataSluiceSession"}),
}

PROVIDER_RETIRED_TOKENS: tuple[str, ...] = (
    "DataSluiceHook",
    "DataSluiceSearchOperator",
    "DataSluiceMaterializeOperator",
    "operators.search",
    "operators.materialize",
    "operators._xcom",
    "example_datasluice",
)

PROVIDER_RUNTIME_SOURCE = PROVIDER_ROOT / "src" / "airflow" / "providers" / "datasluice"

LOCKED_OPT_OUT_ROWS: frozenset[str] = frozenset(
    {
        "ckan.legacy-api",
        "socrata.soda-v2-legacy-resource-endpoints",
    }
)

LOCKED_INTEGRATE_ROWS: tuple[str, ...] = (
    "ckan.action-api-v3.discovery-help-and-status",
    "ckan.action-api-v3.dataset-list-show-search",
    "ckan.action-api-v3.dataset-create-update-patch-delete-purge",
    "ckan.action-api-v3.dataset-collaborators",
    "ckan.action-api-v3.resource-list-show-create-update-patch-delete-upload",
    "ckan.action-api-v3.organization-list-show-search",
    "ckan.action-api-v3.organization-create-update-delete-members",
    "ckan.action-api-v3.group-list-show-search",
    "ckan.action-api-v3.group-create-update-delete-members",
    "ckan.action-api-v3.user-list-show",
    "ckan.action-api-v3.user-create-update-delete-token-management",
    "ckan.action-api-v3.tags-vocabularies-licenses-list-show",
    "ckan.action-api-v3.tags-vocabularies-licenses-create-update-delete",
    "ckan.action-api-v3.relationships-follows",
    "ckan.action-api-v3.activity",
    "ckan.action-api-v3.resource-views",
    "ckan.datastore-extension.query-and-record-crud",
    "ckan.datastore-extension.sql-search",
    "ckan.filestore.upload-and-resource-file-replacement",
    "ckan.action-api-v3.jobs-and-task-status",
    "ckan.action-api-v3.config-options",
    "ckan.plugin-provided-action-and-extension-probes",
    "udata.api-v1.root-and-effective-profile-probe",
    "udata.api-v1.dataset-list-search-show-create-update-delete",
    "udata.api-v1.dataset-resource-create-update-reorder-upload-delete",
    "udata.api-v1.organizations-and-memberships",
    "udata.api-v1.users-me-and-api-token-management",
    "udata.api-v1.authentication-and-oauth-flows",
    "udata.api-v1.taxonomies-licenses-frequencies-formats-badges-and-schemas",
    "udata.api-v1.followers-activities-discussions-and-reuses",
    "udata.api-v1.topics-territories-contact-points-and-dataservices",
    "udata.api-v1.harvest-moderation-and-admin-operations",
    "udata.deployment-plugin-and-configuration-dependent-routes",
    "socrata.soda-v3-query",
    "socrata.soda-v3-export",
    "socrata.soda-v3-row-create-update-upsert-delete",
    "socrata.soda-v3-soql-query-types-and-format-negotiation",
    "socrata.catalog-discovery-and-view-metadata",
    "socrata.asset-dataset-metadata-and-permission-management",
    "socrata.user-current-identity-and-permission-probe",
    "socrata.application-token-basic-auth-and-oauth",
    "socrata.async-request-status-rate-limit-and-request-id",
)

NATIVE_OPERATION_MEMBERS: dict[str, dict[str, tuple[str, str, str]]] = {
    "ckan": {
        "ckan/action-api-v3.discovery-help-and-status": (
            "SyncCKANActionDiscoveryService",
            "AsyncCKANActionDiscoveryService",
            "discovery_help_and_status",
        ),
        "ckan/action-api-v3.dataset-list-show-search": (
            "SyncCKANDatasetService",
            "AsyncCKANDatasetService",
            "list_show_search",
        ),
        "ckan/action-api-v3.dataset-create-update-patch-delete-purge": (
            "SyncCKANDatasetService",
            "AsyncCKANDatasetService",
            "create_update_patch_delete_purge",
        ),
        "ckan/action-api-v3.dataset-collaborators": (
            "SyncCKANDatasetService",
            "AsyncCKANDatasetService",
            "create_update_patch_delete_purge",
        ),
        "ckan/action-api-v3.resource-list-show-create-update-patch-delete-upload": (
            "SyncCKANResourceService",
            "AsyncCKANResourceService",
            "list_show_create_update_patch_delete_upload",
        ),
        "ckan/action-api-v3.organization-list-show-search": (
            "SyncCKANOrganizationService",
            "AsyncCKANOrganizationService",
            "list_show_create_update_delete_members",
        ),
        "ckan/action-api-v3.organization-create-update-delete-members": (
            "SyncCKANOrganizationService",
            "AsyncCKANOrganizationService",
            "list_show_create_update_delete_members",
        ),
        "ckan/action-api-v3.group-list-show-search": (
            "SyncCKANGroupService",
            "AsyncCKANGroupService",
            "list_show_create_update_delete_members",
        ),
        "ckan/action-api-v3.group-create-update-delete-members": (
            "SyncCKANGroupService",
            "AsyncCKANGroupService",
            "list_show_create_update_delete_members",
        ),
        "ckan/action-api-v3.user-list-show": (
            "SyncCKANUserService",
            "AsyncCKANUserService",
            "list_show_create_update_delete_token_management",
        ),
        "ckan/action-api-v3.user-create-update-delete-token-management": (
            "SyncCKANUserService",
            "AsyncCKANUserService",
            "list_show_create_update_delete_token_management",
        ),
        "ckan/action-api-v3.tags-vocabularies-licenses-list-show": (
            "SyncCKANVocabularyLicenseService",
            "AsyncCKANVocabularyLicenseService",
            "tags_vocabularies_and_licenses",
        ),
        "ckan/action-api-v3.tags-vocabularies-licenses-create-update-delete": (
            "SyncCKANVocabularyLicenseService",
            "AsyncCKANVocabularyLicenseService",
            "tags_vocabularies_and_licenses",
        ),
        "ckan/action-api-v3.relationships-follows": (
            "SyncCKANRelationshipActivityService",
            "AsyncCKANRelationshipActivityService",
            "relationships_followers_and_activity",
        ),
        "ckan/action-api-v3.activity": (
            "SyncCKANRelationshipActivityService",
            "AsyncCKANRelationshipActivityService",
            "relationships_followers_and_activity",
        ),
        "ckan/action-api-v3.resource-views": ("SyncCKANViewService", "AsyncCKANViewService", "resource_views"),
        "ckan/datastore-extension.query-and-record-crud": (
            "SyncCKANDatastoreService",
            "AsyncCKANDatastoreService",
            "query_and_record_crud",
        ),
        "ckan/datastore-extension.sql-search": (
            "SyncCKANDatastoreService",
            "AsyncCKANDatastoreService",
            "query_and_record_crud",
        ),
        "ckan/filestore.upload-and-resource-file-replacement": (
            "SyncCKANFilestoreService",
            "AsyncCKANFilestoreService",
            "upload_and_resource_file_replacement",
        ),
        "ckan/action-api-v3.jobs-and-task-status": (
            "SyncCKANExtensionService",
            "AsyncCKANExtensionService",
            "extension_probes",
        ),
        "ckan/action-api-v3.config-options": (
            "SyncCKANExtensionService",
            "AsyncCKANExtensionService",
            "extension_probes",
        ),
        "ckan/plugin-provided-action-and-extension-probes": (
            "SyncCKANExtensionService",
            "AsyncCKANExtensionService",
            "extension_probes",
        ),
    },
    "udata": {
        "udata/api-v1.root-and-effective-profile-probe": (
            "SyncUDataServices",
            "AsyncUDataServices",
            "root_profile",
        ),
        "udata/api-v1.dataset-list-search-show-create-update-delete": (
            "SyncUDataServices",
            "AsyncUDataServices",
            "datasets",
        ),
        "udata/api-v1.dataset-resource-create-update-reorder-upload-delete": (
            "SyncUDataServices",
            "AsyncUDataServices",
            "resources",
        ),
        "udata/api-v1.organizations-and-memberships": (
            "SyncUDataServices",
            "AsyncUDataServices",
            "organizations_memberships",
        ),
        "udata/api-v1.users-me-and-api-token-management": (
            "SyncUDataServices",
            "AsyncUDataServices",
            "users_tokens",
        ),
        "udata/api-v1.authentication-and-oauth-flows": ("SyncUDataServices", "AsyncUDataServices", "auth_oauth"),
        "udata/api-v1.taxonomies-licenses-frequencies-formats-badges-and-schemas": (
            "SyncUDataServices",
            "AsyncUDataServices",
            "taxonomies",
        ),
        "udata/api-v1.followers-activities-discussions-and-reuses": (
            "SyncUDataServices",
            "AsyncUDataServices",
            "social",
        ),
        "udata/api-v1.topics-territories-contact-points-and-dataservices": (
            "SyncUDataServices",
            "AsyncUDataServices",
            "geography",
        ),
        "udata/api-v1.harvest-moderation-and-admin-operations": (
            "SyncUDataServices",
            "AsyncUDataServices",
            "harvest_moderation_admin",
        ),
        "udata/deployment-plugin-and-configuration-dependent-routes": (
            "SyncUDataServices",
            "AsyncUDataServices",
            "extensions",
        ),
    },
    "socrata": {
        "socrata/soda-v3-query": ("SyncSocrataServices", "AsyncSocrataServices", "soda"),
        "socrata/soda-v3-export": ("SyncSocrataServices", "AsyncSocrataServices", "soda"),
        "socrata/soda-v3-row-create-update-upsert-delete": ("SyncSocrataServices", "AsyncSocrataServices", "soda"),
        "socrata/soda-v3-soql-query-types-and-format-negotiation": (
            "SyncSocrataServices",
            "AsyncSocrataServices",
            "soda",
        ),
        "socrata/catalog-discovery-and-view-metadata": ("SyncSocrataServices", "AsyncSocrataServices", "catalog"),
        "socrata/asset-dataset-metadata-and-permission-management": (
            "SyncSocrataServices",
            "AsyncSocrataServices",
            "assets_permissions",
        ),
        "socrata/user-current-identity-and-permission-probe": (
            "SyncSocrataServices",
            "AsyncSocrataServices",
            "identity_permissions",
        ),
        "socrata/application-token-basic-auth-and-oauth": (
            "SyncSocrataServices",
            "AsyncSocrataServices",
            "auth",
        ),
        "socrata/async-request-status-rate-limit-and-request-id": (
            "SyncSocrataServices",
            "AsyncSocrataServices",
            "async_status",
        ),
    },
}

PUBLIC_CATALOG_TREES: tuple[Path, ...] = (
    SRC_ROOT / "datasluice" / "contracts" / "catalog",
    SRC_ROOT / "datasluice" / "connectors" / "catalog",
    SRC_ROOT / "datasluice" / "domain" / "catalog",
)

_SCAN_SUFFIXES = frozenset({".py", ".md", ".toml", ".json", ".yaml", ".yml", ".cfg"})
_COVERAGE_ROW_RE = re.compile(r"^\|\s*(\w+)\.([\w.-]+)\s*\|\s*(INTEGRATE|OPT-OUT)\s*\|", re.MULTILINE)

RETIRED_WORD_RE = re.compile(r"adapters?", re.IGNORECASE)
RETIRED_WORD_SCAN_ROOTS: tuple[str, ...] = (
    "src/datasluice/connectors/",
    "docs/",
    ".github/workflows/",
)
RETIRED_WORD_SCAN_TOPS: tuple[str, ...] = ("README.md", "zensical.toml", "AGENTS.md", "CONTEXT.md")
RETIRED_WORD_ALLOWLIST: dict[str, frozenset[str]] = {
    "CONTEXT.md": frozenset({"adapter"}),
}


def _tracked_files() -> list[str]:
    result = subprocess.run(["git", "ls-files"], cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        pytest.fail(
            f"git ls-files failed; repository audit requires a git checkout: {result.stderr.strip()}", pytrace=False
        )
    return result.stdout.splitlines()


def _audit_corpus(tracked: list[str]) -> list[Path]:
    roots = ("src/", "tests/", "docs/", "providers/", ".github/")
    tops = {"README.md", "zensical.toml", "pyproject.toml", "Makefile"}
    return [REPO_ROOT / name for name in tracked if name.startswith(roots) or name in tops]


def _pyproject() -> dict[str, Any]:
    return tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))


def _profile(platform: str) -> dict[str, Any]:
    matches = [
        path
        for path in PROFILES_DIR.glob("*.json")
        if json.loads(path.read_text(encoding="utf-8"))["platform"] == platform
    ]
    assert len(matches) == 1, f"platform {platform!r} must have exactly one pinned profile"
    return json.loads(matches[0].read_text(encoding="utf-8"))


def _coverage_rows() -> list[tuple[str, str, str]]:
    rows = _COVERAGE_ROW_RE.findall(COVERAGE_MD.read_text(encoding="utf-8")) if COVERAGE_MD.exists() else []
    return [(platform, name, decision) for platform, name, decision in rows]


def _row_to_operation_id(row: str) -> str:
    platform, _, name = row.partition(".")
    return f"{platform}/{name}"


LOCKED_DATASET_ROUTE_OPERATIONS = frozenset(
    {
        "udata/api-v1.list-datasets",
        "udata/api-v1.create-dataset",
        "udata/api-v1.recent-datasets-atom",
        "udata/api-v1.get-dataset",
        "udata/api-v1.update-dataset",
        "udata/api-v1.delete-dataset",
        "udata/api-v1.feature-dataset",
        "udata/api-v1.unfeature-dataset",
        "udata/api-v1.rdf-dataset",
        "udata/api-v1.rdf-dataset-format",
        "udata/api-v1.suggest-datasets",
        "udata/api-v2.search-datasets",
        "udata/api-v2.list-datasets",
        "udata/api-v2.get-dataset",
        "udata/api-v2.get-dataset-extras",
        "udata/api-v2.update-dataset-extras",
        "udata/api-v2.delete-dataset-extras",
    }
)


def _retired_word_violations() -> list[str]:
    """Scan connector-facing surfaces for any casing of the retired word.

    Per-file granularity: a file listed in ``RETIRED_WORD_ALLOWLIST`` permits
    only its enumerated spellings; every other match in connector trees,
    docs, workflows, and top-level surfaces fails the gate.
    """
    corpus = [
        REPO_ROOT / name
        for name in _tracked_files()
        if name.startswith(RETIRED_WORD_SCAN_ROOTS) or name in RETIRED_WORD_SCAN_TOPS
    ]
    violations: list[str] = []
    for path in corpus:
        if not path.exists() or path.suffix not in _SCAN_SUFFIXES:
            continue
        relative = path.relative_to(REPO_ROOT).as_posix()
        allowed = RETIRED_WORD_ALLOWLIST.get(relative, frozenset())
        text = path.read_text(encoding="utf-8", errors="replace")
        for match in RETIRED_WORD_RE.finditer(text):
            if match.group(0).lower() not in allowed:
                violations.append(f"{relative}: retired word {match.group(0)!r}")
                break
    return violations


def _connector_module_violations(tracked: list[str]) -> list[str]:
    """Prove the renamed connector modules are tracked canonical surface."""
    tracked_set = set(tracked)
    violations: list[str] = []
    for platform, module_path in CONNECTOR_MODULE_PATHS.items():
        if module_path not in tracked_set:
            violations.append(f"{platform} connector module missing from tracking: {module_path}")
        former = module_path.replace("connector.py", "adapter.py")
        if former in tracked_set:
            violations.append(f"{platform} former facade module still tracked: {former}")
    return violations


def _identifier_violations() -> list[str]:
    """Scan every tracked auditable surface for removed identifiers."""
    corpus = [path for path in _audit_corpus(_tracked_files()) if path.exists() and path.suffix in _SCAN_SUFFIXES]
    violations: list[str] = []
    for path in corpus:
        relative = path.relative_to(REPO_ROOT).as_posix()
        allowed = NEGATIVE_AUDIT_ALLOWLIST.get(relative, frozenset())
        text = path.read_text(encoding="utf-8", errors="replace")
        for identifier in BANNED_IDENTIFIERS:
            if identifier not in allowed and identifier in text:
                violations.append(f"{relative}: removed identifier {identifier!r}")
    return violations


def _removed_module_violations() -> list[str]:
    """Report removed module paths that are still importable."""
    import importlib.util

    violations = []
    for removed_module in (*REMOVED_MODULES, *REMOVED_RUNTIME_MODULES):
        if importlib.util.find_spec(removed_module) is not None:
            violations.append(removed_module)
    return violations


def _entry_point_violations() -> list[str]:
    """Compare declared and installed entry points with the canonical factories."""
    violations = []
    try:
        declared = dict(_pyproject()["project"]["entry-points"]["datasluice.connectors"])
    except (KeyError, TypeError, tomllib.TOMLDecodeError):
        return ["pyproject entry points are unreadable"]
    if declared != EXPECTED_ENTRY_POINTS:
        violations.append(f"pyproject entry points diverge: {sorted(declared)}")
    installed = {
        entry_point.name: entry_point.value
        for entry_point in importlib.metadata.entry_points(group="datasluice.connectors")
    }
    if installed != EXPECTED_ENTRY_POINTS:
        violations.append(f"installed entry points diverge: {sorted(installed)}")
    return violations


def _fixture_linkage_violations() -> list[str]:
    """Check profile operations, the locked matrix, and case corpora agree statically."""
    violations = []
    integrate_ids = {_row_to_operation_id(row) for row in LOCKED_INTEGRATE_ROWS}
    for platform in PLATFORMS:
        try:
            profile = _profile(platform)
        except (AssertionError, OSError, json.JSONDecodeError) as error:
            violations.append(f"{platform} profile unreadable: {error}")
            continue
        operations = {operation["id"] for operation in profile["operations"]}
        expected = {operation_id for operation_id in integrate_ids if operation_id.startswith(f"{platform}/")}
        approved_routes = LOCKED_DATASET_ROUTE_OPERATIONS if platform == "udata" else frozenset()
        if operations != expected | approved_routes:
            violations.append(f"{platform} profile diverges from the locked matrix")
        cases_path = REPO_ROOT / "src/datasluice/contracts/catalog/fixtures" / platform / "cases.json"
        try:
            covered = {case["operation"] for case in json.loads(cases_path.read_text(encoding="utf-8"))["cases"]}
        except (OSError, json.JSONDecodeError, KeyError):
            violations.append(f"{platform} fixture cases unreadable")
            continue
        if not expected <= covered:
            violations.append(f"{platform} fixture cases miss: {sorted(expected - covered)}")
    return violations


def _static_audit_gaps() -> list[str]:
    """Aggregate the fast static invariants gating this module's execution."""
    tracked = _tracked_files()
    return (
        _removed_module_violations()
        + _entry_point_violations()
        + _identifier_violations()
        + _fixture_linkage_violations()
        + _retired_word_violations()
        + _connector_module_violations(tracked)
    )


_STATIC_GAPS = _static_audit_gaps()
if _STATIC_GAPS:
    pytest.fail(f"catalog surface invariants unmet: {'; '.join(_STATIC_GAPS[:6])}", pytrace=False)


def _certificate_parts(platform: str):
    from datetime import date
    from typing import cast

    from datasluice.contracts.catalog import certify_catalog_report, load_reference_fixture_set
    from datasluice.contracts.catalog.fakes import AsyncReferenceConnector, SyncReferenceConnector
    from datasluice.contracts.catalog.protocols import AsyncCatalogClient, SyncCatalogClient
    from datasluice.contracts.catalog.runner import catalog_contract_cases, run_catalog_contract
    from datasluice.domain.catalog.extensions import (
        ActivationPolicy,
        CertificationRecord,
        ConnectorId,
        ConnectorManifest,
        OptionalInstallRequirement,
    )
    from datasluice.domain.catalog.operations import (
        Atomicity,
        AuthClass,
        CapabilityClass,
        ConcurrencyRequirement,
        Idempotency,
        MutationClass,
        OperationId,
        OperationSpec,
        OperationTier,
    )
    from datasluice.domain.catalog.profiles import DeclaredCapabilityProfile

    fixture_set = load_reference_fixture_set(platform)
    cases = catalog_contract_cases(fixture_set)
    report = run_catalog_contract(
        cases,
        sync_client=cast(SyncCatalogClient, SyncReferenceConnector(fixture_set)),
        async_client=cast(AsyncCatalogClient, AsyncReferenceConnector(fixture_set)),
        fixture_set=fixture_set,
    )
    operation_id = OperationId(platform=platform, service="catalog", method="get")
    profile = DeclaredCapabilityProfile(
        profile_version=fixture_set.profile_version,
        schema_version="1",
        platform_api_version="fixture",
        official_source_uri="https://example.test/catalog",
        source_accessed_at=date(2026, 8, 15),
        fixture_fingerprint=fixture_set.fingerprint,
        operations={
            operation_id: OperationSpec(
                id=operation_id,
                tier=OperationTier.NORMALIZED,
                request_type="CatalogRequest",
                response_type="CatalogResponse",
                auth_class=AuthClass.PUBLIC,
                mutation_class=MutationClass.READ,
                idempotency=Idempotency.SAFE,
                concurrency=ConcurrencyRequirement.NONE,
                atomicity=Atomicity.SINGLE_RESOURCE,
                capability_class=CapabilityClass.CORE,
            )
        },
    )
    return (
        certify_catalog_report,
        fixture_set,
        cases,
        report,
        profile,
        (
            ActivationPolicy,
            CertificationRecord,
            ConnectorId,
            ConnectorManifest,
            OptionalInstallRequirement,
        ),
    )


def test_canonical_platform_packages_export_exactly_the_typed_surface() -> None:
    """Test 1: each platform package exports only its canonical published surface."""
    for platform, (connector_name, factory_name) in CANONICAL_PLATFORM_EXPORTS.items():
        module = importlib.import_module(f"datasluice.connectors.catalog.{platform}")
        expected = CANONICAL_LIVE_CLIENT_EXPORTS.get(platform, (connector_name, factory_name))
        assert sorted(module.__all__) == sorted(expected)
        connector = getattr(module, connector_name)
        factory = getattr(module, factory_name)
        assert inspect.isclass(connector) and connector.__module__.startswith(
            f"datasluice.connectors.catalog.{platform}."
        )
        assert inspect.isfunction(factory) and factory.__module__.startswith(
            f"datasluice.connectors.catalog.{platform}."
        )
        for extra in set(expected) - {connector_name, factory_name}:
            assert getattr(module, extra).__module__.startswith(f"datasluice.connectors.catalog.{platform}.")


def test_namespaced_entry_points_declare_and_install_the_canonical_factories() -> None:
    """Test 1: pyproject and installed distribution metadata target the same canonical factories."""
    assert not _entry_point_violations()
    project = _pyproject()["project"]
    entry_points = project["entry-points"]["datasluice.connectors"]
    assert isinstance(entry_points, dict)
    assert dict(entry_points) == EXPECTED_ENTRY_POINTS
    installed = {
        entry_point.name: entry_point.value
        for entry_point in importlib.metadata.entry_points(group="datasluice.connectors")
    }
    assert installed == EXPECTED_ENTRY_POINTS
    scripts = project["scripts"]
    assert isinstance(scripts, dict) and scripts == {"datasluice": "datasluice.cli.app:app"}
    for name, target in EXPECTED_ENTRY_POINTS.items():
        module_name, _, attribute = target.partition(":")
        resolved = getattr(importlib.import_module(module_name), attribute)
        platform = name.partition("/")[-1]
        platform_module = importlib.import_module(f"datasluice.connectors.catalog.{platform}")
        assert resolved is getattr(platform_module, f"create_{platform}_connector")


@pytest.mark.parametrize("package", ["datasluice", "datasluice.connectors", "datasluice.connectors.catalog"])
def test_catalog_root_packages_do_not_re_export_platform_apis(package: str) -> None:
    """Test 1: the package root and catalog roots stay free of platform symbols."""
    module = importlib.import_module(package)
    for _, (connector_name, factory_name) in CANONICAL_PLATFORM_EXPORTS.items():
        assert not hasattr(module, connector_name), f"{package} re-exports {connector_name}"
        assert not hasattr(module, factory_name), f"{package} re-exports {factory_name}"
    contracts = importlib.import_module("datasluice.contracts.catalog")
    errors = importlib.import_module("datasluice.errors.catalog")
    domain = importlib.import_module("datasluice.domain.catalog")
    for public_module in (contracts, errors, domain):
        for _, (connector_name, factory_name) in CANONICAL_PLATFORM_EXPORTS.items():
            assert not hasattr(public_module, connector_name)
            assert not hasattr(public_module, factory_name)


@pytest.mark.parametrize("removed_module", (*REMOVED_MODULES, *REMOVED_RUNTIME_MODULES))
def test_removed_modules_fail_to_import(removed_module: str) -> None:
    """Test 2: every legacy module path is gone, not forwarded or aliased."""
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(removed_module)


def test_removed_identifiers_are_absent_from_all_tracked_surfaces() -> None:
    """Test 2: the clean break holds across production, tests, docs, and workflows."""
    assert not _identifier_violations()


def test_removed_facade_methods_and_url_construction_are_gone() -> None:
    """Test 2: the application facade exposes no portal search/read runtime."""
    from datasluice.application import DataSluice

    removed_methods = {"search", "get_dataset", "download", "download_all", "read"}
    assert removed_methods.isdisjoint(vars(DataSluice))
    parameters = set(inspect.signature(DataSluice.__init__).parameters)
    assert parameters == {"self", "session", "reader", "session_kwargs"}
    assert {"url", "portal", "portal_type"}.isdisjoint(parameters)


def test_cli_registers_only_retained_direct_resource_commands() -> None:
    """Test 2: removed CLI commands are neither registered nor importable."""
    from datasluice.cli.app import app

    names = {info.name for info in app.registered_commands}
    assert names == {"scan", "open", "materialize"}
    for removed in ("search", "inspect", "detect", "download"):
        assert removed not in names
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module(f"datasluice.cli.{removed}")


def test_removed_fixture_trees_and_example_page_stay_deleted() -> None:
    """Test 2: unversioned fixture trees and the former example page remain gone."""
    for removed_fixture in (
        "tests/fixtures/ckan",
        "tests/fixtures/datagouv",
        "tests/fixtures/socrata",
        "tests/fixtures/catalog",
    ):
        assert not (REPO_ROOT / removed_fixture).exists(), f"{removed_fixture} must stay deleted"
    assert not (REPO_ROOT / "docs/examples/datagouv.md").exists()
    navigation = ZENSICAL.read_text(encoding="utf-8")
    assert "datagouv" not in navigation and "data.gouv" not in navigation
    for platform in PLATFORMS:
        for fixture_file in ("cases.json", "evidence.json"):
            assert (REPO_ROOT / "src/datasluice/contracts/catalog/fixtures" / platform / fixture_file).is_file()


def test_airflow_provider_tree_preserves_only_the_new_runtime_surface() -> None:
    """Test 2: only the new hook/operator surface survives without legacy paths or DAGs."""
    tracked = [name for name in _tracked_files() if name.startswith("providers/")]
    assert tracked, "provider tree must be tracked"
    violations: list[str] = []
    for name in tracked:
        path = REPO_ROOT / name
        relative = path.relative_to(REPO_ROOT).as_posix()
        if path.suffix not in _SCAN_SUFFIXES or not path.exists():
            continue
        allowed = NEGATIVE_AUDIT_ALLOWLIST.get(relative, frozenset())
        text = path.read_text(encoding="utf-8", errors="replace")
        for token in PROVIDER_RETIRED_TOKENS:
            if token not in allowed and token in text:
                violations.append(f"{relative}: retired provider token {token!r}")
    assert not violations, "provider violations: " + "; ".join(violations)
    for retired_source in (
        PROVIDER_RUNTIME_SOURCE / "operators" / "search.py",
        PROVIDER_RUNTIME_SOURCE / "operators" / "materialize.py",
        PROVIDER_RUNTIME_SOURCE / "operators" / "_xcom.py",
    ):
        assert not retired_source.exists()
    provider_dags = PROVIDER_ROOT / "tests" / "dags"
    assert not provider_dags.exists() or not any(provider_dags.iterdir()), "sample DAG files must stay deleted"
    hook_source = PROVIDER_RUNTIME_SOURCE / "hooks" / "datasluice.py"
    assert hook_source.is_file(), "provider hook must be present"
    hook_tree = ast.parse(hook_source.read_text(encoding="utf-8"))
    assert any(node.name == "DatasluiceHook" for node in hook_tree.body if isinstance(node, ast.ClassDef))
    operator_source = PROVIDER_RUNTIME_SOURCE / "operators" / "__init__.py"
    operator_tree = ast.parse(operator_source.read_text(encoding="utf-8"))
    assert any(
        node.name == "DatasluiceCatalogOperator" for node in operator_tree.body if isinstance(node, ast.ClassDef)
    )


def test_provider_metadata_and_dependency_table_stay_locked() -> None:
    """Test 2: provider metadata declares runtime modules and preserves the HTTP floor."""
    import yaml

    provider_yaml = yaml.safe_load((PROVIDER_RUNTIME_SOURCE / "provider.yaml").read_text(encoding="utf-8"))
    assert set(provider_yaml) <= {
        "package-name",
        "name",
        "description",
        "versions",
        "integrations",
        "hooks",
        "operators",
    }
    for required_key in ("hooks", "operators"):
        assert provider_yaml[required_key]
    for banned_key in ("connections", "sensors"):
        assert banned_key not in provider_yaml
    info_source = (PROVIDER_RUNTIME_SOURCE / "get_provider_info.py").read_text(encoding="utf-8")
    for required_key in ("hooks", "operators"):
        assert f'"{required_key}"' in info_source
    for banned_key in ("connections",):
        assert f'"{banned_key}"' not in info_source
    provider_pyproject = tomllib.loads((PROVIDER_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert provider_pyproject["project"]["dependencies"] == ["datasluice[http]>=0.2,<1", "apache-airflow>=3.2,<4"]
    compatibility = json.loads((PROVIDER_ROOT / "compatibility.json").read_text(encoding="utf-8"))
    assert set(compatibility) == {"airflow", "python"}
    runtime_sources = list((PROVIDER_RUNTIME_SOURCE).rglob("*.py"))
    assert runtime_sources, "provider runtime source must exist"
    allowed_modules = {
        "datasluice.application",
        "datasluice.connectors.catalog.ckan",
        "datasluice.contracts.catalog.protocols",
        "datasluice.domain.catalog.auth",
        "datasluice.domain.catalog.ids",
        "datasluice.domain.catalog.operations",
        "datasluice.domain.catalog.profiles",
        "datasluice.runtime.clients",
        "datasluice.runtime.credentials",
        "datasluice.runtime.transport.base",
    }
    for source in runtime_sources:
        tree = ast.parse(source.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert all(
                    not alias.name.startswith("datasluice") or alias.name in allowed_modules for alias in node.names
                ), f"{source} imports private core"
            if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("datasluice"):
                assert node.module in allowed_modules, f"{source} imports private core package {node.module}"


def test_locked_coverage_matrix_matches_profiles_exactly() -> None:
    """Test 3: every INTEGRATE row is one pinned profile operation and vice versa."""
    if COVERAGE_MD.exists():
        rows = _coverage_rows()
        file_integrate = {f"{platform}.{name}" for platform, name, decision in rows if decision == "INTEGRATE"}
        file_opt_out = {f"{platform}.{name}" for platform, name, decision in rows if decision == "OPT-OUT"}
        assert file_integrate == set(LOCKED_INTEGRATE_ROWS)
        assert file_opt_out == set(LOCKED_OPT_OUT_ROWS)
    integrate_ids = {_row_to_operation_id(row) for row in LOCKED_INTEGRATE_ROWS}
    for platform in PLATFORMS:
        profile = _profile(platform)
        operations = [operation["id"] for operation in profile["operations"]]
        assert len(operations) == len(set(operations)), f"{platform} declares duplicate operations"
        platform_ids = {operation_id for operation_id in integrate_ids if operation_id.startswith(f"{platform}/")}
        approved_routes = LOCKED_DATASET_ROUTE_OPERATIONS if platform == "udata" else frozenset()
        assert set(operations) == platform_ids | approved_routes, f"{platform} profile diverges from the locked matrix"
        locked_outs = {row for row in LOCKED_OPT_OUT_ROWS if row.startswith(f"{platform}.")}
        opt_out_ids = {entry["id"] for entry in profile.get("opt_out_operations", [])}
        for row in locked_outs:
            operation_id = _row_to_operation_id(row)
            assert operation_id not in operations
            assert operation_id in opt_out_ids or f"{operation_id}.unsupported" in opt_out_ids
        extra_outs = (
            opt_out_ids
            - {_row_to_operation_id(row) for row in locked_outs}
            - {f"{_row_to_operation_id(row)}.unsupported" for row in locked_outs}
        )
        assert not extra_outs, f"{platform} declares unlocked opt-outs: {extra_outs}"


def test_every_integrate_operation_has_both_native_protocol_modes() -> None:
    """Test 3: each operation maps uniquely onto typed sync and async native Protocols."""
    integrate_ids = {_row_to_operation_id(row) for row in LOCKED_INTEGRATE_ROWS}
    mapped_ids = {operation_id for platform in NATIVE_OPERATION_MEMBERS.values() for operation_id in platform}
    assert mapped_ids == integrate_ids
    for platform, operations in NATIVE_OPERATION_MEMBERS.items():
        native = importlib.import_module(f"datasluice.contracts.catalog.native.{platform}")
        for operation_id, (sync_name, async_name, member) in operations.items():
            sync_protocol = getattr(native, sync_name)
            async_protocol = getattr(native, async_name)
            assert member in dir(sync_protocol), f"{operation_id} missing on {sync_name}"
            assert member in dir(async_protocol), f"{operation_id} missing on {async_name}"


def test_every_integrate_operation_reaches_report_outcomes_in_both_modes() -> None:
    """Test 3: the public runner yields passing sync and async evidence per operation."""
    from typing import cast

    from datasluice.contracts.catalog import catalog_contract_cases, load_reference_fixture_set, run_catalog_contract
    from datasluice.contracts.catalog.fakes import AsyncReferenceConnector, SyncReferenceConnector
    from datasluice.contracts.catalog.protocols import AsyncCatalogClient, SyncCatalogClient

    integrate_ids = {_row_to_operation_id(row) for row in LOCKED_INTEGRATE_ROWS}
    for platform in PLATFORMS:
        fixture_set = load_reference_fixture_set(platform)
        assert {str(case.operation_id) for case in fixture_set.cases} == {
            operation_id for operation_id in integrate_ids if operation_id.startswith(f"{platform}/")
        }, f"{platform} fixture cases diverge from the locked matrix"
        cases = catalog_contract_cases(fixture_set)
        report = run_catalog_contract(
            cases,
            sync_client=cast(SyncCatalogClient, SyncReferenceConnector(fixture_set)),
            async_client=cast(AsyncCatalogClient, AsyncReferenceConnector(fixture_set)),
            fixture_set=fixture_set,
        )
        assert report.is_compliant, f"{platform} report gaps: {report.gaps}"
        assert tuple(report.expected_case_ids) == tuple(sorted(case.pytest_id for case in cases))
        assert {outcome.case_id for outcome in report.outcomes} == set(report.expected_case_ids)
        modes_by_operation: dict[str, set[str]] = {}
        for outcome in report.outcomes:
            modes_by_operation.setdefault(outcome.operation_id, set()).add(outcome.mode)
        for operation_id, modes in modes_by_operation.items():
            assert modes == {"sync", "async"}, f"{operation_id} lacks both modes: {modes}"
        assert set(modes_by_operation) == {
            operation_id for operation_id in integrate_ids if operation_id.startswith(f"{platform}/")
        }


def test_locked_opt_outs_stay_absent_from_every_executable_api() -> None:
    """Test 3: only the two locked OPT-OUT rows are excluded from executable surfaces."""
    from datasluice.contracts.catalog import load_reference_fixture_set

    opt_out_ids = {_row_to_operation_id(row) for row in LOCKED_OPT_OUT_ROWS}
    for platform in PLATFORMS:
        profile = _profile(platform)
        operations = {operation["id"] for operation in profile["operations"]}
        assert operations.isdisjoint(opt_out_ids)
        fixture_set = load_reference_fixture_set(platform)
        assert {str(case.operation_id) for case in fixture_set.cases}.isdisjoint(opt_out_ids)
    for platform_operations in NATIVE_OPERATION_MEMBERS.values():
        assert set(platform_operations).isdisjoint(opt_out_ids)
    for row in LOCKED_OPT_OUT_ROWS:
        assert row.split(".", 1)[0] in PLATFORMS


@pytest.mark.parametrize("platform", PLATFORMS)
def test_full_reference_certification_for_builtin_connectors(platform: str) -> None:
    """Test 3: each builtin connector certifies through complete runner-owned evidence."""
    certify, fixture_set, cases, report, profile, parts = _certificate_parts(platform)
    activation_policy, certification_record, connector_id_cls, manifest_cls, requirement_cls = parts
    connector_id = connector_id_cls.parse(f"datasluice/{platform}")
    manifest = manifest_cls(
        connector_id=connector_id,
        entry_point=f"datasluice.connectors.catalog.{platform}.factory:create_{platform}_connector",
        profile_version=fixture_set.profile_version,
        optional_requirements=(),
        certification=None,
        activation_policy=activation_policy.EXPLICIT,
    )
    certification = certify(
        manifest=manifest,
        profile=profile,
        fixture_set=fixture_set,
        cases=cases,
        report=report,
        selected_connector_id=connector_id,
    )
    assert certification.connector_id == connector_id
    assert certification.profile_version == fixture_set.profile_version
    assert certification.fixture_fingerprint == fixture_set.fingerprint
    assert certification.outcome_count == len(report.outcomes)


def test_namespaced_third_party_fake_certifies_through_the_same_runner() -> None:
    """Test 3: a third-party namespaced connector certifies by identical evidence."""
    from dataclasses import replace

    certify, fixture_set, cases, report, profile, parts = _certificate_parts("socrata")
    activation_policy, certification_record, connector_id_cls, manifest_cls, requirement_cls = parts
    connector_id = connector_id_cls.parse("acme/socrata")
    namespaced_report = replace(report, connector_id=str(connector_id))
    manifest = manifest_cls(
        connector_id=connector_id,
        entry_point="acme.connectors.catalog.socrata:create_socrata_connector",
        profile_version=fixture_set.profile_version,
        optional_requirements=(
            requirement_cls(extra="acme-socrata", install_hint="Install DataSluice with `datasluice[acme-socrata]`."),
        ),
        certification=certification_record(
            connector_id=connector_id,
            contract_schema_version="1",
            profile_version=fixture_set.profile_version,
            report_version="1",
            report_id=namespaced_report.report_id,
        ),
        activation_policy=activation_policy.EXPLICIT,
    )
    certification = certify(
        manifest=manifest,
        profile=profile,
        fixture_set=fixture_set,
        cases=cases,
        report=namespaced_report,
        selected_connector_id=connector_id,
    )
    assert certification.connector_id == connector_id
    assert certification.report_fingerprint == namespaced_report.fingerprint


def test_public_catalog_models_are_frozen_typed_values() -> None:
    """Test 4: every public domain catalog dataclass is immutable."""
    modules = [
        "auth",
        "extensions",
        "ids",
        "models",
        "observability",
        "operations",
        "patches",
        "profiles",
        "receipts",
        "resilience",
        "safety",
    ]
    checked = 0
    for module_name in modules:
        module = importlib.import_module(f"datasluice.domain.catalog.{module_name}")
        for name, value in vars(module).items():
            if dataclasses.is_dataclass(value) and value.__module__ == module.__name__:
                assert value.__dataclass_params__.frozen, f"{module_name}.{name} must be frozen"
                checked += 1
    assert checked >= 50, f"expected the full public model corpus, checked {checked}"
    from dataclasses import FrozenInstanceError

    from datasluice.contracts.catalog import CaseOutcome, CatalogContractCase

    case = CatalogContractCase(operation_id="op")
    with pytest.raises(FrozenInstanceError):
        case.operation_id = "other"  # ty: ignore[invalid-assignment]
    outcome = CaseOutcome(operation_id="op", mode="sync", capability="available", state="passed")
    with pytest.raises(FrozenInstanceError):
        outcome.state = "failed"  # ty: ignore[invalid-assignment]


def test_public_catalog_imports_stay_import_light() -> None:
    """Test 4: importing the public catalog surface pulls no heavy optional runtime."""
    optional_imports = _optional_dependency_imports()
    script = (
        "import sys\n"
        "import datasluice\n"
        "import datasluice.contracts.catalog\n"
        "import datasluice.connectors.catalog.ckan\n"
        "import datasluice.connectors.catalog.udata\n"
        "import datasluice.connectors.catalog.socrata\n"
        f"heavy = [name for name in {optional_imports!r}"
        " if any(loaded == name or loaded.startswith(name + '.') for loaded in sys.modules)]\n"
        "print(','.join(heavy))\n"
    )
    env = {**os.environ, "PYTHONPATH": str(SRC_ROOT)}
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, env=env, check=True)
    assert result.stdout.strip() == "", f"public catalog imports pulled heavy modules: {result.stdout.strip()}"
    assert (SRC_ROOT / "datasluice" / "py.typed").is_file()


def test_public_catalog_surface_exposes_no_raw_http_methods() -> None:
    """Test 4: no public catalog module defines raw HTTP request methods or transport imports."""
    banned_definitions = {"request", "get_json", "download", "raw_request"}
    for tree_root in PUBLIC_CATALOG_TREES:
        for source in tree_root.rglob("*.py"):
            parsed = ast.parse(source.read_text(encoding="utf-8"))
            for node in ast.walk(parsed):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in banned_definitions:
                    pytest.fail(f"{source} defines raw HTTP method {node.name}()")
                if isinstance(node, (ast.ImportFrom, ast.Import)) and "datasluice.transport" in ast.unparse(node):
                    pytest.fail(f"{source} imports the transport layer at module scope")


def test_no_runtime_installation_or_implicit_activation_exists() -> None:
    """Test 4: nothing installs dependencies at runtime and activation stays explicit."""
    from datasluice.domain.catalog.extensions import ActivationPolicy

    assert {state.value for state in ActivationPolicy} == {"inactive", "explicit"}
    for source in (SRC_ROOT / "datasluice").rglob("*.py"):
        relative = source.relative_to(REPO_ROOT).as_posix()
        parsed = ast.parse(source.read_text(encoding="utf-8"))
        for node in ast.walk(parsed):
            if isinstance(node, ast.Import) and any(alias.name == "subprocess" for alias in node.names):
                pytest.fail(f"{relative} imports subprocess for runtime installation")
            if isinstance(node, ast.ImportFrom) and node.module == "subprocess":
                pytest.fail(f"{relative} imports subprocess for runtime installation")
            if isinstance(node, ast.Call) and ast.unparse(node.func) in {"os.system", "pip.main"}:
                pytest.fail(f"{relative} executes a package installation command")


def test_no_global_write_flag_or_sync_thread_async_wrapper_exists() -> None:
    """Test 4: catalog state stays instance-scoped and modes stay independently executed."""
    global_re = re.compile(r"^\s*global\s+\w+", re.MULTILINE)
    banned_calls = ("asyncio.to_thread", "run_until_complete", "new_event_loop", "asyncio.run(")
    for tree_root in (*PUBLIC_CATALOG_TREES, SRC_ROOT / "datasluice" / "runtime"):
        for source in tree_root.rglob("*.py"):
            relative = source.relative_to(REPO_ROOT).as_posix()
            if relative == "src/datasluice/contracts/catalog/runner.py":
                continue
            text = source.read_text(encoding="utf-8")
            assert not global_re.search(text), f"{relative} mutates module-global state"
            for call in banned_calls:
                assert call not in text, f"{relative} wraps async execution behind sync calls ({call})"


def test_extension_manifests_stay_immutable_and_evidence_bound() -> None:
    """Test 4: manifests carry no mutable collection fields."""
    from datasluice.domain.catalog.extensions import ConnectorManifest

    for field in dataclasses.fields(ConnectorManifest):
        assert _annotation_base_name(field.type) not in {"list", "dict", "set"}, (
            f"ConnectorManifest.{field.name} is a mutable extension bag"
        )


def _optional_dependency_imports() -> tuple[str, ...]:
    """Derive optional dependency import roots from the packaging table."""
    optional_dependencies = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["project"]["optional-dependencies"]
    distributions = {
        _dependency_distribution(dependency)
        for dependencies in optional_dependencies.values()
        for dependency in dependencies
    }
    package_map = importlib.metadata.packages_distributions()
    imports = {
        package
        for package, provided_by in package_map.items()
        if any(distribution in distributions for distribution in provided_by)
    }
    imports.discard("datasluice")
    return tuple(sorted(imports))


def _dependency_distribution(requirement: str) -> str:
    """Return a normalized distribution name from a dependency specifier."""
    return re.split(r"[<>=!~;[]", requirement, maxsplit=1)[0].replace("_", "-").lower()


def _annotation_base_name(annotation: object) -> str:
    """Return the unparameterized lower-case type name for an annotation."""
    rendered = str(annotation).strip().split("[")[0].split("|")[0].strip()
    return rendered.removeprefix("typing.").lower()


def test_optional_dependency_imports_follow_the_extras_table() -> None:
    """Every non-self optional distribution participates in the derived audit."""
    imports = _optional_dependency_imports()
    assert {"httpx", "pandas", "pyarrow", "zstandard", "fsspec"}.issubset(imports)


def test_mutable_annotation_base_name_detects_parameterized_fields() -> None:
    """Parameterized mutable annotations retain their mutable base name."""

    @dataclasses.dataclass(frozen=True)
    class MutableProbe:
        values: list[str]

    field = dataclasses.fields(MutableProbe)[0]
    assert _annotation_base_name(field.type) == "list"


def test_outputs_are_secret_safe_by_default() -> None:
    """Test 4: diagnostics stay redacted, telemetry off, TLS verified, reports sanitized."""
    from datasluice.contracts.catalog import CaseOutcome, ComplianceReport
    from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
    from datasluice.domain.catalog.observability import DiagnosticPolicy, TelemetryPolicy, TLSPolicy
    from datasluice.domain.catalog.receipts import MutationReceipt
    from datasluice.errors.catalog import NativeCatalogError
    from datasluice.exceptions import DataSluiceError
    from datasluice.logging import RedactingFilter
    from datasluice.runtime.redaction import redact_for_output

    assert DiagnosticPolicy().include_raw_body is False
    assert TelemetryPolicy().enabled is False
    assert TLSPolicy().verify is True
    leak = CaseOutcome(
        operation_id="op",
        mode="sync",
        capability="available",
        state="passed",
        warnings=("authorization: Bearer abc123",),
    )
    assert leak.warnings[0] == "authorization: [REDACTED]"
    report = ComplianceReport(
        outcomes=(leak,),
        warnings=("api_key: secret-value",),
    )
    assert report.warnings[0] == "api_key: [REDACTED]"
    serialized = json.dumps(report.to_dict())
    assert "abc123" not in serialized and "secret-value" not in serialized

    secret = "Bearer aBcDeFgH1234"
    with pytest.raises(DataSluiceError):
        MutationReceipt(
            operation="datasets.update",
            outcome="succeeded",
            target=CatalogId(CatalogPlatform.CKAN, ResourceKind.DATASET, "weather"),
            audit_metadata={"details": {"value": secret}},
        )
    receipt = MutationReceipt(
        operation="datasets.update",
        outcome="succeeded",
        target=CatalogId(CatalogPlatform.CKAN, ResourceKind.DATASET, "weather"),
        audit_metadata={"details": {"value": "Bearer ***"}},
    )
    error = NativeCatalogError(
        "request failed",
        operation="datasets.update",
        platform=CatalogPlatform.CKAN,
        metadata={"details": {"value": secret}},
    )
    record = logging.LogRecord("datasluice", logging.INFO, __file__, 1, "message", ({"details": secret},), None)
    RedactingFilter().filter(record)
    serialized_outputs = json.dumps(
        {
            "receipt": receipt.to_dict(),
            "error": dict(error.metadata),
            "log": record.args,
            "gate": redact_for_output(secret),
        },
        default=str,
    )
    assert "aBcDeFgH1234" not in serialized_outputs
    assert "Bearer ***" in serialized_outputs


def test_every_runtime_event_sink_is_secret_safe() -> None:
    """Test 4: every shipped event sink receives only gate-redacted envelopes."""
    from datasluice.runtime import events

    class CapturingHandler(logging.Handler):
        """Keep formatted log messages for sink assertions."""

        def __init__(self) -> None:
            super().__init__()
            self.messages: list[str] = []

        def emit(self, record: logging.LogRecord) -> None:
            """Capture one formatted log record."""
            self.messages.append(self.format(record))

    logger = logging.getLogger("datasluice.runtime.events.quality")
    handler = CapturingHandler()
    logger.addHandler(handler)
    logger.propagate = False
    try:
        sink_classes = [
            value
            for name, value in inspect.getmembers(events, inspect.isclass)
            if name.endswith("Sink") and not getattr(value, "_is_protocol", False)
        ]
        sinks = [value(logger) if value is events.LoggingSink else value() for value in sink_classes]
        envelope = events.EventEmitter(sinks=tuple(sinks)).record(
            operation_id="reference/datasets/get",
            platform="reference",
            outcome="succeeded",
            metadata={"bearer": "Bearer aBcDeFgH1234", "detail": "api_key=credential-value"},
        )
    finally:
        logger.removeHandler(handler)

    serialized = json.dumps(envelope.to_dict())
    list_outputs = [json.dumps(sink.events[0].to_dict()) for sink in sinks if isinstance(sink, events.ListSink)]
    all_outputs = (serialized, *list_outputs, *handler.messages)
    assert all("aBcDeFgH1234" not in output and "credential-value" not in output for output in all_outputs)


def test_no_alias_shim_or_deprecation_wrapper_survives() -> None:
    """Test 4: public modules define no compatibility shims."""
    shim_tokens = ("DeprecationWarning", "PendingDeprecationWarning", "warnings.warn")
    for tree_root in (*PUBLIC_CATALOG_TREES, SRC_ROOT / "datasluice" / "runtime", SRC_ROOT / "datasluice" / "cli"):
        for source in tree_root.rglob("*.py"):
            text = source.read_text(encoding="utf-8")
            for token in shim_tokens:
                assert token not in text, f"{source} keeps a deprecation shim ({token})"
            parsed = ast.parse(text)
            for node in parsed.body:
                if isinstance(node, ast.FunctionDef) and node.name == "__getattr__":
                    pytest.fail(f"{source} defines a module-level __getattr__ alias shim")


def test_base_installation_stays_lean_without_connector_extras() -> None:
    """Test 4: the base distribution stays lean while connector extras stay explicit."""
    project = _pyproject()["project"]
    dependencies = project["dependencies"]
    assert isinstance(dependencies, list)
    assert sorted(dependencies) == ["rich", "typer"]
    optional = project["optional-dependencies"]
    assert isinstance(optional, dict)
    assert set(optional) == {
        "all",
        "all-connectors",
        "ckan",
        "compression",
        "dlt",
        "duckdb",
        "http",
        "keychain",
        "oauth",
        "pandas",
        "parquet",
        "polars",
        "secrets-aws",
        "secrets-vault",
        "socrata",
        "storage",
        "telemetry",
        "udata",
        "xlsx",
    }
    assert optional["all-connectors"] == ["datasluice[ckan,udata,socrata]"]
    assert all(optional[platform] == ["datasluice[http]"] for platform in ("ckan", "udata", "socrata"))
