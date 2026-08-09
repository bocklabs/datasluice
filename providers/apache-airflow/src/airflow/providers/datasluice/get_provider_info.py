"""Native Airflow provider discovery callable for DataSluice.

Returns declarative metadata consumed by Airflow's ``ProviderManager`` via the
``apache_airflow_provider`` entry point. The callable is metadata-only: it performs
no Connection lookup, network I/O, or private-core import.
"""

from __future__ import annotations

import importlib.metadata
from typing import Any

_PROVIDER_PACKAGE = "apache-airflow-providers-datasluice"
_PROVIDER_NAME = "DataSluice"
_PROVIDER_DESCRIPTION = "Apache Airflow provider for DataSluice open-data discovery, streaming, and materialization."
_HOOK_CLASS = "airflow.providers.datasluice.hooks.datasluice.DataSluiceHook"
_OPERATOR_MODULES = (
    "airflow.providers.datasluice.operators.search",
    "airflow.providers.datasluice.operators.materialize",
)


def get_provider_info() -> dict[str, Any]:
    """Return the provider metadata dict consumed by Airflow ``ProviderManager``.

    Returns:
        A mapping with the locked package identity, hook, and connection metadata.
    """
    return {
        "package-name": _PROVIDER_PACKAGE,
        "name": _PROVIDER_NAME,
        "description": _PROVIDER_DESCRIPTION,
        "versions": [importlib.metadata.version(_PROVIDER_PACKAGE)],
        "operators": [
            {
                "integration-name": _PROVIDER_NAME,
                "python-modules": list(_OPERATOR_MODULES),
            }
        ],
        "hook-class-names": [_HOOK_CLASS],
        "connection-types": [
            {
                "hook-class-name": _HOOK_CLASS,
                "connection-type": "datasluice",
            }
        ],
    }
