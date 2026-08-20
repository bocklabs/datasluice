"""Native Airflow provider discovery callable for DataSluice.

Returns declarative metadata consumed by Airflow's ``ProviderManager`` via the
``apache_airflow_provider`` entry point. The callable performs no Connection
lookup or network I/O. Platform actions await the canonical executors of later
phases.
"""

from __future__ import annotations

import importlib.metadata
from typing import Any

_PROVIDER_PACKAGE = "apache-airflow-providers-datasluice"
_PROVIDER_NAME = "DataSluice"
_PROVIDER_DESCRIPTION = (
    "Apache Airflow provider for DataSluice runtime client composition. Platform actions await the "
    "canonical executors of Phases 3-5."
)


def get_provider_info() -> dict[str, Any]:
    """Return the provider metadata dict consumed by Airflow ``ProviderManager``.

    Returns:
        A mapping with the locked package identity and runtime declarations.
    """
    return {
        "package-name": _PROVIDER_PACKAGE,
        "name": _PROVIDER_NAME,
        "description": _PROVIDER_DESCRIPTION,
        "versions": [importlib.metadata.version(_PROVIDER_PACKAGE)],
        "hooks": [{"integration-name": _PROVIDER_NAME, "python-modules": ["airflow.providers.datasluice.hooks"]}],
        "operators": [
            {"integration-name": _PROVIDER_NAME, "python-modules": ["airflow.providers.datasluice.operators"]}
        ],
    }
