"""Native Airflow provider discovery callable for DataSluice.

Returns declarative metadata consumed by Airflow's ``ProviderManager`` via the
``apache_airflow_provider`` entry point. The callable is metadata-only: it
performs no Connection lookup, network I/O, or private-core import. Runtime
integration awaits the canonical platform executors of later phases.
"""

from __future__ import annotations

import importlib.metadata
from typing import Any

_PROVIDER_PACKAGE = "apache-airflow-providers-datasluice"
_PROVIDER_NAME = "DataSluice"
_PROVIDER_DESCRIPTION = (
    "Metadata-only Apache Airflow provider for DataSluice. Runtime integration awaits the "
    "canonical platform executors of later phases."
)


def get_provider_info() -> dict[str, Any]:
    """Return the provider metadata dict consumed by Airflow ``ProviderManager``.

    Returns:
        A mapping with the locked package identity and no runtime registration.
    """
    return {
        "package-name": _PROVIDER_PACKAGE,
        "name": _PROVIDER_NAME,
        "description": _PROVIDER_DESCRIPTION,
        "versions": [importlib.metadata.version(_PROVIDER_PACKAGE)],
    }
