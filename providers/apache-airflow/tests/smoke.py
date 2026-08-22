"""Provider smoke convention module.

Validates the installed provider package, its discovery metadata, and the
runtime hook/operator declarations, and the absence of connection declarations.
Run from the provider package root: ``python tests/smoke.py``.
"""

from __future__ import annotations

import sys

_RUNTIME_DECLARATION_KEYS = ("operators", "hooks")
_FORBIDDEN_DECLARATION_KEYS = ("hook-class-names", "connection-types")


def main() -> int:
    import airflow.providers.datasluice as pkg
    from airflow.providers.datasluice.get_provider_info import get_provider_info

    assert pkg.__name__ == "airflow.providers.datasluice"
    info = get_provider_info()
    assert info["package-name"] == "apache-airflow-providers-datasluice", info
    for key in _RUNTIME_DECLARATION_KEYS:
        assert info.get(key), f"provider metadata omits {key}"
    for key in _FORBIDDEN_DECLARATION_KEYS:
        assert key not in info, f"provider metadata declares {key}"
    return 0


if __name__ == "__main__":
    sys.exit(main())
