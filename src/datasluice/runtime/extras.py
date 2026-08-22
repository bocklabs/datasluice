"""Runtime gates for optional DataSluice capabilities."""

from __future__ import annotations

import importlib.util

_EXTRA_IMPORTS = {
    "ckan": "httpx",
    "http": "httpx",
    "socrata": "httpx",
    "udata": "httpx",
}


def require_extra(extra_name: str) -> None:
    """Require the dependency that implements an optional extra.

    Args:
        extra_name: The published optional-dependency name to require.

    Raises:
        ImportError: If the extra's implementation dependency is unavailable.
        ValueError: If the extra has no runtime gate.
    """
    try:
        module_name = _EXTRA_IMPORTS[extra_name]
    except KeyError as exc:
        raise ValueError(f"No runtime dependency gate is defined for datasluice[{extra_name}].") from exc
    if importlib.util.find_spec(module_name) is None:
        raise ImportError(
            f"This feature requires datasluice[{extra_name}]. Install with: pip install datasluice[{extra_name}]"
        )
