"""Fixture loading helpers for the conformance suite.

Hand-authored portal-response fixtures live under ``tests/fixtures/<portal>/``
as small JSON documents: no VCR cassettes, no recorded-live captures,
no credentials. These helpers load them into parsed dicts for
:func:`datasluice.contracts.run_contract_suite` callers.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any


def load_fixture(path: str | Path) -> dict[str, Any]:
    """Load a single hand-authored portal-response fixture from *path*.

    Args:
        path: Filesystem path to a JSON fixture document.

    Returns:
        The parsed JSON payload.

    Raises:
        ValueError: When the top-level JSON value is not an object.
    """
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        msg = f"fixture {path} must be a JSON object, got {type(payload).__name__}"
        raise ValueError(msg)
    return payload


def load_fixture_set(paths: Mapping[str, str | Path]) -> dict[str, dict[str, Any]]:
    """Load a keyed fixture set: ``{fixture_name: parsed fixture JSON}``."""
    return {name: load_fixture(path) for name, path in paths.items()}
