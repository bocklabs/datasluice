"""Shared human and machine output helpers for CLI workflows."""

from __future__ import annotations

import json
import sys
from collections.abc import Iterable, Mapping
from typing import Any

from rich.console import Console

result_console = Console()
diagnostic_console = Console(stderr=True)


def render_json(value: Mapping[str, Any]) -> None:
    """Write one machine-readable JSON result to stdout."""
    json.dump(value, sys.stdout, default=str, separators=(",", ":"))
    sys.stdout.write("\n")


def render_jsonl_rows(rows: Iterable[Mapping[str, Any]]) -> None:
    """Write rows as incrementally serialized JSON Lines on stdout."""
    for row in rows:
        json.dump(row, sys.stdout, default=str, separators=(",", ":"))
        sys.stdout.write("\n")
