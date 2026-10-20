"""Shared human and machine output helpers for CLI workflows."""

from __future__ import annotations

import json
import sys
from typing import TYPE_CHECKING, Any

import typer
from rich.console import Console

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

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


def validate_output(output: str, *, allowed: frozenset[str] = frozenset({"human", "json"})) -> None:
    """Reject unsupported output formats before processing input."""
    if output not in allowed:
        raise typer.BadParameter(f"--output must be one of: {', '.join(sorted(allowed))}")
