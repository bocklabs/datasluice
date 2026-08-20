"""``datasluice open`` command for bounded previews and JSONL streams."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Annotated, Any

import typer

from datasluice.cli._output import diagnostic_console, render_json, render_jsonl_rows, result_console
from datasluice.cli._resolver import open_data_sluice, parse_locator, resolve_one_resource
from datasluice.exceptions import DataSluiceError

DEFAULT_PREVIEW_ROWS = 20


def _iter_rows(opened: Any, *, limit: int | None) -> Iterator[Mapping[str, Any]]:
    """Yield rows incrementally while honoring an optional row limit."""
    emitted = 0
    for batch in opened.iter_batches():
        remaining = None if limit is None else limit - emitted
        if remaining == 0:
            return
        selected = batch if remaining is None or batch.num_rows <= remaining else batch.slice(0, remaining)
        yield from selected.to_pylist()
        emitted += selected.num_rows
        if limit is not None and emitted == limit:
            return


def _render_human(rows: list[Mapping[str, Any]]) -> None:
    """Render an interactive preview without machine-output decoration."""
    result_console.print(rows)


def open(
    locator: Annotated[str, typer.Argument(help="Direct resource URI or local path")],
    all_rows: Annotated[bool, typer.Option("--all", help="Stream every row as JSON Lines")] = False,
    output: Annotated[str, typer.Option("--output", help="Output format: human, json, or jsonl")] = "human",
) -> None:
    """Preview one resource or incrementally stream it as JSON Lines."""
    if output not in {"human", "json", "jsonl"}:
        diagnostic_console.print("[red]Error:[/red] --output must be human, json, or jsonl")
        raise typer.Exit(1)
    if all_rows and output != "jsonl":
        diagnostic_console.print("[red]Error:[/red] --all requires --output jsonl")
        raise typer.Exit(1)
    try:
        parsed = parse_locator(locator)
        with open_data_sluice() as data_sluice:
            resolved_locator, resolved_resource = resolve_one_resource(data_sluice, parsed)
            diagnostic_console.print("Opening resource")
            with data_sluice.open(resolved_resource) as opened:
                if output == "jsonl":
                    render_jsonl_rows(_iter_rows(opened, limit=None if all_rows else DEFAULT_PREVIEW_ROWS))
                    return
                rows = list(_iter_rows(opened, limit=DEFAULT_PREVIEW_ROWS))
        result = {"locator": resolved_locator.to_dict(), "rows": rows}
    except DataSluiceError as exc:
        diagnostic_console.print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(1) from exc
    if output == "json":
        render_json(result)
    else:
        _render_human(rows)
