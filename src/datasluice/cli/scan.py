"""``datasluice scan`` command for bounded resource profiling."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any

import typer

from datasluice.cli._output import diagnostic_console, render_json, result_console
from datasluice.cli._resolver import open_data_sluice, parse_locator, resolve_one_resource
from datasluice.exceptions import DataSluiceError

DEFAULT_SCAN_ROWS = 1_000
DEFAULT_SAMPLE_ROWS = 20


def _scan_result(opened: Any, *, full: bool) -> dict[str, Any]:
    """Collect bounded schema and null statistics from an opened resource."""
    columns: dict[str, dict[str, Any]] = {}
    sample: list[Mapping[str, Any]] = []
    rows = 0
    limit = None if full else DEFAULT_SCAN_ROWS

    for batch in opened.iter_batches():
        remaining = None if limit is None else limit - rows
        if remaining == 0:
            break
        selected = batch if remaining is None or batch.num_rows <= remaining else batch.slice(0, remaining)
        _update_columns(columns, selected)
        _append_sample(sample, selected)
        rows += selected.num_rows
        if limit is not None and rows == limit:
            break

    return {"rows": rows, "columns": list(columns.values()), "sample": sample, "bounded": not full}


def _update_columns(columns: dict[str, dict[str, Any]], batch: Any) -> None:
    for index, field in enumerate(batch.schema):
        stats = columns.setdefault(field.name, {"name": field.name, "type": str(field.type), "null_count": 0})
        stats["null_count"] += batch.column(index).null_count


def _append_sample(sample: list[Mapping[str, Any]], batch: Any) -> None:
    if len(sample) < DEFAULT_SAMPLE_ROWS:
        sample.extend(batch.to_pylist()[: DEFAULT_SAMPLE_ROWS - len(sample)])


def _render_human(result: Mapping[str, Any]) -> None:
    """Render a concise Rich result for interactive use."""
    result_console.print(f"[bold]Rows scanned:[/bold] {result['rows']}")
    result_console.print(f"[bold]Columns:[/bold] {len(result['columns'])}")
    result_console.print(result["sample"])


def scan(
    locator: Annotated[str, typer.Argument(help="Direct resource URI or local path")],
    full: Annotated[bool, typer.Option("--full", help="Compute exact whole-resource statistics")] = False,
    output: Annotated[str, typer.Option("--output", help="Output format: human or json")] = "human",
) -> None:
    """Profile one resource with a bounded default sample."""
    if output not in {"human", "json"}:
        diagnostic_console.print("[red]Error:[/red] --output must be human or json")
        raise typer.Exit(1)
    try:
        parsed = parse_locator(locator)
        with open_data_sluice() as data_sluice:
            resolved_locator, resolved_resource = resolve_one_resource(data_sluice, parsed)
            diagnostic_console.print("Scanning resource")
            with data_sluice.open(resolved_resource) as opened:
                result = _scan_result(opened, full=full)
        result["locator"] = resolved_locator.to_dict()
    except DataSluiceError as exc:
        diagnostic_console.print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(1) from exc
    if output == "json":
        render_json(result)
    else:
        _render_human(result)
