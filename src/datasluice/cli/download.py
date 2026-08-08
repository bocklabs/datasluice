"""``datasluice download`` command — raw bulk copy (D-15)."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

import typer

from datasluice import DataSluice
from datasluice.cli._output import diagnostic_console, render_json, result_console
from datasluice.exceptions import DataSluiceError


def _render_human(results: list[dict[str, object]], count: int) -> None:
    """Render raw download results to stdout."""
    result_console.print(f"[green]Downloaded {count} file(s)[/green]")
    for entry in results:
        result_console.print(f"  {entry['path']}")


def download(
    portal: Annotated[str, typer.Option("--portal", "-p", help="Portal base URL")],
    dataset_id: Annotated[str, typer.Argument(help="Dataset ID")],
    dest: Annotated[Path, typer.Option("--dest", "-o", help="Destination directory")] = Path("."),
    fmt: Annotated[str | None, typer.Option("--format", "-f", help="Filter resources by format")] = None,
    output: Annotated[str, typer.Option("--output", help="Output format: human or json")] = "human",
) -> None:
    """Download all resources from a dataset as raw bulk copies."""
    if output not in {"human", "json"}:
        diagnostic_console.print("[red]Error:[/red] --output must be human or json")
        raise typer.Exit(1)
    try:
        with DataSluice() as ds:
            dataset = ds.portal(portal).get_dataset(dataset_id)
            resources = list(dataset.resources)
            if fmt:
                resources = [r for r in resources if (r.format or "").upper() == fmt.upper()]
            if not resources:
                diagnostic_console.print("[yellow]No resources found matching criteria.[/yellow]")
                raise typer.Exit(1)
            diagnostic_console.print(f"[bold]Downloading {len(resources)} resource(s) to {dest}...[/bold]")
            dest.mkdir(parents=True, exist_ok=True)
            results = ds.download_many(resources, str(dest))
    except DataSluiceError as exc:
        diagnostic_console.print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(1) from exc
    payload: dict[str, Any] = {"count": len(results), "downloaded": results}
    if output == "json":
        render_json(payload)
    else:
        _render_human(results, len(results))
