"""``datasluice inspect`` command."""

from __future__ import annotations

from typing import Annotated, Any

import typer

from datasluice import DataSluice
from datasluice.cli._output import diagnostic_console, render_json, result_console
from datasluice.exceptions import DataSluiceError


def _dataset_json(dataset: Any) -> dict[str, Any]:
    """Serialize one catalog dataset into a JSON-safe metadata envelope."""
    return {
        "id": dataset.id,
        "title": dataset.title or dataset.name,
        "description": dataset.description,
        "url": dataset.url,
        "organization": dataset.organization.name if dataset.organization else None,
        "tags": list(dataset.tags) if dataset.tags else [],
        "resources": [
            {
                "id": resource.id,
                "name": resource.name or resource.id,
                "format": resource.format,
                "url": resource.url,
            }
            for resource in dataset.resources
        ],
    }


def _render_human(dataset: Any) -> None:
    """Render Rich catalog metadata to stdout without reading resource bytes."""
    from rich.panel import Panel
    from rich.table import Table

    result_console.print(
        Panel(
            f"[bold]{dataset.title or dataset.name or dataset.id}[/bold]",
            subtitle=dataset.url or "",
            title=f"Dataset: {dataset.id}",
        )
    )
    if dataset.description:
        result_console.print(f"\n{dataset.description[:500]}{'...' if len(dataset.description or '') > 500 else ''}\n")
    if dataset.organization:
        result_console.print(f"[green]Organization:[/green] {dataset.organization.title or dataset.organization.name}")
    if dataset.tags:
        result_console.print(f"[blue]Tags:[/blue] {', '.join(dataset.tags)}")
    if dataset.resources:
        table = Table(title="Resources")
        table.add_column("Name", style="white")
        table.add_column("Format", style="cyan")
        table.add_column("URL", style="blue", overflow="fold")
        for resource in dataset.resources:
            table.add_row(resource.name or resource.id, resource.format or "", resource.url or "")
        result_console.print(table)


def inspect(
    portal: Annotated[str, typer.Option("--portal", "-p", help="Portal base URL")],
    dataset_id: Annotated[str, typer.Argument(help="Dataset ID")],
    output: Annotated[str, typer.Option("--output", help="Output format: human or json")] = "human",
) -> None:
    """Inspect a single dataset's catalog metadata without reading resource bytes."""
    if output not in {"human", "json"}:
        diagnostic_console.print("[red]Error:[/red] --output must be human or json")
        raise typer.Exit(1)
    try:
        with DataSluice() as ds:
            dataset = ds.portal(portal).get_dataset(dataset_id)
    except DataSluiceError as exc:
        diagnostic_console.print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(1) from exc
    if output == "json":
        render_json(_dataset_json(dataset))
    else:
        _render_human(dataset)
