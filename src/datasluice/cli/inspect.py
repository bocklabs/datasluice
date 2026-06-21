"""``datasluice inspect`` command."""

from __future__ import annotations

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


def inspect(
    portal: str = typer.Option(..., "--portal", "-p", help="Portal base URL"),
    dataset_id: str = typer.Argument(..., help="Dataset ID"),
) -> None:
    """Inspect a single dataset in detail."""
    from datasluice import DataSluice

    ds = DataSluice(portal)
    dataset = ds.get_dataset(dataset_id)

    console.print(
        Panel(
            f"[bold]{dataset.title or dataset.name or dataset.id}[/bold]",
            subtitle=dataset.url or "",
            title=f"Dataset: {dataset.id}",
        )
    )

    if dataset.description:
        console.print(f"\n{dataset.description[:500]}{'...' if len(dataset.description or '') > 500 else ''}\n")

    if dataset.organization:
        console.print(f"[green]Organization:[/green] {dataset.organization.title or dataset.organization.name}")

    if dataset.tags:
        console.print(f"[blue]Tags:[/blue] {', '.join(dataset.tags)}")

    if dataset.resources:
        table = Table(title="Resources")
        table.add_column("Name", style="white")
        table.add_column("Format", style="cyan")
        table.add_column("URL", style="blue", overflow="fold")
        for resource in dataset.resources:
            table.add_row(
                resource.name or resource.id,
                resource.format or "",
                resource.url or "",
            )
        console.print(table)
