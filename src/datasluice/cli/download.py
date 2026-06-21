"""``datasluice download`` command."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

console = Console()


def download(
    portal: Annotated[str, typer.Option("--portal", "-p", help="Portal base URL")],
    dataset_id: Annotated[str, typer.Argument(help="Dataset ID")],
    dest: Annotated[Path, typer.Option("--dest", "-o", help="Destination directory")] = Path("."),
    fmt: Annotated[str | None, typer.Option("--format", "-f", help="Filter resources by format")] = None,
) -> None:
    """Download all resources from a dataset."""
    from datasluice import DataSluice

    ds = DataSluice(portal)
    dataset = ds.get_dataset(dataset_id)

    resources = dataset.resources
    if fmt:
        resources = [r for r in resources if (r.format or "").upper() == fmt.upper()]

    if not resources:
        console.print("[yellow]No resources found matching criteria.[/yellow]")
        raise typer.Exit(1)

    console.print(f"[bold]Downloading {len(resources)} resource(s) to {dest}...[/bold]")
    dest.mkdir(parents=True, exist_ok=True)
    paths = ds.download_all(dataset, dest)
    console.print(f"[green]Downloaded {len(paths)} file(s)[/green]")
    for path in paths:
        console.print(f"  {path}")
