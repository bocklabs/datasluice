"""``datasluice search`` command."""

from __future__ import annotations

import typer
from rich.console import Console
from rich.table import Table

from datasluice.domain import Query

console = Console()


def search(
    portal: str = typer.Option(..., "--portal", "-p", help="Portal base URL"),
    query: str = typer.Argument(None, help="Search query"),
    limit: int = typer.Option(20, "--limit", "-n", help="Maximum results"),
) -> None:
    """Search for datasets on an open-data portal."""
    from datasluice import DataSluice

    ds = DataSluice(portal)
    result = ds.search(Query(text=query, limit=limit))

    table = Table(title=f"Search: {query or '(all)'} on {portal}")
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Title", style="white")
    table.add_column("Org", style="green")
    table.add_column("Resources", justify="right")

    for dataset in result.datasets:
        table.add_row(
            str(dataset.id),
            dataset.title or dataset.name or "",
            dataset.organization.name if dataset.organization else "",
            str(len(dataset.resources)),
        )

    console.print(table)
    console.print(f"\n[dim]{result.total} total result(s)[/dim]")
