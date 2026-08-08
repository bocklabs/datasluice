"""``datasluice search`` command."""

from __future__ import annotations

from typing import Annotated, Any

import typer

from datasluice import DataSluice
from datasluice.cli._output import diagnostic_console, render_json, result_console
from datasluice.domain import Query
from datasluice.exceptions import DataSluiceError


def _dataset_json(dataset: Any) -> dict[str, Any]:
    """Serialize one catalog dataset into a JSON-safe summary."""
    return {
        "id": dataset.id,
        "title": dataset.title or dataset.name,
        "organization": dataset.organization.name if dataset.organization else None,
        "resource_count": len(dataset.resources),
    }


def _search_json(result: Any, portal: str, query: str | None) -> dict[str, Any]:
    """Build one machine-readable search result envelope."""
    return {
        "portal": portal,
        "query": query,
        "total": result.total,
        "datasets": [_dataset_json(dataset) for dataset in result.datasets],
    }


def _render_human(result: Any, portal: str, query: str | None) -> None:
    """Render a Rich table of search results to stdout."""
    from rich.table import Table

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
    result_console.print(table)
    result_console.print(f"\n[dim]{result.total} total result(s)[/dim]")


def search(
    portal: Annotated[str, typer.Option("--portal", "-p", help="Portal base URL")],
    query: Annotated[str | None, typer.Argument(help="Search query")] = None,
    limit: Annotated[int, typer.Option("--limit", "-n", help="Maximum results")] = 20,
    output: Annotated[str, typer.Option("--output", help="Output format: human or json")] = "human",
) -> None:
    """Search for datasets on an open-data portal."""
    if output not in {"human", "json"}:
        diagnostic_console.print("[red]Error:[/red] --output must be human or json")
        raise typer.Exit(1)
    try:
        with DataSluice() as ds:
            result = ds.search(portal, Query(text=query, limit=limit))
    except DataSluiceError as exc:
        diagnostic_console.print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(1) from exc
    if output == "json":
        render_json(_search_json(result, portal, query))
    else:
        _render_human(result, portal, query)
