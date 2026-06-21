"""``datasluice detect`` command."""

from __future__ import annotations

import typer
from rich.console import Console

console = Console()


def detect(
    portal: str = typer.Argument(..., help="Portal base URL to inspect"),
) -> None:
    """Auto-detect the platform type of an open-data portal."""
    from datasluice.discovery import detect_portal_type

    try:
        portal_type = detect_portal_type(portal)
        console.print(f"[green]Detected portal type:[/green] [bold]{portal_type}[/bold]")
    except Exception as exc:
        console.print(f"[red]Detection failed:[/red] {exc}")
        raise typer.Exit(1) from exc
