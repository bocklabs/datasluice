"""``datasluice detect`` command."""

from __future__ import annotations

import typer
from rich.console import Console

console = Console()


def detect(
    portal: str = typer.Argument(..., help="Portal base URL to inspect"),
) -> None:
    """Auto-detect the platform type of an open-data portal."""
    from datasluice.discovery import detect
    from datasluice.runtime.plugin_manager import PluginManager
    from datasluice.transport import HttpClient

    try:
        result = detect(portal, transport=HttpClient(), plugin_manager=PluginManager())
        if result.portal_type is None:
            console.print(f"[red]No portal detected for[/red] {portal!r}")
            raise typer.Exit(1)
        console.print(f"[green]Detected portal type:[/green] [bold]{result.portal_type}[/bold]")
    except Exception as exc:
        console.print(f"[red]Detection failed:[/red] {exc}")
        raise typer.Exit(1) from exc
