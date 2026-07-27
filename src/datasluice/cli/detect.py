"""``datasluice detect`` command — evidence-based portal detection (D-P5-21)."""

from __future__ import annotations

from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

console = Console()


def detect(
    portal: Annotated[str, typer.Argument(help="Portal base URL to inspect")],
    portal_type: Annotated[str | None, typer.Option("--type", "-t", help="Explicit portal type override")] = None,
) -> None:
    """Auto-detect the platform type of an open-data portal.

    Probes well-known API endpoints and renders a rich table of every
    detection-probe outcome. Exits with code 1 when no portal is detected
    (D-P5-21).
    """
    if portal_type is not None:
        console.print(f"[green]Using explicit portal_type:[/green] [bold]{portal_type}[/bold]")
        return

    from datasluice.discovery import detect as do_detect
    from datasluice.domain.detection import DetectionEvidence
    from datasluice.runtime.plugin_manager import PluginManager
    from datasluice.transport import HttpClient

    result = do_detect(portal, transport=HttpClient(), plugin_manager=PluginManager())
    if result.portal_type is None:
        console.print(f"[red]No portal detected for[/red] {portal!r} ({len(result.evidence)} probes)")
        raise typer.Exit(1)

    console.print(f"[green]Detected:[/green] [bold]{result.portal_type}[/bold] (confidence {result.confidence:.2f})")
    table = Table(title="Detection Evidence")
    table.add_column("Probe", style="cyan")
    table.add_column("Matched")
    table.add_column("Detail")
    for evidence in result.evidence:
        ev: DetectionEvidence = evidence
        matched_cell = "[green]yes[/green]" if ev.matched else "[dim]no[/dim]"
        table.add_row(str(ev.check), matched_cell, str(ev.detail))
    console.print(table)
