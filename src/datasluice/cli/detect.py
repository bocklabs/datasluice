"""``datasluice detect`` command — evidence-based portal detection (D-P5-21, D-07)."""

from __future__ import annotations

from typing import Annotated, Any

import typer

from datasluice import DataSluice
from datasluice.cli._output import diagnostic_console, render_json, result_console
from datasluice.exceptions import DataSluiceError


def _detection_json(result: Any) -> dict[str, Any]:
    """Serialize one public DetectionResult into a JSON-safe envelope."""
    return {
        "portal_type": result.portal_type,
        "confidence": result.confidence,
        "evidence": [
            {"check": str(ev.check), "matched": ev.matched, "detail": str(ev.detail)} for ev in result.evidence
        ],
    }


def _render_human(result: Any) -> None:
    """Render Rich detection evidence to stdout."""
    from rich.table import Table

    result_console.print(
        f"[green]Detected:[/green] [bold]{result.portal_type}[/bold] (confidence {result.confidence:.2f})"
    )
    table = Table(title="Detection Evidence")
    table.add_column("Probe", style="cyan")
    table.add_column("Matched")
    table.add_column("Detail")
    for evidence in result.evidence:
        matched_cell = "[green]yes[/green]" if evidence.matched else "[dim]no[/dim]"
        table.add_row(str(evidence.check), matched_cell, str(evidence.detail))
    result_console.print(table)


def detect(
    portal: Annotated[str, typer.Argument(help="Portal base URL to inspect")],
    portal_type: Annotated[str | None, typer.Option("--type", "-t", help="Explicit portal type override")] = None,
    output: Annotated[str, typer.Option("--output", help="Output format: human or json")] = "human",
) -> None:
    """Auto-detect the platform type of an open-data portal.

    Probes well-known API endpoints and renders a rich table of every
    detection-probe outcome. Exits with code 1 when no portal is detected
    (D-P5-21).
    """
    if output not in {"human", "json"}:
        diagnostic_console.print("[red]Error:[/red] --output must be human or json")
        raise typer.Exit(1)
    if portal_type is not None:
        result_console.print(f"[green]Using explicit portal_type:[/green] [bold]{portal_type}[/bold]")
        return
    try:
        with DataSluice() as ds:
            result = ds.detect(portal)
    except DataSluiceError as exc:
        diagnostic_console.print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(1) from exc
    if result.portal_type is None:
        diagnostic_console.print(f"[red]No portal detected for[/red] {portal!r} ({len(result.evidence)} probes)")
        raise typer.Exit(1)
    if output == "json":
        render_json(_detection_json(result))
    else:
        _render_human(result)
