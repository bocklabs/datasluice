"""``datasluice materialize`` command for one canonical Artifact result."""

from __future__ import annotations

from typing import Annotated, Any

import typer

from datasluice import DataSluice
from datasluice.cli._output import diagnostic_console, render_json, result_console
from datasluice.cli._resolver import parse_locator, resolve_one_resource
from datasluice.exceptions import DataSluiceError


def _render_human(artifact: Any) -> None:
    """Render public Artifact fields without exposing source credentials."""
    result_console.print(f"[bold]URI:[/bold] {artifact.uri}")
    result_console.print(f"[bold]Media type:[/bold] {artifact.media_type}")
    result_console.print(f"[bold]Size:[/bold] {artifact.size}")
    result_console.print(f"[bold]Content digest:[/bold] {artifact.content_digest.value}")
    result_console.print(f"[bold]Blob digest:[/bold] {artifact.blob_digest.value}")
    result_console.print(f"[bold]Source:[/bold] {artifact.provenance.source_locator.to_dict()}")


def materialize(
    destination: Annotated[str, typer.Option("--destination", help="Destination URI for the Artifact")],
    locator: Annotated[str, typer.Argument(help="Direct resource URI or local path")],
    mode: Annotated[str, typer.Option("--mode", help="Materialization mode: parquet or raw")] = "parquet",
    output: Annotated[str, typer.Option("--output", help="Output format: human or json")] = "human",
) -> None:
    """Materialize exactly one resource into a canonical Artifact."""
    if mode not in {"parquet", "raw"}:
        diagnostic_console.print("[red]Error:[/red] --mode must be parquet or raw")
        raise typer.Exit(1)
    if output not in {"human", "json"}:
        diagnostic_console.print("[red]Error:[/red] --output must be human or json")
        raise typer.Exit(1)
    try:
        parsed = parse_locator(locator)
        with DataSluice() as data_sluice:
            _resolved_locator, resolved_resource = resolve_one_resource(data_sluice, parsed)
            diagnostic_console.print("Materializing resource")
            artifact = data_sluice.materialize(resolved_resource, destination, mode=mode)
    except DataSluiceError as exc:
        diagnostic_console.print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(1) from exc
    if output == "json":
        render_json(artifact.to_dict())
    else:
        _render_human(artifact)
