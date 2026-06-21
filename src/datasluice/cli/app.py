"""Main Typer application for the DataSluice CLI."""

from __future__ import annotations

import typer
from rich.console import Console

from datasluice import __version__
from datasluice.cli.detect import detect
from datasluice.cli.download import download
from datasluice.cli.inspect import inspect
from datasluice.cli.search import search

console = Console()

app = typer.Typer(
    name="datasluice",
    help="One Python interface for open-data discovery, extraction, and integration.",
    no_args_is_help=True,
)


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        "-V",
        help="Show the version and exit.",
        is_eager=True,
    ),
) -> None:
    """DataSluice — unified open-data toolkit."""
    if version:
        console.print(f"datasluice {__version__}")
        raise typer.Exit()


app.command(name="search")(search)
app.command(name="inspect")(inspect)
app.command(name="download")(download)
app.command(name="detect")(detect)


if __name__ == "__main__":
    app()
