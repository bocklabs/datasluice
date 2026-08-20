"""Main Typer application for the DataSluice CLI."""

from __future__ import annotations

from typing import Annotated

import typer
from rich.console import Console

from datasluice import __version__
from datasluice.cli.capabilities import app as capabilities_app
from datasluice.cli.credentials import app as credentials_app
from datasluice.cli.materialize import materialize
from datasluice.cli.open import open
from datasluice.cli.scan import scan

console = Console()

app = typer.Typer(
    name="datasluice",
    help="Catalog capabilities, credentials, and direct resource data operations.",
    no_args_is_help=True,
)


@app.callback(invoke_without_command=True)
def main(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            "-V",
            help="Show the version and exit.",
            is_eager=True,
        ),
    ] = False,
) -> None:
    """DataSluice — unified open-data toolkit."""
    if version:
        console.print(f"datasluice {__version__}")
        raise typer.Exit()


app.command(name="scan")(scan)
app.command(name="open")(open)
app.command(name="materialize")(materialize)
app.add_typer(capabilities_app, name="capabilities")
app.add_typer(credentials_app, name="credentials")


if __name__ == "__main__":
    app()
