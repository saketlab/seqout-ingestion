import logging
from typing import Literal, Optional

import typer

from cli.ingest import ingest_app

cli = typer.Typer()


@cli.callback()
def main(
    ctx: typer.Context,
    log_file: Optional[str] = typer.Option(
        None, help="redirect logs to a file instead"
    ),
    log_level: Literal["debug", "info", "warning", "error"] = typer.Option("info"),
):
    ctx.ensure_object(dict)

    handlers = []
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    else:
        handlers.append(logging.StreamHandler())

    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=handlers,
    )


cli.add_typer(ingest_app, name="ingest")

if __name__ == "__main__":
    cli()
