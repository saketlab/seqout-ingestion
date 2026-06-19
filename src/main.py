import logging
from typing import Literal, Optional

import typer

from cli.backfill import backfill_app
from cli.geocode import geocode_app
from cli.ingest import ingest_app
from cli.sc_enrich import sc_enrich_app
from cli.transfer import transfer_app
from cli.unify import unify_app

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
cli.add_typer(backfill_app, name="backfill")
cli.add_typer(unify_app, name="unify")
cli.add_typer(geocode_app)
cli.add_typer(sc_enrich_app, name="sc_enrich")
cli.add_typer(transfer_app, name="transfer")

if __name__ == "__main__":
    cli()
