import logging
from typing import Literal

import typer

from cli.age import age_app
from cli.backfill import backfill_app
from cli.embed import embed_app
from cli.enrich import enrich_app
from cli.gard import gard_app
from cli.geocode import geocode_app
from cli.ingest import ingest_app
from cli.inheritance import inheritance_app
from cli.nord import nord_app
from cli.ontology import ontology_app
from cli.pentimento import pentimento_app
from cli.refresh import refresh_app
from cli.sc_db import sc_db_app
from cli.sc_enrich import sc_enrich_app
from cli.sc_evidence import sc_evidence_app
from cli.transfer import transfer_app
from cli.unify import unify_app

cli = typer.Typer()


@cli.callback()
def main(
    ctx: typer.Context,
    log_file: str | None = typer.Option(
        None,
        help="redirect logs to a file instead",
    ),
    log_level: Literal["debug", "info", "warning", "error"] = typer.Option("info"),
) -> None:
    ctx.ensure_object(dict)

    handlers: list[logging.Handler] = []
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
cli.add_typer(sc_evidence_app, name="sc_evidence")
cli.add_typer(sc_db_app, name="sc_db")
cli.add_typer(pentimento_app, name="pentimento")
cli.add_typer(transfer_app, name="transfer")
cli.add_typer(embed_app, name="embed")
cli.add_typer(enrich_app, name="enrich")
cli.add_typer(age_app, name="age")
cli.add_typer(ontology_app, name="ontology")
cli.add_typer(refresh_app, name="refresh")
cli.add_typer(gard_app, name="gard")
cli.add_typer(nord_app, name="nord")
cli.add_typer(inheritance_app, name="inheritance")

if __name__ == "__main__":
    cli()
