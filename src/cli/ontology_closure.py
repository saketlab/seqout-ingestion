from pathlib import Path

import typer

ontology_closure_app = typer.Typer(
    help="Full-DAG ontology closure (mondo/uberon/cl/efo) behind resolve_ontology_term "
    "and the Beacon includeDescendantTerms filter. See transfer ontology-closure "
    "for the Postgres load."
)


@ontology_closure_app.command()
def build(
    output_db_file: Path = typer.Option(
        ..., help="ontology_closure.db -- closure + term-label staging"
    ),
) -> None:
    """Parse the ontology JSONs and compute the full ancestor/descendant closure."""
    from ontology_closure.build import build as build_closure

    build_closure(output_db_file)


@ontology_closure_app.command("verify-sqlite")
def verify_sqlite(
    db_file: Path = typer.Option(..., help="ontology_closure.db from `build`"),
) -> None:
    """Sanity-check the staged closure before transferring it."""
    from ontology_closure.build import verify_sqlite as verify

    verify(db_file)
