from pathlib import Path

import typer

from sc_enrich.sc_regex_enricher import sc_regex_enricher

sc_enrich_app = typer.Typer()


@sc_enrich_app.command()
def regex(
    unified_metadata_db_file: Path = typer.Option(...),
    batch_size: int = typer.Option(5000),
    is_dry_run: bool = typer.Option(False),
):
    sc_regex_enricher(unified_metadata_db_file, batch_size, is_dry_run)
