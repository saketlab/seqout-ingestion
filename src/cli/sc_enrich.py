from pathlib import Path

import typer

from sc_enrich.fastcounter import sc_fastcounter
from sc_enrich.sc_regex_enricher import sc_regex_enricher

sc_enrich_app = typer.Typer()


@sc_enrich_app.command()
def regex(
    unified_metadata_db_file: Path = typer.Option(...),
    batch_size: int = typer.Option(5000),
    is_dry_run: bool = typer.Option(False),
):
    sc_regex_enricher(unified_metadata_db_file, batch_size, is_dry_run)


@sc_enrich_app.command()
def fastcounter(
    unified_metadata_db_file: Path = typer.Option(...),
    geo_db_file: Path = typer.Option(...),
    num_workers: int = typer.Option(4, "--workers"),
):
    sc_fastcounter(unified_metadata_db_file, geo_db_file, num_workers)
