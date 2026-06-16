from pathlib import Path

import typer

from cli.utils import _parse_chunk_size, _parse_timespan
from sc_enrich.fastcounter import sc_fastcounter
from sc_enrich.regex import sc_regex_enricher

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
    base_output_dir: Path = typer.Option(..., "--output-dir"),
    unified_metadata_db_file: Path = typer.Option(...),
    geo_db_file: Path = typer.Option(...),
    num_workers: int = typer.Option(4, "--workers"),
    chunk_size: int = typer.Option("20 MB", parser=_parse_chunk_size),
    retries: int = typer.Option(10),
    timeout: int = typer.Option("30 seconds", parser=_parse_timespan),
    max_timeout: int = typer.Option("2 mins", parser=_parse_timespan),
):
    sc_fastcounter(
        base_output_dir,
        unified_metadata_db_file,
        geo_db_file,
        num_workers,
        chunk_size,
        retries,
        timeout,
        max_timeout,
    )
