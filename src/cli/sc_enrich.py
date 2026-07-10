from pathlib import Path

import typer

from sc_enrich.classify import sc_classify
from sc_enrich.counter import sc_counter

sc_enrich_app = typer.Typer()


@sc_enrich_app.command()
def classify(
    unified_metadata_db_file: Path = typer.Option(...),
    batch_size: int = typer.Option(5000),
    is_dry_run: bool = typer.Option(default=False),
) -> None:
    sc_classify(unified_metadata_db_file, batch_size, is_dry_run)


@sc_enrich_app.command()
def counter(
    output_dir: Path = typer.Option(...),
    unified_metadata_db_file: Path = typer.Option(...),
    geo_db_file: Path = typer.Option(...),
    ae_db_file: Path = typer.Option(...),
    workers: int = typer.Option(8),
    chunk_size: int = typer.Option(10 * 1024 * 1024),
    retries: int = typer.Option(20),
    timeout: int = typer.Option(30),
    max_timeout: int = typer.Option(120),
    batch_size: int = typer.Option(5000),
    count_exact: bool = typer.Option(default=True, help="store exact cell counts"),
    check_all: bool = typer.Option(
        default=False, help="check all records regardless of sc_count_last_checked"
    ),
) -> None:
    sc_counter(
        output_dir,
        unified_metadata_db_file,
        geo_db_file,
        ae_db_file,
        workers,
        chunk_size,
        retries,
        timeout,
        max_timeout,
        batch_size,
        count_exact=count_exact,
        check_all=check_all,
    )
