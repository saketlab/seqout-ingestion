from pathlib import Path

import typer

from cli.utils import _parse_chunk_size, _parse_timespan
from sc_enrich.classify import sc_classify
from sc_enrich.counter import sc_counter

sc_enrich_app = typer.Typer()


@sc_enrich_app.command()
def classify(
    unified_metadata_db_file: Path = typer.Option(...),  # noqa: B008
    batch_size: int = typer.Option(5000),
    is_dry_run: bool = typer.Option(default=False),
) -> None:
    sc_classify(unified_metadata_db_file, batch_size, is_dry_run)


@sc_enrich_app.command()
def counter(
    base_output_dir: Path = typer.Option(..., "--output-dir"),  # noqa: B008
    unified_metadata_db_file: Path = typer.Option(...),  # noqa: B008
    geo_db_file: Path = typer.Option(...),  # noqa: B008
    ae_db_file: Path = typer.Option(...),  # noqa: B008
    num_workers: int = typer.Option(4, "--workers"),
    chunk_size: int = typer.Option("20 MB", parser=_parse_chunk_size),
    batch_size: int = typer.Option(100),
    retries: int = typer.Option(10),
    timeout: int = typer.Option("30 seconds", parser=_parse_timespan),
    max_timeout: int = typer.Option("2 mins", parser=_parse_timespan),
    check: bool = typer.Option(
        default=False, help="stop at first file with count > 0 per accession"
    ),
    save_data: bool = typer.Option(
        default=False, help="keep downloaded files after processing"
    ),
) -> None:
    sc_counter(
        base_output_dir,
        unified_metadata_db_file,
        geo_db_file,
        ae_db_file,
        num_workers,
        chunk_size,
        retries,
        timeout,
        max_timeout,
        batch_size,
        check=check,
        save_data=save_data,
    )
