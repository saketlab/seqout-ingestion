from pathlib import Path

import typer

from sc_enrich.classify import sc_classify
from sc_enrich.deep.yuj_setup import yuj_setup

sc_enrich_app = typer.Typer()


@sc_enrich_app.command()
def classify(
    unified_metadata_db_file: Path = typer.Option(...),
    batch_size: int = typer.Option(5000),
    is_dry_run: bool = typer.Option(default=False),
) -> None:
    sc_classify(unified_metadata_db_file, batch_size, is_dry_run)


@sc_enrich_app.command("counter-setup")
def counter_setup(
    unified_metadata_db_file: Path = typer.Option(...),
    geo_db_file: Path = typer.Option(...),
    ae_db_file: Path = typer.Option(...),
    output_dir: Path = typer.Option(...),
    batch_size: int = typer.Option(500),
    check_all: bool = typer.Option(
        default=False, help="check all records regardless of sc_count_last_checked"
    ),
    accession_list_file: Path = typer.Option(
        None,
        help="Count exactly these accessions instead of the is_single_cell=1 set. "
        "Needed for studies the FTS classifier never flagged: the default universe "
        "IS that flag set, so anything outside it can never be reached.",
    ),
) -> None:
    yuj_setup(
        unified_metadata_db_file,
        geo_db_file,
        ae_db_file,
        output_dir,
        batch_size,
        check_all=check_all,
        accession_list_file=accession_list_file,
    )
