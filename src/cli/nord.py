import logging
from pathlib import Path

import typer

nord_app = typer.Typer()
logger = logging.getLogger(__name__)


@nord_app.command()
def fetch(
    output_csv_file: Path = typer.Option(..., help="NORD catalogue csv"),
    ontology_dir: Path = typer.Option(
        ...,
        help="ontology stage root; mondo.json is read from (or downloaded into) raw/",
    ),
    timeout: int = typer.Option(60),
    num_retries: int = typer.Option(5),
) -> None:
    """Download NORD's catalogue and resolve it onto MONDO by name."""
    from rare_disease.nord_fetch import fetch_nord

    fetch_nord(output_csv_file, ontology_dir, timeout=timeout, num_retries=num_retries)


@nord_app.command()
def load(
    csv_file: Path = typer.Option(..., help="NORD catalogue csv from `nord fetch`"),
    output_db_file: Path = typer.Option(..., help="nord.db -- one row per entry"),
) -> None:
    """Stage NORD's catalogue into SQLite. Re-run `nord fetch` to refresh it."""
    from rare_disease.nord_load import load_nord

    load_nord(csv_file, output_db_file)
