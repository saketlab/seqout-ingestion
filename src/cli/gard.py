import logging
from pathlib import Path

import typer

gard_app = typer.Typer()
logger = logging.getLogger(__name__)


@gard_app.command()
def fetch(
    output_db_file: Path = typer.Option(..., help="gard.db -- one row per disease"),
    ontology_dir: Path = typer.Option(
        ...,
        help="ontology stage root; mondo.json is read from (or downloaded into) raw/",
    ),
    timeout: int = typer.Option(120),
    num_retries: int = typer.Option(5),
) -> None:
    """Download GARD's catalogue and resolve it onto MONDO by cross-reference."""
    from rare_disease.gard import fetch_gard

    fetch_gard(output_db_file, ontology_dir, timeout, num_retries)


@gard_app.command()
def inheritance(
    gard_db_file: Path = typer.Option(..., help="gard.db from `gard fetch`"),
    workers: int = typer.Option(
        8, help="kept low: 6,265 requests to a public NIH site"
    ),
    timeout: int = typer.Option(60),
    num_retries: int = typer.Option(3),
    refresh: bool = typer.Option(
        False,  # noqa: FBT003 - typer flag default
        "--refresh",
        help="re-scrape rows that already carry a value",
    ),
) -> None:
    """Scrape GARD's mode of inheritance, which its downloadable catalogue omits."""
    from rare_disease.gard_scrape import scrape_inheritance

    scrape_inheritance(gard_db_file, workers, timeout, num_retries, refresh=refresh)
