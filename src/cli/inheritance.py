import logging
from pathlib import Path

import typer

inheritance_app = typer.Typer()
logger = logging.getLogger(__name__)


@inheritance_app.command()
def fetch(
    output_db_file: Path = typer.Option(
        ..., help="inheritance.db -- one row per MONDO term"
    ),
    ontology_dir: Path = typer.Option(
        ...,
        help="ontology stage root; mondo.json is read from (or downloaded into) raw/",
    ),
    timeout: int = typer.Option(120),
    num_retries: int = typer.Option(5),
    hpoa_url: str = typer.Option(
        "",
        help="pin a specific phenotype.hpoa release; default is HPO's 'latest'",
    ),
) -> None:
    """Fetch HPO's mode-of-inheritance annotations and map them onto MONDO."""
    from rare_disease.inheritance import HPOA_URL, fetch_inheritance

    fetch_inheritance(
        output_db_file, ontology_dir, timeout, num_retries, hpoa_url or HPOA_URL
    )
