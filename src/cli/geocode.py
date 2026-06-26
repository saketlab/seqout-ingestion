from pathlib import Path
from typing import Optional

import typer

from cli.utils import _parse_timespan
from geocode.geocode_missing import geocode_missing

geocode_app = typer.Typer()


@geocode_app.command()
def geocode(
    unified_metadata_db_file: Path = typer.Option(...),
    geo_db_file: Path = typer.Option(...),
    limit: Optional[int] = typer.Option(None, "--limit", "-n"),
    cache_file: Optional[Path] = typer.Option(None, "--cache-file"),
    save_every: int = typer.Option(50, "--save-every"),
    rate_limit: float = typer.Option(1.0, "--rate-limit", parser=_parse_timespan),
    retries: int = typer.Option(20, "--retries"),
    is_dry_run: bool = typer.Option(False, "--dry-run"),
):
    geocode_missing(
        unified_metadata_db_file,
        geo_db_file,
        cache_file,
        save_every,
        rate_limit,
        retries,
        limit,
        is_dry_run,
    )
