from pathlib import Path
from typing import Optional

import typer

from geocode.geocode_missing import geocode_missing

geocode_app = typer.Typer()


@geocode_app.command()
def geocode(
    db_file: Path = typer.Option(..., "--db-file"),
    limit: Optional[int] = typer.Option(None, "--limit", "-n"),
    cache_file: Optional[Path] = typer.Option(None, "--cache-file"),
    save_every: int = typer.Option(50, "--save-every"),
    rate_limit: float = typer.Option(1.0, "--rate-limit"),
    is_dry_run: bool = typer.Option(False, "--dry-run"),
):
    geocode_missing(
        db_file=db_file,
        limit=limit,
        cache_file=cache_file,
        save_every=save_every,
        rate_limit=rate_limit,
        is_dry_run=is_dry_run,
    )
