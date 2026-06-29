from pathlib import Path

import typer

from cli.utils import _parse_timespan
from geocode.geocode_missing import geocode_missing

geocode_app = typer.Typer()


@geocode_app.command()
def geocode(
    unified_metadata_db_file: Path = typer.Option(...),  # noqa: B008
    geo_db_file: Path = typer.Option(...),  # noqa: B008
    limit: int | None = typer.Option(None, "--limit", "-n"),
    cache_file: Path | None = typer.Option(None, "--cache-file"),  # noqa: B008
    save_every: int = typer.Option(50, "--save-every"),
    rate_limit: float = typer.Option(1.0, "--rate-limit", parser=_parse_timespan),
    retries: int = typer.Option(20, "--retries"),
    is_dry_run: bool = typer.Option(default=False),
) -> None:
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
