from pathlib import Path

import typer

from cli.utils import _parse_timespan
from geocode.geocode import geocode_missing

geocode_app = typer.Typer()


@geocode_app.command()
def geocode(
    unified_metadata_db_file: Path = typer.Option(...),  # noqa: B008
    geo_db_file: Path = typer.Option(...),  # noqa: B008
    ror_data_file: Path = typer.Option(None),  # noqa: B008
    cache_file: Path | None = typer.Option(None),  # noqa: B008
    save_every: int = typer.Option(50),
    rate_limit: int = typer.Option(
        "3 seconds",
        parser=_parse_timespan,
    ),
    retries: int = typer.Option(20),
    timeout: int = typer.Option("30 seconds", parser=_parse_timespan),
    is_dry_run: bool = typer.Option(default=False),
    mode: str = typer.Option("missing"),
) -> None:
    geocode_missing(
        unified_metadata_db_file,
        geo_db_file,
        cache_file,
        save_every,
        rate_limit,
        retries,
        timeout,
        is_dry_run,
        ror_data_file,
        mode,
    )
