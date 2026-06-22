from pathlib import Path
from typing import Optional

import typer

from transfer.pg import pg_transfer

transfer_app = typer.Typer()


@transfer_app.command()
def pg(
    pg_url: str = typer.Option(...),
    geo_db_file: Optional[Path] = typer.Option(None),
    num_workers: int = typer.Option(4),
    batch_size: int = typer.Option(default=500),
):
    pg_transfer(pg_url, geo_db_file, num_workers, batch_size)
