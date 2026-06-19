from pathlib import Path
from typing import Optional

import typer

from transfer.pg import pg_transfer

transfer_app = typer.Typer()


@transfer_app.command()
def pg(
    sra_db_file: Optional[Path] = typer.Option(None),
    geo_db_file: Optional[Path] = typer.Option(None),
    ena_db_file: Optional[Path] = typer.Option(None),
    ae_db_file: Optional[Path] = typer.Option(None),
    pg_url: str = typer.Option(...),
    num_workers: int = typer.Option(..., "--workers"),
    batch_size: int = typer.Option(500),
):
    pg_transfer(
        sra_db_file,
        geo_db_file,
        ena_db_file,
        ae_db_file,
        pg_url,
        num_workers,
        batch_size,
    )
