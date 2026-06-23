from pathlib import Path

import typer

from transfer.pg import pg_transfer

transfer_app = typer.Typer()


@transfer_app.command()
def pg(
    pg_url: str = typer.Option(...),
    sra_db_file: Path = typer.Option(...),
    geo_db_file: Path = typer.Option(...),
    unified_metadata_db_file: Path = typer.Option(...),
    download_links_db_file: Path = typer.Option(...),
    num_workers: int = typer.Option(4, "--workers"),
    batch_size: int = typer.Option(default=500),
):
    pg_transfer(
        pg_url,
        sra_db_file,
        geo_db_file,
        unified_metadata_db_file,
        download_links_db_file,
        num_workers,
        batch_size,
    )
