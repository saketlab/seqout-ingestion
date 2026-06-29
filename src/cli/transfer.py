from pathlib import Path

import typer

from transfer.parquet import parquet_transfer
from transfer.postgres import postgres_transfer

transfer_app = typer.Typer()


@transfer_app.command()
def postgres(
    pg_url: str = typer.Option(...),
    sra_db_file: Path = typer.Option(...),  # noqa: B008
    geo_db_file: Path = typer.Option(...),  # noqa: B008
    ena_db_file: Path = typer.Option(...),  # noqa: B008
    ae_db_file: Path = typer.Option(...),  # noqa: B008
    pubmed_metadata_db_file: Path = typer.Option(...),  # noqa: B008
    unified_metadata_db_file: Path = typer.Option(...),  # noqa: B008
    download_links_db_file: Path = typer.Option(...),  # noqa: B008
    num_workers: int = typer.Option(4, "--workers"),
    batch_size: int = typer.Option(default=500),
) -> None:
    postgres_transfer(
        pg_url,
        sra_db_file,
        geo_db_file,
        ena_db_file,
        ae_db_file,
        pubmed_metadata_db_file,
        unified_metadata_db_file,
        download_links_db_file,
        num_workers,
        batch_size,
    )


@transfer_app.command()
def parquet(
    output_dir: Path = typer.Option(...),  # noqa: B008
    sra_db_file: Path = typer.Option(...),  # noqa: B008
    geo_db_file: Path = typer.Option(...),  # noqa: B008
    ena_db_file: Path = typer.Option(...),  # noqa: B008
    ae_db_file: Path = typer.Option(...),  # noqa: B008
    pubmed_metadata_db_file: Path = typer.Option(...),  # noqa: B008
    unified_metadata_db_file: Path = typer.Option(...),  # noqa: B008
    download_links_db_file: Path = typer.Option(...),  # noqa: B008
    num_workers: int = typer.Option(4, "--workers"),
    batch_size: int = typer.Option(default=500),
) -> None:
    parquet_transfer(
        output_dir,
        sra_db_file,
        geo_db_file,
        ena_db_file,
        ae_db_file,
        pubmed_metadata_db_file,
        unified_metadata_db_file,
        download_links_db_file,
        num_workers,
        batch_size,
    )
