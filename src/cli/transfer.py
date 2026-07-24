from pathlib import Path

import typer

from embed.embed_constants import TransferStep

transfer_app = typer.Typer()


@transfer_app.command()
def postgres(
    pg_url: str = typer.Option(...),
    sra_db_file: Path = typer.Option(...),
    geo_db_file: Path = typer.Option(...),
    ena_db_file: Path = typer.Option(...),
    ae_db_file: Path = typer.Option(...),
    pubmed_metadata_db_file: Path = typer.Option(...),
    unified_metadata_db_file: Path = typer.Option(...),
    download_links_db_file: Path = typer.Option(...),
    read_batch_size: int = typer.Option(default=500),
    num_retries: int = typer.Option(3, "--retries"),
    num_workers: int = typer.Option(4, "--workers"),
    gsa_db_file: Path | None = typer.Option(None),
    gsa_human_db_file: Path | None = typer.Option(None),
    gsa_bioproject_db_file: Path | None = typer.Option(None),
) -> None:
    from transfer.postgres import postgres_transfer

    postgres_transfer(
        pg_url,
        sra_db_file,
        geo_db_file,
        ena_db_file,
        ae_db_file,
        pubmed_metadata_db_file,
        unified_metadata_db_file,
        download_links_db_file,
        read_batch_size,
        num_retries,
        num_workers,
        gsa_db_file=gsa_db_file,
        gsa_human_db_file=gsa_human_db_file,
        gsa_bioproject_db_file=gsa_bioproject_db_file,
    )


@transfer_app.command()
def leiden_swap(
    pg_url: str = typer.Option(...),
    chroma_db_dir: Path = typer.Option(...),
    labels_dir: Path = typer.Option(...),
) -> None:
    """Overwrite EVoC with Leiden in Postgres (remap leiden_l* -> cluster_l*)."""
    from transfer.leiden_swap import leiden_swap as run

    run(pg_url, chroma_db_dir, labels_dir)


@transfer_app.command()
def sample_tsv(
    pg_url: str = typer.Option(...),
) -> None:
    """Fold sample title/description into each study's search tsv (recall)."""
    from transfer.sample_tsv import sample_tsv_transfer

    sample_tsv_transfer(pg_url)


@transfer_app.command()
def pub_tsv(
    pg_url: str = typer.Option(...),
) -> None:
    """Fold linked publication titles into each study's search tsv (recall)."""
    from transfer.pub_tsv import pub_tsv_transfer

    pub_tsv_transfer(pg_url)


@transfer_app.command()
def ddbj(
    pg_url: str = typer.Option(...),
    dra_db_file: Path | None = typer.Option(None),
    gea_db_file: Path | None = typer.Option(None),
    download_links_db_file: Path | None = typer.Option(None),
    read_batch_size: int = typer.Option(default=500),
) -> None:
    """Upsert DDBJ DRA and GEA into the dra_* / gea_* Postgres tables."""
    from transfer.ddbj import ddbj_transfer

    ddbj_transfer(
        pg_url,
        dra_db_file,
        gea_db_file,
        read_batch_size,
        download_links_db_file=download_links_db_file,
    )


@transfer_app.command()
def age(
    pg_url: str = typer.Option(...),
    age_norm_db_file: Path = typer.Option(...),
    read_batch_size: int = typer.Option(default=5000),
) -> None:
    """Annotate ontology_samples_v4 with normalized age columns (age_days, ...)."""
    from transfer.age import age_transfer

    age_transfer(pg_url, age_norm_db_file, read_batch_size)


@transfer_app.command()
def gsa_search_tsv(
    pg_url: str = typer.Option(...),
) -> None:
    """Fold BioProject title/summary, organism, and sample text into the GSA
    search tsv (recall). Run after the gsa_* tables are loaded."""
    from transfer.gsa_search import gsa_search_tsv_transfer

    gsa_search_tsv_transfer(pg_url)


@transfer_app.command()
def parquet(
    output_dir: Path = typer.Option(...),
    sra_db_file: Path = typer.Option(...),
    geo_db_file: Path = typer.Option(...),
    ena_db_file: Path = typer.Option(...),
    ae_db_file: Path = typer.Option(...),
    pubmed_metadata_db_file: Path = typer.Option(...),
    unified_metadata_db_file: Path = typer.Option(...),
    download_links_db_file: Path = typer.Option(...),
    num_workers: int = typer.Option(4, "--workers"),
    batch_size: int = typer.Option(default=500),
) -> None:
    from transfer.parquet import parquet_transfer

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


@transfer_app.command()
def embed(
    pg_url: str = typer.Option(...),
    chroma_db_dir: Path = typer.Option(...),
    neighbors_db_file: Path = typer.Option(...),
    labels_dir: Path = typer.Option(...),
    start_from: TransferStep = typer.Option(
        default=TransferStep.coords_neighbors,
        help="resume the transfer from this sub-step onward "
        "(coords-neighbors -> coords2d -> labels)",
    ),
) -> None:
    from transfer.embed import embed_transfer

    embed_transfer(
        pg_url,
        chroma_db_dir,
        neighbors_db_file,
        labels_dir,
        start_from=start_from,
    )
