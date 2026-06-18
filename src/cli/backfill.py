from pathlib import Path
from typing import Optional

import typer

from backfill.download_links import backfill_download_links
from backfill.pubmed_ids import backfill_pubmed_ids
from backfill.pubmed_meta import backfill_pubmed_meta

backfill_app = typer.Typer()

_NUM_WORKERS_FLAG = typer.Option(8, "--workers", "-w")
_SOURCE_FLAG = typer.Option("ena", "--source", help="source: ena or ncbi")
_BATCH_SIZE_FLAG = typer.Option(100)


@backfill_app.command()
def pm_ids(
    geo_db_file: Optional[Path] = typer.Option(None),
    sra_db_file: Optional[Path] = typer.Option(None),
    ena_db_file: Optional[Path] = typer.Option(None),
    ae_db_file: Optional[Path] = typer.Option(None),
    ncbi_keys_file: Path = typer.Option(...),
    num_workers: int = _NUM_WORKERS_FLAG,
    batch_size: int = _BATCH_SIZE_FLAG,
    is_dry_run: bool = typer.Option(False),
    process_all: bool = typer.Option(False),
):
    backfill_pubmed_ids(
        geo_db_file,
        sra_db_file,
        ena_db_file,
        ae_db_file,
        ncbi_keys_file,
        num_workers,
        batch_size,
        is_dry_run,
        process_all,
    )


@backfill_app.command()
def pm_meta(
    base_output_dir: Path = typer.Option(..., "--output-dir"),
    geo_db_file: Optional[Path] = typer.Option(None),
    sra_db_file: Optional[Path] = typer.Option(None),
    ena_db_file: Optional[Path] = typer.Option(None),
    ae_db_file: Optional[Path] = typer.Option(None),
    db_file: Optional[Path] = typer.Option(None),
    ncbi_keys_file: Path = typer.Option(...),
    openalex_keys_file: Path = typer.Option(...),
    num_workers: int = _NUM_WORKERS_FLAG,
    batch_size: int = _BATCH_SIZE_FLAG,
    fill_metrics: bool = typer.Option(False),
    update_metrics: bool = typer.Option(False),
    skip_journal_metrics: bool = typer.Option(False),
    update_journal_metrics: bool = typer.Option(False),
    refresh_all_journals_metrics: bool = typer.Option(False),
    is_dry_run: bool = typer.Option(False),
):
    backfill_pubmed_meta(
        base_output_dir,
        geo_db_file,
        sra_db_file,
        ena_db_file,
        ae_db_file,
        db_file,
        ncbi_keys_file,
        openalex_keys_file,
        num_workers,
        batch_size,
        fill_metrics,
        update_metrics,
        skip_journal_metrics,
        update_journal_metrics,
        refresh_all_journals_metrics,
        is_dry_run,
    )


@backfill_app.command()
def download_links(
    output_dir: Path = typer.Option(..., "--output-dir"),
    sra_db_file: Optional[Path] = typer.Option(None),
    ena_db_file: Optional[Path] = typer.Option(None),
    ncbi_keys_file: Optional[Path] = typer.Option(None),
    num_workers: int = _NUM_WORKERS_FLAG,
    is_dry_run: bool = typer.Option(False),
    source: str = _SOURCE_FLAG,
    limit: Optional[int] = typer.Option(None),
):
    output_db_file = output_dir / "download_links" / "download_links.db"
    output_db_file.parent.mkdir(parents=True, exist_ok=True)
    backfill_download_links(
        sra_db_file,
        ena_db_file,
        output_db_file,
        ncbi_keys_file,
        num_workers,
        is_dry_run,
        source,
        limit,
    )
