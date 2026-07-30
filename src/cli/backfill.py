from pathlib import Path
from typing import Literal

import typer

from backfill.citation_pmids import backfill_citation_pmids
from backfill.download_links import backfill_download_links
from backfill.dra_download_links import backfill_dra_download_links
from backfill.pubmed_ids import backfill_pubmed_ids
from backfill.pubmed_meta import backfill_pubmed_metadata
from cli.utils import _parse_chunk_size, _parse_timespan

backfill_app = typer.Typer()

_NUM_WORKERS_FLAG = typer.Option(8, "--workers")
_BATCH_SIZE_FLAG = typer.Option(100)


@backfill_app.command()
def pm_ids(
    geo_db_file: Path | None = typer.Option(None),
    sra_db_file: Path | None = typer.Option(None),
    ena_db_file: Path | None = typer.Option(None),
    ae_db_file: Path | None = typer.Option(None),
    dra_db_file: Path | None = typer.Option(None),
    gea_db_file: Path | None = typer.Option(None),
    gsa_db_file: Path | None = typer.Option(None),
    gsa_human_db_file: Path | None = typer.Option(None),
    ncbi_keys_file: Path = typer.Option(...),
    num_workers: int = _NUM_WORKERS_FLAG,
    batch_size: int = _BATCH_SIZE_FLAG,
    is_dry_run: bool = typer.Option(default=False),
    retries: int = typer.Option(20),
    timeout: int = typer.Option("30 seconds", parser=_parse_timespan),
    process_all: bool = typer.Option(default=False),
) -> None:
    backfill_pubmed_ids(
        geo_db_file,
        sra_db_file,
        ena_db_file,
        ae_db_file,
        dra_db_file,
        gea_db_file,
        gsa_db_file,
        gsa_human_db_file,
        ncbi_keys_file,
        num_workers,
        batch_size,
        is_dry_run,
        retries,
        timeout,
        process_all,
    )


@backfill_app.command()
def pm_meta(
    base_output_dir: Path = typer.Option(..., "--output-dir"),
    geo_db_file: Path | None = typer.Option(None),
    sra_db_file: Path | None = typer.Option(None),
    ena_db_file: Path | None = typer.Option(None),
    ae_db_file: Path | None = typer.Option(None),
    dra_db_file: Path | None = typer.Option(None),
    gea_db_file: Path | None = typer.Option(None),
    gsa_db_file: Path | None = typer.Option(None),
    gsa_human_db_file: Path | None = typer.Option(None),
    db_file: Path | None = typer.Option(None),
    ncbi_keys_file: Path = typer.Option(...),
    openalex_keys_file: Path = typer.Option(...),
    num_workers: int = _NUM_WORKERS_FLAG,
    batch_size: int = _BATCH_SIZE_FLAG,
    fill_metrics: bool = typer.Option(default=False),
    update_metrics: bool = typer.Option(default=False),
    skip_journal_metrics: bool = typer.Option(default=False),
    update_journal_metrics: bool = typer.Option(default=False),
    refresh_all_journals_metrics: bool = typer.Option(default=False),
    retries: int = typer.Option(20),
    timeout: int = typer.Option("30 seconds", parser=_parse_timespan),
    is_dry_run: bool = typer.Option(default=False),
) -> None:
    backfill_pubmed_metadata(
        base_output_dir,
        geo_db_file,
        sra_db_file,
        ena_db_file,
        ae_db_file,
        dra_db_file,
        gea_db_file,
        gsa_db_file,
        gsa_human_db_file,
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
        retries,
        timeout,
        is_dry_run,
    )


@backfill_app.command()
def citation_pmids(
    geo_db_file: Path = typer.Option(..., "--geo-db-file"),
    ncbi_keys_file: Path = typer.Option(...),
    num_workers: int = _NUM_WORKERS_FLAG,
    batch_size: int = _BATCH_SIZE_FLAG,
    is_dry_run: bool = typer.Option(default=False),
    retries: int = typer.Option(20),
    timeout: int = typer.Option("30 seconds", parser=_parse_timespan),
    process_all: bool = typer.Option(default=False),
) -> None:
    """Resolve GEO <Citation> DOIs to PMIDs into geo_series.pubmed_id_citation."""
    backfill_citation_pmids(
        geo_db_file,
        ncbi_keys_file,
        num_workers,
        batch_size,
        is_dry_run,
        retries,
        timeout,
        process_all,
    )


@backfill_app.command()
def download_links(
    base_output_dir: Path = typer.Option(..., "--output-dir"),
    source: Literal["ncbi", "ena", "both"] = typer.Option(...),
    sra_db_file: Path | None = typer.Option(None),
    ena_db_file: Path | None = typer.Option(None),
    dra_db_file: Path | None = typer.Option(None),
    ncbi_keys_file: Path | None = typer.Option(None),
    num_workers: int = _NUM_WORKERS_FLAG,
    is_dry_run: bool = typer.Option(default=False),
    batch_size: int = typer.Option(2000),
    retries: int = typer.Option(20),
    timeout: int = typer.Option("30 seconds", parser=_parse_timespan),
    limit: int | None = typer.Option(None),
) -> None:
    backfill_download_links(
        base_output_dir,
        sra_db_file,
        ena_db_file,
        dra_db_file,
        ncbi_keys_file,
        num_workers,
        is_dry_run,
        batch_size,
        source,
        retries,
        timeout,
        limit,
    )


@backfill_app.command()
def dra_download_links(
    base_output_dir: Path = typer.Option(..., "--output-dir"),
    dra_db_file: Path = typer.Option(..., "--dra-db-file"),
    download_links_db_file: Path = typer.Option(..., "--download-links-db-file"),
    num_workers: int = _NUM_WORKERS_FLAG,
    chunk_size: int = typer.Option("20 MB", parser=_parse_chunk_size),
    retries: int = typer.Option(20),
    timeout: int = typer.Option("30 seconds", parser=_parse_timespan),
    max_timeout: int = typer.Option("2 mins", parser=_parse_timespan),
) -> None:
    """FASTQ links for DRA runs, from DDBJ's fastqlist manifest."""
    backfill_dra_download_links(
        base_output_dir,
        dra_db_file,
        download_links_db_file,
        num_workers,
        chunk_size,
        retries,
        timeout,
        max_timeout,
    )
