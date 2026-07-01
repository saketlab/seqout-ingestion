from pathlib import Path
from typing import Literal

import typer

from backfill.download_links import backfill_download_links
from backfill.pubmed_ids import backfill_pubmed_ids
from backfill.pubmed_meta import backfill_pubmed_metadata
from cli.utils import _parse_timespan

backfill_app = typer.Typer()

_NUM_WORKERS_FLAG = typer.Option(8, "--workers")
_BATCH_SIZE_FLAG = typer.Option(100)


@backfill_app.command()
def pm_ids(
    geo_db_file: Path | None = typer.Option(None),  # noqa: B008
    sra_db_file: Path | None = typer.Option(None),  # noqa: B008
    ena_db_file: Path | None = typer.Option(None),  # noqa: B008
    ae_db_file: Path | None = typer.Option(None),  # noqa: B008
    ncbi_keys_file: Path = typer.Option(...),  # noqa: B008
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
    base_output_dir: Path = typer.Option(..., "--output-dir"),  # noqa: B008
    geo_db_file: Path | None = typer.Option(None),  # noqa: B008
    sra_db_file: Path | None = typer.Option(None),  # noqa: B008
    ena_db_file: Path | None = typer.Option(None),  # noqa: B008
    ae_db_file: Path | None = typer.Option(None),  # noqa: B008
    db_file: Path | None = typer.Option(None),  # noqa: B008
    ncbi_keys_file: Path = typer.Option(...),  # noqa: B008
    openalex_keys_file: Path = typer.Option(...),  # noqa: B008
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
def download_links(
    base_output_dir: Path = typer.Option(..., "--output-dir"),  # noqa: B008
    source: Literal["ncbi", "ena"] = typer.Option(...),
    sra_db_file: Path | None = typer.Option(None),  # noqa: B008
    ena_db_file: Path | None = typer.Option(None),  # noqa: B008
    ncbi_keys_file: Path | None = typer.Option(None),  # noqa: B008
    num_workers: int = _NUM_WORKERS_FLAG,
    is_dry_run: bool = typer.Option(default=False),
    retries: int = typer.Option(20),
    timeout: int = typer.Option("30 seconds", parser=_parse_timespan),
    limit: int | None = typer.Option(None),
) -> None:
    backfill_download_links(
        base_output_dir,
        sra_db_file,
        ena_db_file,
        ncbi_keys_file,
        num_workers,
        is_dry_run,
        source,
        retries,
        timeout,
        limit,
    )
