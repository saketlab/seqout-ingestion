import asyncio
import datetime
from pathlib import Path

import typer

from cli.utils import (
    _parse_chunk_size,
    _parse_date,
    _parse_timespan,
    _validate_date_range,
)
from ingest.ae import ingest_ae
from ingest.dra import ingest_dra
from ingest.ena import ingest_ena
from ingest.gea import ingest_gea
from ingest.geo import ingest_geo
from ingest.gsa import ingest_gsa
from ingest.gsa_bioproject import ingest_gsa_bioproject
from ingest.gsa_human import ingest_gsa_human
from ingest.sra import ingest_sra

ingest_app = typer.Typer()

_BASE_OUTPUT_DIR_FLAG = typer.Option(..., "--output-dir")
_DB_FILE_FLAG = typer.Option(None, "--db-file")
_NUM_WORKERS_FLAG = typer.Option(4, "--workers")
_DATE_FLAG = typer.Option(
    datetime.datetime.now(tz=datetime.UTC).strftime("%Y-%m-%d"),
    parser=_parse_date,
)
_CHUNK_SIZE_FLAG = typer.Option("20 MB", parser=_parse_chunk_size)
_BATCH_SIZE_FLAG = typer.Option(2000)
_RETRIES_FLAG = typer.Option(10)
_TIMEOUT_FLAG = typer.Option("30 seconds", parser=_parse_timespan)
_MAX_TIMEOUT = typer.Option("2 mins", parser=_parse_timespan)


@ingest_app.command()
@_validate_date_range
def sra(
    base_output_dir: Path = _BASE_OUTPUT_DIR_FLAG,
    db_file: Path | None = _DB_FILE_FLAG,
    start_date: datetime.date = _DATE_FLAG,
    end_date: datetime.date = _DATE_FLAG,
    num_workers: int = _NUM_WORKERS_FLAG,
    chunk_size: int = _CHUNK_SIZE_FLAG,
    write_batch_size: int = typer.Option(50000),
    accessions_ttl: int = typer.Option("2 days", parser=_parse_timespan),
    retries: int = _RETRIES_FLAG,
    timeout: int = _TIMEOUT_FLAG,
    max_timeout: int = _MAX_TIMEOUT,
) -> None:
    ingest_sra(
        base_output_dir,
        db_file,
        start_date,
        end_date,
        num_workers,
        chunk_size,
        write_batch_size,
        accessions_ttl,
        retries,
        timeout,
        max_timeout,
    )


@ingest_app.command()
@_validate_date_range
def dra(
    base_output_dir: Path = _BASE_OUTPUT_DIR_FLAG,
    db_file: Path | None = _DB_FILE_FLAG,
    start_date: datetime.date = _DATE_FLAG,
    end_date: datetime.date = _DATE_FLAG,
    num_workers: int = _NUM_WORKERS_FLAG,
    chunk_size: int = _CHUNK_SIZE_FLAG,
    retries: int = _RETRIES_FLAG,
    timeout: int = _TIMEOUT_FLAG,
    max_timeout: int = _MAX_TIMEOUT,
    livelist_ttl: int = typer.Option("2 days", parser=_parse_timespan),
    novelty_filter: bool = typer.Option(
        False,  # noqa: FBT003
        "--novelty-filter/--no-novelty-filter",
        help=(
            "default: ingest ALL of public DRA, stamping is_only_ddbj=1 on the "
            "studies SRA/ENA have no mirror of. --novelty-filter instead skips "
            "the mirrored ones entirely and ingests only the DDBJ-exclusive set."
        ),
    ),
    sra_db_file: Path | None = typer.Option(None, "--sra-db-file"),
    ena_db_file: Path | None = typer.Option(None, "--ena-db-file"),
    pg_url: str | None = typer.Option(
        None,
        "--pg-url",
        help="check novelty against postgres instead of the sqlite dumps",
    ),
    dry_run: bool = typer.Option(
        False,  # noqa: FBT003
        "--dry-run",
        help="report how many new studies would be ingested, then stop",
    ),
) -> None:
    """Ingest DDBJ DRA (raw reads)."""
    ingest_dra(
        base_output_dir=base_output_dir,
        db_file=db_file,
        start_date=start_date,
        end_date=end_date,
        num_workers=num_workers,
        retries=retries,
        timeout=timeout,
        max_timeout=max_timeout,
        chunk_size=chunk_size,
        sra_db_file=sra_db_file,
        ena_db_file=ena_db_file,
        pg_url=pg_url,
        novelty_filter=novelty_filter,
        livelist_ttl=livelist_ttl,
        dry_run=dry_run,
    )


@ingest_app.command()
@_validate_date_range
def gea(
    base_output_dir: Path = _BASE_OUTPUT_DIR_FLAG,
    db_file: Path | None = _DB_FILE_FLAG,
    start_date: datetime.date = _DATE_FLAG,
    end_date: datetime.date = _DATE_FLAG,
    num_workers: int = _NUM_WORKERS_FLAG,
    retries: int = _RETRIES_FLAG,
    timeout: int = _TIMEOUT_FLAG,
    max_timeout: int = _MAX_TIMEOUT,
) -> None:
    """
    Ingest DDBJ GEA (expression).

    No novelty filter: unlike DRA, no GEA experiment is mirrored into GEO or
    ArrayExpress, so everything public is ingested.
    """
    ingest_gea(
        base_output_dir,
        db_file,
        start_date,
        end_date,
        num_workers,
        retries,
        timeout,
        max_timeout,
    )


@ingest_app.command()
@_validate_date_range
def geo(
    base_output_dir: Path = _BASE_OUTPUT_DIR_FLAG,
    db_file: Path | None = _DB_FILE_FLAG,
    start_date: datetime.date = _DATE_FLAG,
    end_date: datetime.date = _DATE_FLAG,
    num_workers: int = _NUM_WORKERS_FLAG,
    chunk_size: int = _CHUNK_SIZE_FLAG,
    batch_size: int = _BATCH_SIZE_FLAG,
    check_accession_subdirs: bool = typer.Option(default=False),
    series_limit: int | None = typer.Option(None),
    retries: int = _RETRIES_FLAG,
    timeout: int = _TIMEOUT_FLAG,
    max_timeout: int = _MAX_TIMEOUT,
) -> None:
    asyncio.run(
        ingest_geo(
            base_output_dir,
            db_file,
            start_date,
            end_date,
            num_workers,
            chunk_size,
            batch_size,
            check_accession_subdirs,
            series_limit,
            retries,
            timeout,
            max_timeout,
        ),
    )


@ingest_app.command()
@_validate_date_range
def ena(
    base_output_dir: Path = _BASE_OUTPUT_DIR_FLAG,
    db_file: Path | None = _DB_FILE_FLAG,
    start_date: datetime.date = _DATE_FLAG,
    end_date: datetime.date = _DATE_FLAG,
    num_workers: int = _NUM_WORKERS_FLAG,
    use_last_updated: bool = typer.Option(default=False),
    skip_experiments: bool = typer.Option(default=False),
    skip_samples: bool = typer.Option(default=False),
    retries: int = _RETRIES_FLAG,
    timeout: int = _TIMEOUT_FLAG,
    max_timeout: int = _MAX_TIMEOUT,
) -> None:
    ingest_ena(
        base_output_dir,
        db_file,
        start_date,
        end_date,
        num_workers,
        use_last_updated,
        skip_experiments,
        skip_samples,
        retries,
        timeout,
        max_timeout,
    )


@ingest_app.command()
@_validate_date_range
def ae(
    base_output_dir: Path = _BASE_OUTPUT_DIR_FLAG,
    db_file: Path | None = _DB_FILE_FLAG,
    start_date: datetime.date = _DATE_FLAG,
    end_date: datetime.date = _DATE_FLAG,
    num_workers: int = _NUM_WORKERS_FLAG,
    batch_size: int = _BATCH_SIZE_FLAG,
    retries: int = _RETRIES_FLAG,
    timeout: int = _TIMEOUT_FLAG,
    max_timeout: int = _MAX_TIMEOUT,
) -> None:
    ingest_ae(
        base_output_dir,
        db_file,
        start_date,
        end_date,
        num_workers,
        batch_size,
        retries,
        timeout,
        max_timeout,
    )


@ingest_app.command()
@_validate_date_range
def gsa(
    base_output_dir: Path = _BASE_OUTPUT_DIR_FLAG,
    db_file: Path | None = _DB_FILE_FLAG,
    start_date: datetime.date = _DATE_FLAG,
    end_date: datetime.date = _DATE_FLAG,
    num_workers: int = _NUM_WORKERS_FLAG,
    retries: int = _RETRIES_FLAG,
    timeout: int = _TIMEOUT_FLAG,
    max_timeout: int = _MAX_TIMEOUT,
) -> None:
    ingest_gsa(
        base_output_dir,
        db_file,
        start_date,
        end_date,
        num_workers,
        retries,
        timeout,
        max_timeout,
    )


@ingest_app.command()
@_validate_date_range
def gsa_human(
    base_output_dir: Path = _BASE_OUTPUT_DIR_FLAG,
    db_file: Path | None = _DB_FILE_FLAG,
    start_date: datetime.date = _DATE_FLAG,
    end_date: datetime.date = _DATE_FLAG,
    num_workers: int = _NUM_WORKERS_FLAG,
    retries: int = _RETRIES_FLAG,
    timeout: int = _TIMEOUT_FLAG,
    max_timeout: int = _MAX_TIMEOUT,
) -> None:
    ingest_gsa_human(
        base_output_dir,
        db_file,
        start_date,
        end_date,
        num_workers,
        retries,
        timeout,
        max_timeout,
    )


@ingest_app.command()
def gsa_bioproject(
    base_output_dir: Path = _BASE_OUTPUT_DIR_FLAG,
    gsa_db_file: Path | None = typer.Option(None, "--gsa-db-file"),
    gsa_human_db_file: Path | None = typer.Option(None, "--gsa-human-db-file"),
    db_file: Path | None = _DB_FILE_FLAG,
    num_workers: int = _NUM_WORKERS_FLAG,
    retries: int = _RETRIES_FLAG,
    timeout: int = _TIMEOUT_FLAG,
    max_timeout: int = _MAX_TIMEOUT,
    limit: int | None = typer.Option(None, "--limit"),
) -> None:
    ingest_gsa_bioproject(
        base_output_dir,
        gsa_db_file,
        gsa_human_db_file,
        db_file,
        num_workers,
        retries,
        timeout,
        max_timeout,
        limit,
    )
