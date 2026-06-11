import datetime
from pathlib import Path
from typing import Optional

import typer

from cli.utils import (
    _parse_chunk_size,
    _parse_date,
    _parse_timespan,
    _validate_date_range,
)
from ingest.ae import ingest_ae
from ingest.ena import ingest_ena
from ingest.geo import ingest_geo
from ingest.sra import ingest_sra

ingest_app = typer.Typer()

_BASE_OUTPUT_DIR_FLAG = typer.Option(..., "--output-dir")
_DB_FILE_FLAG = typer.Option(None, "--db-file")
_NUM_WORKERS_FLAG = typer.Option(4, "--workers", "-w")
_DATE_FLAG = typer.Option(
    datetime.datetime.now().strftime("%Y-%m-%d"),
    parser=_parse_date,
)
_CHUNK_SIZE_FLAG = typer.Option("20 MB", parser=_parse_chunk_size)
_BATCH_SIZE_FLAG = typer.Option(2000)
_RETRIES_FLAG = typer.Option(10)
_TIMEOUT_FLAG = typer.Option("30 seconds", parser=_parse_timespan)
_MAX_TIMEOUT = typer.Option("2 mins", parser=_parse_timespan)


@ingest_app.command()
@_validate_date_range
def geo(
    base_output_dir: Path = _BASE_OUTPUT_DIR_FLAG,
    db_file: Optional[Path] = _DB_FILE_FLAG,
    num_workers: int = _NUM_WORKERS_FLAG,
    chunk_size: int = _CHUNK_SIZE_FLAG,
    batch_size: int = _BATCH_SIZE_FLAG,
    series_pickle: Optional[Path] = typer.Option(None),
    series_limit: Optional[int] = typer.Option(None),
    retries: int = _RETRIES_FLAG,
    timeout: int = _TIMEOUT_FLAG,
    max_timeout: int = _MAX_TIMEOUT,
):
    ingest_geo(
        base_output_dir,
        db_file,
        num_workers,
        chunk_size,
        batch_size,
        series_pickle,
        series_limit,
        retries,
        timeout,
        max_timeout,
    )


@ingest_app.command()
@_validate_date_range
def sra(
    base_output_dir: Path = _BASE_OUTPUT_DIR_FLAG,
    db_file: Optional[Path] = _DB_FILE_FLAG,
    start_date: datetime.date = _DATE_FLAG,
    end_date: datetime.date = _DATE_FLAG,
    num_workers: int = _NUM_WORKERS_FLAG,
    chunk_size: int = _CHUNK_SIZE_FLAG,
    read_batch_size: int = typer.Option(500),
    write_batch_size: int = typer.Option(2000),
    accessions_ttl: int = typer.Option("2 days", parser=_parse_timespan),
    retries: int = _RETRIES_FLAG,
    timeout: int = _TIMEOUT_FLAG,
    max_timeout: int = _MAX_TIMEOUT,
):
    ingest_sra(
        base_output_dir,
        db_file,
        start_date,
        end_date,
        num_workers,
        chunk_size,
        read_batch_size,
        write_batch_size,
        accessions_ttl,
        retries,
        timeout,
        max_timeout,
    )


@ingest_app.command()
@_validate_date_range
def ena(
    base_output_dir: Path = _BASE_OUTPUT_DIR_FLAG,
    db_file: Optional[Path] = _DB_FILE_FLAG,
    start_date: datetime.date = _DATE_FLAG,
    end_date: datetime.date = _DATE_FLAG,
    num_workers: int = _NUM_WORKERS_FLAG,
    use_last_updated: bool = typer.Option(False),
    skip_experiments: bool = typer.Option(False),
    skip_samples: bool = typer.Option(False),
    retries: int = _RETRIES_FLAG,
    timeout: int = _TIMEOUT_FLAG,
    max_timeout: int = _MAX_TIMEOUT,
):
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
    db_file: Optional[Path] = _DB_FILE_FLAG,
    start_date: datetime.date = _DATE_FLAG,
    end_date: datetime.date = _DATE_FLAG,
    num_workers: int = _NUM_WORKERS_FLAG,
    batch_size: int = _BATCH_SIZE_FLAG,
    retries: int = _RETRIES_FLAG,
    timeout: int = _TIMEOUT_FLAG,
    max_timeout: int = _MAX_TIMEOUT,
):
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
