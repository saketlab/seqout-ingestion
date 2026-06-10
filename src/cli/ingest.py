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
_CHUNK_SIZE_FLAG = typer.Option("20 MiB", parser=_parse_chunk_size)
_BATCH_SIZE_FLAG = typer.Option(2000)


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
):
    ingest_geo(
        base_output_dir,
        db_file,
        num_workers,
        chunk_size,
        batch_size,
        series_pickle,
        series_limit,
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
    read_batch_size: int = typer.Option(100),
    write_batch_size: int = typer.Option(500),
    accessions_ttl: int = typer.Option(2 * 24 * 60 * 60, parser=_parse_timespan),
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
):
    ingest_ae(base_output_dir, db_file, start_date, end_date, num_workers, batch_size)
