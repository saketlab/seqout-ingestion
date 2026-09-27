"""
Daily incremental SRA metadata ingest.

NCBI publishes two different SRA metadata products under
https://ftp.ncbi.nlm.nih.gov/sra/reports/:

- Metadata/NCBI_SRA_Metadata_YYYYMMDD.tar.gz: this module's target. Each one
  is that day's incremental delta (confirmed empirically: a single day's dump
  holds hundreds of studies, not millions), so every day in the requested
  window must be fetched; taking only the latest, as with a full dump, would
  silently drop every earlier day's new records.
- Mirroring/NCBI_SRA_Mirroring_YYYYMMDD_Full/: a full-corpus snapshot,
  published only occasionally. `ingest.sra.ingest_sra` targets that product.

The two are read into the same downstream sra_studies/sra_experiments/...
tables, so this module reuses sra.py's tar-processing internals (parsing and
writing are identical once the right tarball is in hand) and only replaces
dump discovery/selection.
"""

from __future__ import annotations

import datetime
import functools
import logging
import re
from pathlib import Path

from helpers import download_file, exec_query_from_file
from ingest.sra import (
    SRA_OUTPUT_DIR,
    _build_accessions_dict,
    _convert_tab_to_csv,
    _download_accessions_tab_file,
    _download_sra_file_dumps,
    _extract_sra_file_dumps_in_range,
    _process_tarballs,
)

logger = logging.getLogger(__name__)

SRA_DAILY_TARBALL_RE_PAT = re.compile(r"^NCBI_SRA_Metadata_(\d{8})\.tar\.gz$")


def ingest_sra_daily(
    base_output_dir: Path,
    db_file: Path | None,
    start_date: datetime.date,
    end_date: datetime.date,
    num_workers: int,
    chunk_size: int,
    write_batch_size: int,
    accessions_ttl: int,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> None:
    output_dir = Path(base_output_dir) / SRA_OUTPUT_DIR
    output_dir.mkdir(exist_ok=True, parents=True)

    file_dumps = _extract_sra_file_dumps_in_range(
        start_date, end_date, SRA_DAILY_TARBALL_RE_PAT
    )
    if len(file_dumps) == 0:
        msg = f"no daily sra dumps found in {start_date} to {end_date} range"
        raise ValueError(msg)

    # only claim today's db_dir once there is something to ingest, otherwise
    # "latest" lookups elsewhere (backfill/unify/transfer) pick up an empty,
    # table-less db from a run that found nothing.
    date_str = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
    (output_dir / "db").mkdir(exist_ok=True, parents=True)
    db_dir = output_dir / "db" / date_str
    db_dir.mkdir(exist_ok=True, parents=True)
    db_file = db_file or (db_dir / f"sra_metadata_{date_str}.db")

    logger.info(
        "starting daily sra ingest: %d dump(s) in %s..%s",
        len(file_dumps),
        start_date,
        end_date,
    )

    downloader = functools.partial(
        download_file,
        num_retries=retries,
        timeout=timeout,
        max_wait=max_timeout,
        with_pbar=True,
        validate_content_length=True,
    )

    logger.info("downloading accessions tab file")
    tab_path, is_freshly_downloaded = _download_accessions_tab_file(
        downloader,
        output_dir,
        accessions_ttl,
        num_workers,
        chunk_size,
    )

    accessions_csv_path = output_dir / "sra_accessions.csv"
    stale = (
        accessions_csv_path.exists()
        and accessions_csv_path.stat().st_mtime < tab_path.stat().st_mtime
    )
    if stale:
        logger.warning(
            "%s predates %s; reconverting", accessions_csv_path.name, tab_path.name
        )
    to_reprocess_csv = (
        not accessions_csv_path.exists() or is_freshly_downloaded or stale
    )

    if to_reprocess_csv:
        logger.info("converting accessions tab file into csv")
        _convert_tab_to_csv(accessions_csv_path, tab_path, chunk_size)

    logger.info("downloading daily sra file dumps")
    data_dir = _download_sra_file_dumps(
        downloader,
        file_dumps,
        output_dir,
        num_workers,
        chunk_size,
    )

    logger.info("building accession lookup dict")
    accessions_dict = _build_accessions_dict(
        accessions_csv_path,
        to_reprocess_csv,
        output_dir,
    )

    # the exact dumps this run selected, not a directory-wide glob, so a
    # leftover tarball from another run or from ingest_sra's full-dump product
    # is never accidentally reprocessed.
    tarballs = sorted((data_dir / f for f in file_dumps), reverse=True)

    exec_query_from_file("tables/sra.sql", db_file)
    _process_tarballs(
        tarballs,
        db_file,
        accessions_dict,
        num_workers,
        write_batch_size,
    )
    exec_query_from_file("indexes/sra.sql", db_file)
