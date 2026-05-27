import os
import re
import sqlite3
import time
from datetime import datetime
from importlib.resources import files
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from helpers import better_downloader
from ingest.ingest_config import IngestionConfig

_SRA_REPORTS_BASE_URL = "https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata"
_SRA_FILE_DUMP_PREFIX = "NCBI_SRA_Metadata_Full_"
_SRA_METADATA_FOLDER = "sra_metadata"
_SRA_ACCESSIONS_TAB_FILE_NAME = "SRA_Accessions.tab"
_SRA_TARBALL_FILE_PATTERN = re.compile(r"^NCBI_SRA_Metadata_Full_(\d{8})\.tar\.gz$")


def ingest_sra(cfg: IngestionConfig):
    output_dir = Path(cfg.output_dir, _SRA_METADATA_FOLDER)
    output_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    db_file = os.path.join(output_dir, f"sra_metadata_{date_str}.db")

    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        create_sra_tables_query = files("sql").joinpath("sra_tables.sql").read_text()
        cursor.executescript(create_sra_tables_query)

    sra_file_dumps = _extract_sra_file_dumps_in_range(cfg.start_date, cfg.end_date)
    _download_accessions_tab_file(
        output_dir,
        cfg.sra_accessions_ttl,
        cfg.num_download_workers,
        cfg.download_chunk_size,
    )


def _extract_sra_file_dumps_in_range(
    start_date: datetime, end_date: datetime
) -> list[str]:
    files: list[str] = []

    with requests.get(_SRA_REPORTS_BASE_URL, timeout=30) as r:
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

    for a in soup.find_all("a", href=True):
        href = str(a["href"])
        match = _SRA_TARBALL_FILE_PATTERN.match(href)
        if match is None:
            continue

        file_date = datetime.strptime(match.group(1), "%Y%m%d").date()
        if start_date.date() <= file_date <= end_date.date():
            files.append(href)

    return files


def _download_accessions_tab_file(
    output_dir: Path, accessions_ttl: int, num_workers: int, download_chunk_size: int
) -> None:
    tab_file = os.path.join(output_dir, _SRA_ACCESSIONS_TAB_FILE_NAME)
    to_download = False

    try:
        info = os.stat(tab_file)
        diff = time.time() - info.st_mtime
        if diff > accessions_ttl:
            to_download = True
    except FileNotFoundError:
        print("sra accessions tab file is not found. downloading it fresh...")
        to_download = True

    if not to_download:
        print("skipping download for sra accessions tab file as it is within ttl range")
        return

    better_downloader(
        f"{_SRA_REPORTS_BASE_URL}/{_SRA_ACCESSIONS_TAB_FILE_NAME}",
        tab_file,
        num_workers,
        download_chunk_size,
    )
