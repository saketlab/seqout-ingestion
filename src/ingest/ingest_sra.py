import ctypes
import os
import re
import sqlite3
import time
from datetime import datetime
from importlib.resources import files
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

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
    tab_path = _download_accessions_tab_file(
        output_dir,
        cfg.sra_accessions_ttl,
        cfg.num_download_workers,
        cfg.download_chunk_size,
    )

    if tab_path:
        accessions_csv_path = _convert_tab_to_csv(output_dir, tab_path)


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
) -> Path | None:
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
        return None

    better_downloader(
        f"{_SRA_REPORTS_BASE_URL}/{_SRA_ACCESSIONS_TAB_FILE_NAME}",
        tab_file,
        num_workers,
        download_chunk_size,
    )

    return Path(tab_file)


def _convert_tab_to_csv(output_dir: Path, tab_file_path: Path) -> Path:
    libc = ctypes.CDLL("libc.so.6")
    accessions_csv_path = Path(output_dir, "sra_accessions.csv")

    with (
        open(tab_file_path, "rb", buffering=0) as inf,
        open(accessions_csv_path, "wb") as outf,
        tqdm(
            total=tab_file_path.stat().st_size,
            unit="B",
            unit_scale=True,
            mininterval=2.0,
        ) as pbar,
    ):
        outf.write(b"Accession,Updated,Published\n")
        offset = 0
        leftover = b""

        # at a time only 5 MiB is present in memory
        while chunk := inf.read(5 * 1024 * 1024):
            # a hack to maintain the memory consumed around 100 MiB
            # for converting .tab to .csv
            # after reading a page from memory, it is immediately evicted from page cache to not OOM
            dontneed = 4
            libc.posix_fadvise(inf.fileno(), offset, len(chunk), dontneed)
            offset += len(chunk)
            pbar.update(len(chunk))

            for line in (leftover + chunk).split(b"\n")[:-1]:
                f = line.split(b"\t", 7)
                if len(f) > 6 and f[6] == b"SUBMISSION":
                    outf.write(
                        f[1] + b"," + f[3] + b"," + f[4] + b"\n"
                    )  # manually constructing the csv format to avoid csv.Writer/textmode overhead

            leftover = chunk[chunk.rfind(b"\n") + 1 :]

        if leftover:
            f = leftover.split(b"\t", 7)
            if len(f) > 6 and f[6] == b"SUBMISSION":
                outf.write(f[1] + b"," + f[3] + b"," + f[4] + b"\n")

    return accessions_csv_path
