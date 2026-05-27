import csv
import ctypes
import datetime
import os
import pickle
import re
import sqlite3
import sys
import tarfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from importlib.resources import files
from math import isqrt
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from helpers import better_downloader
from ingest.ingest_config import IngestionConfig

_SRA_METADATA_FTP_BASE_URL = "https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata"
_SRA_METADATA_FOLDER = "sra_metadata"
_SRA_ACCESSIONS_TAB_FILE_NAME = "SRA_Accessions.tab"
_SRA_TARBALL_RE_PAT = re.compile(r"^NCBI_SRA_Metadata_Full_(\d{8})\.tar\.gz$")
_SRA_TARBALL_GLOB_PAT = "NCBI_SRA_Metadata_Full_*.tar.gz"


def ingest_sra(cfg: IngestionConfig):
    output_dir = Path(cfg.output_dir, _SRA_METADATA_FOLDER)
    output_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.datetime.now().strftime("%Y-%m-%d")
    db_file = os.path.join(output_dir, f"sra_metadata_{date_str}.db")

    sra_file_dumps = _extract_sra_file_dumps_in_range(cfg.start_date, cfg.end_date)
    assert len(sra_file_dumps) > 0, (
        f"no sra file dumps found in {cfg.start_date} to {cfg.end_date} range"
    )

    tab_path, to_reprocess_csv = _download_accessions_tab_file(
        output_dir,
        cfg.sra_accessions_ttl,
        cfg.num_workers,
        cfg.chunk_size,
    )

    accessions_csv_path = output_dir / "sra_accessions.csv"
    if to_reprocess_csv:
        _convert_tab_to_csv(accessions_csv_path, output_dir, tab_path, cfg.chunk_size)

    dumps_dir = _download_sra_file_dumps(
        sra_file_dumps, output_dir, cfg.num_workers, cfg.chunk_size
    )

    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        create_sra_tables_query = files("sql").joinpath("sra_tables.sql").read_text()
        cursor.executescript(create_sra_tables_query)

    accessions_idx = _build_accessions_index(
        accessions_csv_path, to_reprocess_csv, output_dir
    )
    tarballs = sorted(dumps_dir.glob(_SRA_TARBALL_GLOB_PAT), reverse=True)

    for tarball in tarballs:
        _process_tarball(tarball)


def _extract_sra_file_dumps_in_range(
    start_date: datetime.date, end_date: datetime.date
) -> list[str]:
    file_dumps: list[str] = []

    with requests.get(_SRA_METADATA_FTP_BASE_URL, timeout=30) as r:
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

    for a in soup.find_all("a", href=True):
        href = str(a["href"])
        match = _SRA_TARBALL_RE_PAT.match(href)
        if match is None:
            continue

        file_date = datetime.datetime.strptime(match.group(1), "%Y%m%d").date()
        if start_date <= file_date <= end_date:
            file_dumps.append(href)

    return file_dumps


def _download_accessions_tab_file(
    output_dir: Path, accessions_ttl: int, num_workers: int, chunk_size: int
) -> tuple[Path, bool]:
    tab_file = Path(os.path.join(output_dir, _SRA_ACCESSIONS_TAB_FILE_NAME))
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
        return tab_file, False

    # TODO: there is a slight issue with the total size of the file which is being shown in tqdm while downloading accessions file. NCBI returns ~30 GB as content length in HEAD request but the actual size of the file is way more than 30 GB
    better_downloader(
        f"{_SRA_METADATA_FTP_BASE_URL}/{_SRA_ACCESSIONS_TAB_FILE_NAME}",
        tab_file,
        num_workers,
        chunk_size,
    )

    return Path(tab_file), True


def _convert_tab_to_csv(
    csv_file_path: Path, output_dir: Path, tab_file_path: Path, chunk_size: int
) -> None:
    libc = ctypes.CDLL("libc.so.6")

    with (
        open(tab_file_path, "rb", buffering=0) as inf,
        open(csv_file_path, "wb") as outf,
        tqdm(
            total=tab_file_path.stat().st_size,
            unit="B",
            unit_scale=True,
            mininterval=2.0,
            desc="converting accessions tab file to csv...",
        ) as pbar,
    ):
        outf.write(b"accession,updated_at,published_at\n")
        offset = 0
        leftover = b""

        # at a time only 5 MiB is present in memory
        while chunk := inf.read(chunk_size):
            # a hack to maintain the memory consumed to be around 100 MiB while converting .tab to .csv
            # the main idea is that after reading a page from the memory, it is immediately evicted from page cache
            # REF: https://www.man7.org/linux/man-pages/man2/posix_fadvise.2.html
            libc.posix_fadvise(inf.fileno(), offset, len(chunk), 4)
            offset += len(chunk)
            pbar.update(len(chunk))

            # as the chunk boundaries are "random" in a way, it is not guaranteed whether it'll split "cleanly" or not
            # so to overcome those problems, leftover chunking strategy is used over here
            for line in (leftover + chunk).split(b"\n")[:-1]:
                f = line.split(b"\t", 7)
                if len(f) > 6 and f[6] == b"SUBMISSION":
                    # manually constructing the csv format to avoid csv.Writer/textmod overhead
                    outf.write(f[1] + b"," + f[3] + b"," + f[4] + b"\n")

            leftover = chunk[chunk.rfind(b"\n") + 1 :]

        if leftover:
            f = leftover.split(b"\t", 7)
            if len(f) > 6 and f[6] == b"SUBMISSION":
                outf.write(f[1] + b"," + f[3] + b"," + f[4] + b"\n")


def _download_sra_file_dumps(
    file_dumps: list[str], output_dir: Path, num_workers: int, chunk_size: int
) -> Path:
    # there are two axes to split the num of workers: num of concurrent files and num of workers per file
    # the most well balanced split would be sqrt(num_workers) and sqrt(num_workers)
    concurrent_files = min(len(file_dumps), isqrt(num_workers))
    workers_per_file = num_workers // concurrent_files
    dumps_dir = output_dir / "dumps"
    dumps_dir.mkdir(parents=True, exist_ok=True)
    missing_files = [f for f in file_dumps if not (dumps_dir / f).exists()]

    if not missing_files:
        print("all sra file dumps are already present. skipping download step...")
        return dumps_dir

    print(f"""starting sra download phase with following configuration:
- file(s) to download: {len(missing_files)}
- concurrent files downloads: {concurrent_files}
- workers per file: {workers_per_file}""")

    def _process_per_file(file: str):
        dest_path = dumps_dir / file
        url = f"{_SRA_METADATA_FTP_BASE_URL}/{file}"
        better_downloader(url, dest_path, workers_per_file, chunk_size)

    if concurrent_files == 1:
        [_process_per_file(file) for file in missing_files]
    else:
        with ThreadPoolExecutor(max_workers=concurrent_files) as pool:
            futures = [pool.submit(_process_per_file, file) for file in missing_files]
            for future in as_completed(futures):
                future.result()

    return dumps_dir


def _build_accessions_index(
    accessions_csv_path: Path, to_rebuild: bool, output_dir: Path
) -> dict[str, str]:
    accessions_idx_pkl_path = output_dir / "sra_accessions_idx.pkl"

    if not to_rebuild and accessions_idx_pkl_path.exists():
        print("loading accessions index from cache...")
        with open(accessions_idx_pkl_path, "rb") as f:
            data = pickle.load(f)
        return {k: sys.intern(v) for k, v in data.items()}

    # NOTE: neither polars nor duckdb are supported on python free threaded version, so going with building the accessions map manually while trying to reduce the memory consumed using sys.intern for dedup
    print("building accessions index...")
    with open(accessions_csv_path, "r", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        col = col = {name: i for i, name in enumerate(header)}
        acc_i, upd_i, pub_i = col["accession"], col["updated_at"], col["published_at"]

        accessions_idx: dict[str, str] = {
            row[acc_i]: sys.intern(row[upd_i] + "|" + row[pub_i]) for row in reader
        }

    with open(accessions_idx_pkl_path, "wb") as f:
        pickle.dump(accessions_idx, f, protocol=pickle.HIGHEST_PROTOCOL)
    return accessions_idx


def _process_tarball(tarball_path: Path):
    print(tarball_path)
    with tarfile.open(tarball_path, "r:*") as f:
        for member in f:
            if not member.isfile():
                continue

            print(f"{tarball_path}: {member.name}")
