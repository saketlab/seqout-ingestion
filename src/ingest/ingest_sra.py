import csv
import ctypes
import datetime
import enum
import os
import pickle
import queue
import re
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import threading
import time
import xml.parsers.expat as expat
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
_STOP = object()


class FileType(enum.StrEnum):
    Submission = "submission"
    Sample = "sample"
    Study = "study"
    Experiment = "experiment"
    Run = "run"


def ingest_sra(cfg: IngestionConfig):
    output_dir = Path(cfg.output_dir, _SRA_METADATA_FOLDER)
    output_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.datetime.now().strftime("%Y-%m-%d")
    db_dir = output_dir / "db" / date_str
    db_dir.mkdir(parents=True, exist_ok=True)
    db_file = os.path.join(db_dir, f"sra_metadata_{date_str}.db")

    sra_file_dumps = _extract_sra_file_dumps_in_range(cfg.start_date, cfg.end_date)
    assert len(sra_file_dumps) > 0, (
        f"no sra file dumps found in {cfg.start_date} to {cfg.end_date} range"
    )

    tab_path, to_reprocess_csv = _download_accessions_tab_file(
        output_dir,
        cfg.sra_accessions_ttl,
        cfg.n_workers,
        cfg.chunk_size,
    )

    accessions_csv_path = output_dir / "sra_accessions.csv"
    if to_reprocess_csv:
        _convert_tab_to_csv(accessions_csv_path, output_dir, tab_path, cfg.chunk_size)

    dumps_dir = _download_sra_file_dumps(
        sra_file_dumps, output_dir, cfg.n_workers, cfg.chunk_size
    )

    accessions_idx = _build_accessions_index(
        accessions_csv_path, to_reprocess_csv, output_dir
    )
    tarballs = sorted(dumps_dir.glob(_SRA_TARBALL_GLOB_PAT), reverse=True)

    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        create_sra_tables_query = files("sql").joinpath("sra_tables.sql").read_text()
        cursor.executescript(create_sra_tables_query)

    _process_tarballs(tarballs, accessions_idx, db_file, cfg.n_workers, cfg.chunk_size)


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

    print("building accessions index...")
    # NOTE: neither polars nor duckdb are supported on python free threaded version, so going with building the accessions map manually while trying to reduce the memory consumed using sys.intern for dedup
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


def _reader_worker(
    tarball: Path,
    transform_queue: queue.Queue[list[tuple[str, FileType, bytes]]],
    pbar: tqdm,
    n_readers: int,
    chunk_size: int,
    batch_size: int,
):
    proc = subprocess.Popen(
        ["pigz", "-dc", "-p", str(n_readers), str(tarball)],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        bufsize=chunk_size,
    )

    batch: list[tuple[str, FileType, bytes]] = []

    try:
        with tarfile.open(fileobj=proc.stdout, mode="r|") as tar:
            for member in tar:
                if not member.isfile():
                    continue

                name = member.name

                try:
                    if name.endswith(".submission.xml"):
                        file_type = FileType.Submission
                    else:
                        continue

                    accession = name.split("/")[0]
                    if not accession:
                        continue

                    f = tar.extractfile(member)
                    if f is None:
                        continue
                    content = f.read()

                    batch.append((accession, file_type, content))
                    if len(batch) >= batch_size:
                        transform_queue.put(batch)
                        pbar.update(len(batch))
                        batch.clear()

                except Exception as e:
                    print(f"[reader] failed to read {tarball}: {e}")

        if batch:
            transform_queue.put(batch)
            pbar.update(len(batch))
            batch.clear()
    finally:
        if proc:
            proc.wait()


def _transform_worker(
    transform_queue: queue.Queue[list[tuple[str, FileType, bytes]]],
    write_queue: queue.Queue[list[tuple[FileType, list]] | object],
    accessions_idx: dict[str, str],
    batch_size: int,
):
    batch: list[tuple[FileType, list]] = []

    while True:
        try:
            record = transform_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        if record is _STOP:
            if batch:
                write_queue.put(batch)
                batch.clear()
            write_queue.put(_STOP)
            break

        for accession, file_type, content in record:
            try:
                if file_type == FileType.Submission:
                    result = _process_submission(accession, content, accessions_idx)
                    if result:
                        batch.append((file_type, result))
            except Exception as e:
                print(f"[processor] failed to process {accession}: {e}")

        if len(batch) >= batch_size:
            write_queue.put(batch)
            batch.clear()


def _process_submission(accession: str, content: bytes, accessions_idx: dict[str, str]):
    attrs_found = {}

    def _start_element(tag: str, attrs: dict):
        if tag == "SUBMISSION":
            attrs_found.update(attrs)
            # early exit once submission root element tag is found
            raise expat.ExpatError

    p = expat.ParserCreate()
    p.StartElementHandler = _start_element

    try:
        p.Parse(content)
    except expat.ExpatError:
        pass

    if not attrs_found:
        return None

    dates = accessions_idx.get(accession)
    updated_at, published_at = dates.split("|") if dates else (None, None)

    return (
        accession,
        attrs_found.get("alias"),
        attrs_found.get("center_name"),
        attrs_found.get("lab_name"),
        attrs_found.get("broker_name"),
        updated_at,
        published_at,
    )


def _writer_worker(
    write_queue: queue.Queue[list[tuple[FileType, list]]],
    db_file: str,
    sentinels_to_receive: int,
    batch_size: int,
):
    conn = sqlite3.connect(db_file)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    cursor = conn.cursor()

    sentinels_received = 0

    sql_statements: dict[FileType, str] = {
        FileType.Submission: "INSERT OR IGNORE INTO sra_submissions (accession, alias, center_name, lab_name, broker_name, updated_at, published_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
    }

    batches: dict[FileType, list] = {
        FileType.Submission: [],
    }

    while True:
        try:
            record = write_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        if record is _STOP:
            sentinels_received += 1
            if sentinels_received == sentinels_to_receive:
                break
            continue

        for file_type, row in record:
            batches[file_type].append(row)
            if len(batches[file_type]) >= batch_size:
                cursor.executemany(sql_statements[file_type], batches[file_type])
                batches[file_type] = []

    for file_type, rows in batches.items():
        if rows:
            cursor.executemany(sql_statements[file_type], rows)

    conn.commit()
    conn.close()


def _process_tarballs(
    tarballs: list[Path],
    accessions_idx: dict[str, str],
    db_file: str,
    n_workers: int,
    chunk_size: int,
):
    if not shutil.which("pigz"):
        raise ValueError(
            "pigz is not found. please install it for parallel decompression"
        )

    cpu = os.cpu_count() or 4

    max_concurrent_readers = max(1, cpu // 4)
    pigz_cores_per_reader = max(1, cpu // max_concurrent_readers)
    n_writer = 1
    n_transform = max(1, n_workers - max_concurrent_readers - n_writer)
    batch_size = max(100, n_transform * 50)

    transform_queue = queue.Queue(n_transform * 200)
    write_queue = queue.Queue(n_transform * 200)

    read_sem = threading.Semaphore(max_concurrent_readers)
    readers_done = threading.Barrier(len(tarballs))

    total_estimate_xml_files = len(accessions_idx) * 5
    pbar = tqdm(
        total=total_estimate_xml_files,
        unit="file",
        desc="processing submissions",
        mininterval=2.0,
    )

    def _reader_with_sem(tarball: Path):
        with read_sem:
            _reader_worker(
                tarball,
                transform_queue,
                pbar,
                pigz_cores_per_reader,
                chunk_size,
                batch_size,
            )

        if readers_done.wait() == 0:
            for _ in range(n_transform):
                transform_queue.put(_STOP)

    with ThreadPoolExecutor(n_workers) as pool:
        reader_futures = [pool.submit(_reader_with_sem, tb) for tb in tarballs]
        transform_futures = [
            pool.submit(
                _transform_worker,
                transform_queue,
                write_queue,
                accessions_idx,
                batch_size,
            )
            for _ in range(n_transform)
        ]
        write_future = pool.submit(
            _writer_worker, write_queue, db_file, n_transform, batch_size
        )

        for f in [*reader_futures, *transform_futures, write_future]:
            f.result()

    pbar.close()
