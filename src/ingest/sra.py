import csv
import ctypes
import datetime
import enum
import functools
import io
import json
import logging
import pickle
import queue
import re
import shlex
import subprocess
import tarfile
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import isqrt
from pathlib import Path
from typing import IO

import httpx
import humanfriendly
import lxml.etree as ET  # noqa: N812
from bs4 import BeautifulSoup
from tqdm import tqdm

from helpers import download_file, exec_query_from_file, sqlite_connect

logger = logging.getLogger(__name__)


class FileType(enum.StrEnum):
    Submission = "submission"
    Sample = "sample"
    Study = "study"
    Experiment = "experiment"
    Run = "run"


SRA_OUTPUT_DIR = "sra"
SRA_FTP_BASE_URL = "https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata"
SRA_ACCESSIONS_TAB_FILE_NAME = "SRA_Accessions.tab"
SRA_TARBALL_RE_PAT = re.compile(r"^NCBI_SRA_Metadata_Full_(\d{8})\.tar\.gz$")
SRA_TARBALL_GLOB_PAT = "NCBI_SRA_Metadata_Full_*.tar.gz"

SRA_SQL_STATEMENTS = {
    FileType.Submission: (
        "INSERT OR REPLACE INTO sra_submissions "
        "(accession, alias, center_name, lab_name, broker_name, "
        "updated_at, published_at) "
        "VALUES (?,?,?,?,?,?,?)"
    ),
    FileType.Sample: (
        "INSERT OR REPLACE INTO sra_samples "
        "(accession, alias, description, title, scientific_name, taxon_id, "
        "external_id, attributes_json, submission, links) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)"
    ),
    FileType.Study: (
        "INSERT OR REPLACE INTO sra_studies "
        "(accession, alias, center_name, abstract, title, external_id, "
        "study_type, attributes_json, links, center_project_name, "
        "submission, published_at, updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
    ),
    FileType.Experiment: (
        "INSERT OR REPLACE INTO sra_experiments "
        "(accession, design_description, library_layout, library_name, "
        "library_selection, library_source, library_strategy, samples, "
        "platform, instrument_model, study, title, submission) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
    ),
    FileType.Run: (
        "INSERT OR REPLACE INTO sra_runs "
        "(accession, experiment, submission) VALUES (?,?,?)"
    ),
}


def ingest_sra(
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

    date_str = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
    (output_dir / "db").mkdir(exist_ok=True, parents=True)
    db_dir = output_dir / "db" / date_str
    db_dir.mkdir(exist_ok=True, parents=True)
    db_file = db_file or (db_dir / f"sra_metadata_{date_str}.db")

    logger.info(
        "starting sra ingest with following config:\n"
        "- num workers: %d\n"
        "- chunk size: %s\n"
        "- write batch size: %d\n"
        "- accessions file ttl: %s\n"
        "- retries: %d\n"
        "- timeout: %s\n"
        "- max timeout: %s",
        num_workers,
        humanfriendly.format_size(chunk_size),
        write_batch_size,
        humanfriendly.format_timespan(accessions_ttl),
        retries,
        humanfriendly.format_timespan(timeout),
        humanfriendly.format_timespan(max_timeout),
    )

    logger.debug(
        "extracting sra file dumps in range: start_date=%s, end_date=%s",
        start_date,
        end_date,
    )
    file_dumps = _extract_sra_file_dumps_in_range(start_date, end_date)
    if len(file_dumps) == 0:
        raise ValueError(f"no sra file dumps found in {start_date} to {end_date} range")

    downloader = functools.partial(
        download_file,
        num_retries=retries,
        timeout=timeout,
        max_wait=max_timeout,
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
    to_reprocess_csv = (
        True if not accessions_csv_path.exists() else is_freshly_downloaded
    )

    if to_reprocess_csv:
        logger.info("converting accessions tab file into csv")
        _convert_tab_to_csv(accessions_csv_path, tab_path, chunk_size)

    logger.info("downloading sra file dumps")
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
    tarballs = sorted(data_dir.glob(SRA_TARBALL_GLOB_PAT), reverse=True)

    exec_query_from_file("tables/sra.sql", db_file)
    _process_tarballs(
        tarballs,
        db_file,
        accessions_dict,
        num_workers,
        write_batch_size,
    )
    exec_query_from_file("indexes/sra.sql", db_file)


def _extract_sra_file_dumps_in_range(
    start_date: datetime.date,
    end_date: datetime.date,
) -> list[str]:
    file_dumps: list[str] = []

    r = httpx.get(SRA_FTP_BASE_URL, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    for a in soup.find_all("a", href=True):
        href = str(a["href"])
        match = SRA_TARBALL_RE_PAT.match(href)
        if match is None:
            continue

        file_date = datetime.datetime.strptime(match.group(1), "%Y%m%d").date()  # noqa: DTZ007
        if start_date <= file_date <= end_date:
            file_dumps.append(href)

    return file_dumps


def _download_accessions_tab_file(
    downloader: functools.partial,
    output_dir: Path,
    accessions_ttl: int,
    num_workers: int,
    chunk_size: int,
) -> tuple[Path, bool]:
    tab_file = output_dir / SRA_ACCESSIONS_TAB_FILE_NAME
    to_download = False

    try:
        info = tab_file.stat()
        diff = time.time() - info.st_mtime
        if diff > accessions_ttl:
            to_download = True
    except FileNotFoundError:
        logger.info("sra accession tab file is not found. downloading it fresh")
        to_download = True

    if not to_download:
        logger.info(
            "skipped download step for sra accessions tab file as it already exists "
            "and it is within the ttl range",
        )
        return tab_file, False

    downloader(
        f"{SRA_FTP_BASE_URL}/{SRA_ACCESSIONS_TAB_FILE_NAME}",
        tab_file,
        num_workers=num_workers,
        chunk_size=chunk_size,
    )

    return Path(tab_file), True


def _convert_tab_to_csv(
    csv_file_path: Path,
    tab_file_path: Path,
    chunk_size: int,
) -> None:
    libc = ctypes.CDLL("libc.so.6")

    file_type_col = 6
    with (
        tab_file_path.open("rb", buffering=0) as inf,
        csv_file_path.open("wb") as outf,
        tqdm(
            total=tab_file_path.stat().st_size,
            unit="B",
            unit_scale=True,
            mininterval=2.0,
            desc="converting accessions tab file to csv",
        ) as pbar,
    ):
        outf.write(b"accession,updated_at,published_at\n")
        offset = 0
        leftover = b""

        while chunk := inf.read(chunk_size):
            libc.posix_fadvise(inf.fileno(), offset, len(chunk), 4)
            offset += len(chunk)
            pbar.update(len(chunk))

            for line in (leftover + chunk).split(b"\n")[:-1]:
                f = line.split(b"\t", 7)
                if len(f) > file_type_col and f[file_type_col] in (
                    b"SUBMISSION",
                    b"STUDY",
                ):
                    outf.write(
                        f[0]
                        + b","
                        + f[3].split(b"T")[0]
                        + b","
                        + f[4].split(b"T")[0]
                        + b"\n"
                    )

            leftover = chunk[chunk.rfind(b"\n") + 1 :]

        if leftover:
            f = leftover.split(b"\t", 7)
            if len(f) > file_type_col and f[file_type_col] in (b"SUBMISSION", b"STUDY"):
                outf.write(
                    f[0]
                    + b","
                    + f[3].split(b"T")[0]
                    + b","
                    + f[4].split(b"T")[0]
                    + b"\n"
                )


def _download_sra_file_dumps(
    downloader: functools.partial,
    file_dumps: list[str],
    output_dir: Path,
    num_workers: int,
    chunk_size: int,
) -> Path:
    concurrent_files = min(len(file_dumps), isqrt(num_workers))
    workers_per_file = num_workers // concurrent_files
    data_dir = output_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    missing_files = [f for f in file_dumps if not (data_dir / f).exists()]

    if not missing_files:
        logger.info(
            "skipped download for sra file dumps as all of them are already present",
        )
        return data_dir

    logger.info(
        "starting downloading phase for sra file dumps with following config:\n"
        "- num of file(s) to download: %d\n"
        "- num of concurrent file downloads: %d\n"
        "- num workers per file: %d",
        len(missing_files),
        concurrent_files,
        workers_per_file,
    )

    def _process_per_file(file: str) -> None:
        dest_path = data_dir / file
        url = f"{SRA_FTP_BASE_URL}/{file}"
        downloader(url, dest_path, num_workers=workers_per_file, chunk_size=chunk_size)

    if concurrent_files == 1:
        for file in missing_files:
            _process_per_file(file)
    else:
        with ThreadPoolExecutor(max_workers=concurrent_files) as pool:
            futures = [pool.submit(_process_per_file, file) for file in missing_files]
            for future in as_completed(futures):
                future.result()

    return data_dir


def _build_accessions_dict(
    accessions_csv_path: Path,
    to_rebuild: bool,
    output_dir: Path,
) -> dict[str, str]:
    pickle_path = output_dir / "sra_accessions.pkl"

    if not to_rebuild and pickle_path.exists():
        logger.debug("loading accessions dict from pickle")
        with pickle_path.open("rb") as f:
            return pickle.load(f)  # noqa: S301

    logger.info("building accessions dict from csv")
    accessions: dict[str, str] = {}
    with accessions_csv_path.open(newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        col = {name: i for i, name in enumerate(header)}
        for row in reader:
            key = row[col["accession"]]
            updated_at = row[col["updated_at"]].split("T")[0]
            published_at = row[col["published_at"]].split("T")[0]
            val = updated_at + "|" + published_at
            accessions[key] = val

    logger.info("saving accessions dict to pickle")
    with pickle_path.open("wb") as f:
        pickle.dump(accessions, f, protocol=pickle.HIGHEST_PROTOCOL)

    return accessions


def _process_submission(
    submission_accession: str,
    content: IO[bytes],
    accessions: dict[str, str],
) -> Iterator[tuple]:
    ctx = ET.iterparse(content, events=("end",))
    attrs: dict[str, str] = {}

    try:
        for _, elem in ctx:
            if elem.tag != "SUBMISSION":
                continue

            attrs = dict(elem.attrib)
            elem.clear()
            break
    except ET.ParseError:
        return

    accession = attrs.get("accession", submission_accession)
    if not accession:
        return

    dates_str = accessions.get(accession)
    if not dates_str:
        return
    updated_at, published_at = dates_str.split("|")

    yield (
        accession,
        attrs.get("alias"),
        attrs.get("center_name"),
        attrs.get("lab_name"),
        attrs.get("broker_name"),
        updated_at,
        published_at,
    )


def _process_studies(
    submission_accession: str,
    content: IO[bytes],
    accessions: dict[str, str],
) -> Iterator[tuple]:
    ctx = ET.iterparse(content, events=("end",))

    try:
        for _, elem in ctx:
            if elem.tag != "STUDY_SET":
                continue

            for study in elem.findall("STUDY"):
                accession = study.get("accession")
                descriptor = study.find("DESCRIPTOR")

                identifiers = study.find("IDENTIFIERS")
                external_ids = (
                    identifiers.findall("EXTERNAL_ID")
                    if identifiers is not None
                    else []
                )
                attrs = study.findall("STUDY_ATTRIBUTES/STUDY_ATTRIBUTE")
                links = study.findall("STUDY_LINKS/STUDY_LINK")

                dates_str = accessions.get(accession)
                if not dates_str:
                    continue
                updated_at, published_at = dates_str.split("|")

                study_type_elem = (
                    descriptor.find("STUDY_TYPE") if descriptor is not None else None
                )
                study_type = (
                    study_type_elem.get("existing_study_type")
                    if study_type_elem is not None
                    else None
                )

                yield (
                    accession,
                    study.get("alias"),
                    study.get("center_name"),
                    descriptor.findtext("STUDY_ABSTRACT")
                    if descriptor is not None
                    else None,
                    descriptor.findtext("STUDY_TITLE")
                    if descriptor is not None
                    else None,
                    _parse_external_ids(external_ids),
                    study_type,
                    _parse_attributes(attrs),
                    _parse_links(links),
                    descriptor.findtext("CENTER_PROJECT_NAME")
                    if descriptor is not None
                    else None,
                    submission_accession,
                    published_at,
                    updated_at,
                )

                study.clear()
            elem.clear()
            break
    except ET.ParseError:
        return


def _process_samples(submission_accession: str, content: IO[bytes]) -> Iterator[tuple]:
    ctx = ET.iterparse(content, events=("end",))

    try:
        for _, elem in ctx:
            if elem.tag != "SAMPLE":
                continue

            accession = elem.get("accession")
            identifiers = elem.find("IDENTIFIERS")
            external_ids = (
                identifiers.findall("EXTERNAL_ID") if identifiers is not None else []
            )

            attrs = elem.findall("SAMPLE_ATTRIBUTES/SAMPLE_ATTRIBUTE")
            links = elem.findall("SAMPLE_LINKS/SAMPLE_LINK")

            yield (
                accession,
                elem.get("alias"),
                elem.findtext("DESCRIPTION"),
                elem.findtext("TITLE"),
                elem.findtext("SAMPLE_NAME/SCIENTIFIC_NAME"),
                elem.findtext("SAMPLE_NAME/TAXON_ID"),
                _parse_external_ids(external_ids),
                _parse_attributes(attrs),
                submission_accession,
                _parse_links(links),
            )

            elem.clear()
    except ET.ParseError:
        return


def _process_experiments(
    submission_accession: str,
    content: IO[bytes],
) -> Iterator[tuple]:
    ctx = ET.iterparse(content, events=("end",))

    try:
        for _, elem in ctx:
            if elem.tag != "EXPERIMENT_SET":
                continue

            for experiment in elem.findall("EXPERIMENT"):
                experiment_accession = experiment.get("accession")

                design_elem = experiment.find("DESIGN")

                design_description = None
                library_layout = None
                library_name = None
                library_selection = None
                library_source = None
                library_strategy = None

                if design_elem is not None:
                    design_description = design_elem.findtext("DESIGN_DESCRIPTION")
                    lib_desc = design_elem.find("LIBRARY_DESCRIPTOR")
                    if lib_desc is not None:
                        library_name = lib_desc.findtext("LIBRARY_NAME")
                        library_selection = lib_desc.findtext("LIBRARY_SELECTION")
                        library_source = lib_desc.findtext("LIBRARY_SOURCE")
                        library_strategy = lib_desc.findtext("LIBRARY_STRATEGY")
                        layout_elem = lib_desc.find("LIBRARY_LAYOUT")
                        if layout_elem is not None:
                            first_child = next(iter(layout_elem), None)
                            library_layout = (
                                str(first_child.tag)
                                if first_child is not None
                                else None
                            )

                samples = []
                if design_elem is not None:
                    for sample in design_elem.findall("SAMPLE_DESCRIPTOR"):
                        sample_accession = sample.get("accession")
                        if sample_accession:
                            samples.append(sample_accession)

                platform_elem = experiment.find("PLATFORM")
                platform = None
                instrument_model = None
                if platform_elem is not None:
                    first_child = next(iter(platform_elem), None)
                    if first_child is not None:
                        platform = str(first_child.tag)
                        instrument_model = first_child.findtext("INSTRUMENT_MODEL")

                study_ref_elem = experiment.find("STUDY_REF")
                study_ref = (
                    study_ref_elem.get("accession")
                    if study_ref_elem is not None
                    else None
                )

                title = experiment.findtext("TITLE")

                yield (
                    experiment_accession,
                    design_description,
                    library_layout,
                    library_name,
                    library_selection,
                    library_source,
                    library_strategy,
                    json.dumps(samples),
                    platform,
                    instrument_model,
                    study_ref,
                    title,
                    submission_accession,
                )

                experiment.clear()
            elem.clear()
            break
    except ET.ParseError:
        return


def _process_runs(submission_accession: str, content: IO[bytes]) -> Iterator[tuple]:
    ctx = ET.iterparse(content, events=("end",))

    try:
        for _, elem in ctx:
            if elem.tag != "RUN_SET":
                continue

            items = elem.findall("RUN")

            for run in items:
                experiment_ref_elem = run.find("EXPERIMENT_REF")
                experiment_ref = (
                    experiment_ref_elem.get("accession")
                    if experiment_ref_elem is not None
                    else None
                )

                yield (run.get("accession"), experiment_ref, submission_accession)

                run.clear()
            elem.clear()
            break
    except ET.ParseError:
        return


def _extract_worker(
    tarball: Path,
    file_queue: queue.Queue,
    pbar: tqdm,
) -> None:
    try:
        with tarfile.open(tarball, mode="r|gz") as tar:
            for member in tar:
                if not member.isfile():
                    continue

                name = member.name
                submission_accession = name.split("/")[0]
                if not submission_accession:
                    continue

                file_type = _convert_file_name_to_type(name)
                if not file_type:
                    continue

                try:
                    f = tar.extractfile(member)
                    if f is None:
                        continue

                    content = f.read()
                    file_queue.put((submission_accession, file_type, content))
                    pbar.update(1)
                except Exception:
                    logger.exception(
                        "[extract] failed to read %s in %s",
                        name,
                        tarball,
                    )
    except Exception:
        logger.exception("[extract] failed to open tarball %s", tarball)
        raise


def _parse_worker(
    file_queue: queue.Queue,
    write_queue: queue.Queue,
    accessions: dict[str, str],
    write_batch_size: int,
    extract_done: threading.Event,
) -> None:
    buffer: dict[FileType, list] = {}

    def _flush(file_type: FileType) -> None:
        rows = buffer.get(file_type)
        if rows:
            write_queue.put((file_type, rows))
            buffer[file_type] = []

    def _append(file_type: FileType, row: tuple) -> None:
        lst = buffer.setdefault(file_type, [])
        lst.append(row)

        if len(lst) >= write_batch_size:
            _flush(file_type)

    while True:
        try:
            item = file_queue.get(timeout=1.0)
        except queue.Empty:
            if extract_done.is_set():
                try:
                    item = file_queue.get(timeout=0.5)
                except queue.Empty:
                    for ft in list(buffer.keys()):
                        _flush(ft)
                    return
            continue

        submission_accession, file_type, content = item
        bio = io.BytesIO(content)

        try:
            if file_type == FileType.Submission:
                processor = _process_submission(submission_accession, bio, accessions)
            elif file_type == FileType.Study:
                processor = _process_studies(submission_accession, bio, accessions)
            elif file_type == FileType.Sample:
                processor = _process_samples(submission_accession, bio)
            elif file_type == FileType.Experiment:
                processor = _process_experiments(submission_accession, bio)
            elif file_type == FileType.Run:
                processor = _process_runs(submission_accession, bio)
            else:
                continue

            for row in processor:
                _append(file_type, row)
        except Exception:
            logger.exception("[parse] failed to process %s", submission_accession)


def _coerce(val: object) -> object:
    if isinstance(val, (str, int, float, bytes, type(None))):
        return val
    return str(val)


def _writer_worker(
    write_queue: queue.Queue,
    db_file: Path,
    write_batch_size: int,
    parse_done: threading.Event,
) -> None:
    conn = sqlite_connect(str(db_file))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA cache_size=-800000")

    cursor = conn.cursor()
    rows_written = 0
    buffer: dict[FileType, list] = {}

    def _flush(file_type: FileType) -> None:
        nonlocal rows_written
        batch = buffer.get(file_type)
        if batch:
            safe_batch = [tuple(_coerce(v) for v in row) for row in batch]
            cursor.executemany(SRA_SQL_STATEMENTS[file_type], safe_batch)
            conn.commit()
            rows_written += len(batch)
            buffer[file_type] = []

    while True:
        try:
            item = write_queue.get(timeout=1.0)
        except queue.Empty:
            if parse_done.is_set():
                for ft in list(buffer.keys()):
                    _flush(ft)
                break
            continue

        file_type, rows = item
        lst = buffer.setdefault(file_type, [])
        lst.extend(rows)
        if len(lst) >= write_batch_size:
            _flush(file_type)

    conn.close()


def _process_tarballs(
    tarballs: list[Path],
    db_file: Path,
    accessions: dict[str, str],
    num_workers: int,
    write_batch_size: int,
) -> None:
    num_extract = max(1, min(len(tarballs), num_workers - 2))
    num_parse = max(1, num_workers - num_extract - 1)
    num_writer = 1

    logger.debug(
        "worker distribution: %d extract, %d parse, %d writer",
        num_extract,
        num_parse,
        num_writer,
    )

    file_queue: queue.Queue = queue.Queue(maxsize=100)
    write_queue: queue.Queue = queue.Queue()

    total_files = 0
    for tb in tarballs:
        # takes around ~5-6 mins for performing file counting over 15 GB SRA file dump
        result = subprocess.run(  # noqa: S602
            f"tar -I pigz -tf {shlex.quote(str(tb))} | grep -v '/$' | wc -l",
            capture_output=True,
            text=True,
            shell=True,
            check=False,
        )
        result.check_returncode()
        total_files += int(result.stdout.strip())

    extract_done = threading.Event()
    parse_done = threading.Event()

    with (
        ThreadPoolExecutor(max_workers=num_extract) as extract_pool,
        ThreadPoolExecutor(max_workers=num_parse) as parse_pool,
        ThreadPoolExecutor(max_workers=num_writer) as writer_pool,
        tqdm(
            total=total_files,
            unit="file",
            desc="processing files",
            mininterval=2.0,
        ) as pbar,
    ):
        write_future = writer_pool.submit(
            _writer_worker,
            write_queue,
            db_file,
            write_batch_size,
            parse_done,
        )
        parse_futures = [
            parse_pool.submit(
                _parse_worker,
                file_queue,
                write_queue,
                accessions,
                write_batch_size,
                extract_done,
            )
            for _ in range(num_parse)
        ]
        extract_futures = [
            extract_pool.submit(_extract_worker, tb, file_queue, pbar)
            for tb in tarballs
        ]

        for f in as_completed(extract_futures):
            f.result()
        extract_done.set()

        for f in as_completed(parse_futures):
            f.result()
        parse_done.set()

        write_future.result()


def _parse_external_ids(external_ids: list) -> str | None:
    if not external_ids:
        return None
    return json.dumps({el.get("namespace"): el.text for el in external_ids})


def _parse_attributes(sample_attrs: list) -> str | None:
    if not sample_attrs:
        return None
    return json.dumps(
        {attr.findtext("TAG"): attr.findtext("VALUE") for attr in sample_attrs},
    )


def _parse_links(sample_links: list) -> str | None:
    if not sample_links:
        return None
    results = {}

    for link in sample_links:
        url = link.find("URL_LINK")
        if url is not None:
            label = url.findtext("LABEL")
            results[label] = url.findtext("URL")
        else:
            for child in link:
                results[str(child.tag)] = {str(el.tag): el.text for el in child}

    return json.dumps(results)


def _convert_file_name_to_type(file_name: str) -> FileType | None:
    if file_name.endswith(".submission.xml"):
        return FileType.Submission
    if file_name.endswith(".sample.xml"):
        return FileType.Sample
    if file_name.endswith(".study.xml"):
        return FileType.Study
    if file_name.endswith(".experiment.xml"):
        return FileType.Experiment
    if file_name.endswith(".run.xml"):
        return FileType.Run
    return None
