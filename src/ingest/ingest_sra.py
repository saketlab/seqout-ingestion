import csv
import ctypes
import enum
import json
import logging
import logging.config
import os
import pickle
import queue
import re
import sqlite3
import sys
import tarfile
import time
import typing
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from math import isqrt
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from helpers import better_downloader, exec_query_from_file
from ingest.ingest_config import IngestionConfig

_SRA_OUTPUT_DIR = "sra"
_SRA_FTP_BASE_URL = "https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata"
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
    output_dir = Path(cfg.output_dir) / _SRA_OUTPUT_DIR
    output_dir.mkdir(exist_ok=True, parents=True)

    date_str = datetime.now().strftime("%Y-%m-%d")
    db_dir = output_dir / "db" / date_str
    db_dir.mkdir(exist_ok=True, parents=True)
    db_file = db_dir / f"sra_metadata_{date_str}.db"

    logging.debug(
        "extracting sra file dumps in range: start_date=%s, end_date=%s",
        cfg.start_date,
        cfg.end_date,
    )
    file_dumps = _extract_sra_file_dumps_in_range(cfg.start_date, cfg.end_date)
    assert len(file_dumps) > 0, (
        f"no sra file dumps found in {cfg.start_date} to {cfg.end_date} range"
    )

    logging.debug("downloading accessions tab file")
    tab_path, to_reprocess_csv = _download_accessions_tab_file(
        output_dir,
        cfg.sra_accessions_ttl,
        cfg.num_workers,
        cfg.chunk_size,
    )

    accessions_csv_path = output_dir / "sra_accessions.csv"
    if to_reprocess_csv:
        logging.debug("converting accessions tab file into csv")
        _convert_tab_to_csv(accessions_csv_path, output_dir, tab_path, cfg.chunk_size)

    logging.debug("downloading sra file dumps")
    dumps_dir = _download_sra_file_dumps(
        file_dumps, output_dir, cfg.num_workers, cfg.chunk_size
    )

    logging.debug("building accession index")
    accessions_idx = _build_accessions_index(
        accessions_csv_path, to_reprocess_csv, output_dir
    )
    tarballs = sorted(dumps_dir.glob(_SRA_TARBALL_GLOB_PAT), reverse=True)

    exec_query_from_file("tables/sql_tables.sql", db_file)
    _process_tarballs(tarballs, accessions_idx, db_file, cfg.num_workers)
    exec_query_from_file("indexes/sql_indexes.sql", db_file)


def _extract_sra_file_dumps_in_range(start_date: date, end_date: date) -> list[str]:
    file_dumps: list[str] = []

    with requests.get(_SRA_FTP_BASE_URL, timeout=30) as r:
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

    for a in soup.find_all("a", href=True):
        href = str(a["href"])
        match = _SRA_TARBALL_RE_PAT.match(href)
        if match is None:
            continue

        file_date = datetime.strptime(match.group(1), "%Y%m%d").date()
        if start_date <= file_date <= end_date:
            file_dumps.append(href)

    return file_dumps


def _download_accessions_tab_file(
    output_dir: Path, accessions_ttl: int, num_workers: int, chunk_size: int
) -> tuple[Path, bool]:
    tab_file = output_dir / _SRA_ACCESSIONS_TAB_FILE_NAME
    to_download = False

    try:
        info = os.stat(tab_file)
        diff = time.time() - info.st_mtime
        if diff > accessions_ttl:
            to_download = True
    except FileNotFoundError:
        logging.info("sra accession tab file is not found. downloading it fresh")
        to_download = True

    if not to_download:
        logging.info(
            "skipped download step for sra accessions tab file as it already exists and it is within the ttl range"
        )
        return tab_file, False

    better_downloader(
        f"{_SRA_FTP_BASE_URL}/{_SRA_ACCESSIONS_TAB_FILE_NAME}",
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
                if len(f) > 6 and f[6] == b"SUBMISSION":
                    outf.write(f[1] + b"," + f[3] + b"," + f[4] + b"\n")

            leftover = chunk[chunk.rfind(b"\n") + 1 :]

        if leftover:
            f = leftover.split(b"\t", 7)
            if len(f) > 6 and f[6] == b"SUBMISSION":
                outf.write(f[1] + b"," + f[3] + b"," + f[4] + b"\n")


def _download_sra_file_dumps(
    file_dumps: list[str], output_dir: Path, num_workers: int, chunk_size: int
) -> Path:
    concurrent_files = min(len(file_dumps), isqrt(num_workers))
    workers_per_file = num_workers // concurrent_files
    dumps_dir = output_dir / "dumps"
    dumps_dir.mkdir(parents=True, exist_ok=True)
    missing_files = [f for f in file_dumps if not (dumps_dir / f).exists()]

    if not missing_files:
        logging.info(
            "skipped download for sra file dumps as all of them are already present"
        )
        return dumps_dir

    logging.info(f"""starting download phase for sra file dumps with following configuration:
- num of file(s) to download: {len(missing_files)}
- num of concurrent file downloads: {concurrent_files}
- num workers per file: {workers_per_file}""")

    def _process_per_file(file: str):
        dest_path = dumps_dir / file
        url = f"{_SRA_FTP_BASE_URL}/{file}"
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
        with open(accessions_idx_pkl_path, "rb") as f:
            data = pickle.load(f)
        return {k: sys.intern(v) for k, v in data.items()}

    with open(accessions_csv_path, "r", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        col = {name: i for i, name in enumerate(header)}
        acc_i, upd_i, pub_i = col["accession"], col["updated_at"], col["published_at"]

        accessions_idx: dict[str, str] = {
            row[acc_i]: sys.intern(row[upd_i] + "|" + row[pub_i]) for row in reader
        }

    with open(accessions_idx_pkl_path, "wb") as f:
        pickle.dump(accessions_idx, f, protocol=pickle.HIGHEST_PROTOCOL)

    return accessions_idx


def _parse_external_ids(external_ids: list) -> str | None:
    if not external_ids:
        return None
    return json.dumps({el.get("namespace"): el.text for el in external_ids})


def _parse_attributes(sample_attrs: list) -> str | None:
    if not sample_attrs:
        return None
    return json.dumps(
        {attr.findtext("TAG"): attr.findtext("VALUE") for attr in sample_attrs}
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
                results[child.tag] = {el.tag: el.text for el in child}

    return json.dumps(results)


def _xml_elem_to_dict(elem) -> dict:
    if elem is None:
        return {}

    result = {}
    for child in elem:
        if len(child) == 0:
            result[child.tag] = child.attrib if child.attrib else child.text
        else:
            result[child.tag] = _xml_elem_to_dict(child)

    return result


def _process_submission(
    submission_accession: str, content: typing.BinaryIO, accessions_idx: dict[str, str]
):
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
        return None

    accession = attrs.get("accession", submission_accession)
    if not accession:
        return None

    dates = accessions_idx.get(accession)
    updated_at, published_at = dates.split("|") if dates else (None, None)

    return (
        accession,
        attrs.get("alias"),
        attrs.get("center_name"),
        attrs.get("lab_name"),
        attrs.get("broker_name"),
        updated_at,
        published_at,
    )


def _process_samples(submission_accession: str, content: typing.BinaryIO):
    ctx = ET.iterparse(content, events=("end",))
    results = []

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

            record = (
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

            results.append(record)
            elem.clear()
    except ET.ParseError:
        return None

    return results


def _process_studies(
    submission_accession: str, content: typing.BinaryIO, accessions_idx: dict[str, str]
):
    ctx = ET.iterparse(content, events=("end",))
    results = []

    try:
        for _, elem in ctx:
            if elem.tag != "STUDY_SET":
                continue

            items = elem.findall("STUDY")

            for study in items:
                accession = study.get("accession")
                descriptor = _xml_elem_to_dict(study.find("DESCRIPTOR"))

                identifiers = study.find("IDENTIFIERS")
                external_ids = (
                    identifiers.findall("EXTERNAL_ID")
                    if identifiers is not None
                    else []
                )
                attrs = study.findall("STUDY_ATTRIBUTES/STUDY_ATTRIBUTE")
                links = study.findall("STUDY_LINKS/STUDY_LINK")

                dates = accessions_idx.get(accession)
                updated_at, published_at = dates.split("|") if dates else (None, None)

                record = (
                    accession,
                    study.get("alias"),
                    study.get("center_name"),
                    descriptor.get("STUDY_ABSTRACT"),
                    descriptor.get("STUDY_TITLE"),
                    _parse_external_ids(external_ids),
                    descriptor.get("STUDY_TYPE", {}).get("existing_study_type"),
                    _parse_attributes(attrs),
                    _parse_links(links),
                    descriptor.get("CENTER_PROJECT_NAME"),
                    submission_accession,
                    published_at,
                    updated_at,
                )

                results.append(record)
                study.clear()
            elem.clear()
            break
    except ET.ParseError:
        return None

    return results


def _process_experiments(submission_accession: str, content: typing.BinaryIO):
    ctx = ET.iterparse(content, events=("end",))
    results = []

    try:
        for _, elem in ctx:
            if elem.tag != "EXPERIMENT_SET":
                continue

            items = elem.findall("EXPERIMENT")

            for experiment in items:
                experiment_accession = experiment.get("accession")

                design_elem = experiment.find("DESIGN")
                design_dict = _xml_elem_to_dict(design_elem)
                library_descriptor = design_dict.get("LIBRARY_DESCRIPTOR", {})
                design_description = design_dict.get("DESIGN_DESCRIPTION")

                library_layout_raw = library_descriptor.get("LIBRARY_LAYOUT", {})
                library_layout = next(iter(library_layout_raw), None)

                library_name = library_descriptor.get("LIBRARY_NAME")
                library_selection = library_descriptor.get("LIBRARY_SELECTION")
                library_source = library_descriptor.get("LIBRARY_SOURCE")
                library_strategy = library_descriptor.get("LIBRARY_STRATEGY")

                samples = []
                if design_elem is not None:
                    for sample in design_elem.findall("SAMPLE_DESCRIPTOR"):
                        sample_accession = sample.get("accession")
                        if sample_accession:
                            samples.append(sample_accession)

                platform_elem = experiment.find("PLATFORM")
                platform_dict = _xml_elem_to_dict(platform_elem)
                platform = next(iter(platform_dict), None)
                instrument_model = (
                    platform_dict.get(platform, {}).get("INSTRUMENT_MODEL")
                    if platform
                    else None
                )

                study_ref_elem = experiment.find("STUDY_REF")
                study_ref = (
                    study_ref_elem.get("accession")
                    if study_ref_elem is not None
                    else None
                )

                title = experiment.findtext("TITLE")

                record = (
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

                results.append(record)
                experiment.clear()
            elem.clear()
            break
    except ET.ParseError:
        return None

    return results


def _process_runs(submission_accession: str, content: typing.BinaryIO):
    ctx = ET.iterparse(content, events=("end",))
    results = []

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
                record = (run.get("accession"), experiment_ref, submission_accession)

                results.append(record)
                run.clear()
            elem.clear()
            break
    except ET.ParseError:
        return None

    return results


def _convert_file_name_to_type(file_name: str) -> FileType | None:
    if file_name.endswith(".submission.xml"):
        return FileType.Submission
    elif file_name.endswith(".sample.xml"):
        return FileType.Sample
    elif file_name.endswith(".study.xml"):
        return FileType.Study
    elif file_name.endswith(".experiment.xml"):
        return FileType.Experiment
    elif file_name.endswith(".run.xml"):
        return FileType.Run
    else:
        return None


def _reader_worker(
    tarball: Path,
    transform_queue: queue.Queue,
    pbar: tqdm,
    read_batch_size: int = 50,
):
    batch: list = []

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

                    batch.append((submission_accession, file_type, f))
                    pbar.update(1)

                    if len(batch) >= read_batch_size:
                        transform_queue.put(batch)
                        batch = []
                except Exception as e:
                    logging.error(f"[reader] failed to read {name} in {tarball}: {e}")

        if batch:
            transform_queue.put(batch)

    except Exception as e:
        logging.error(f"[reader] failed to open tarball {tarball}: {e}")
        raise


def _transform_worker(
    transform_queue: queue.Queue,
    write_queue: queue.Queue,
    accessions_idx: dict[str, str],
):
    while True:
        try:
            batch = transform_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        if batch is _STOP:
            write_queue.put(_STOP)
            break

        for submission_accession, file_type, content in batch:
            try:
                result = None

                if file_type == FileType.Submission:
                    result = _process_submission(
                        submission_accession, content, accessions_idx
                    )
                elif file_type == FileType.Sample:
                    result = _process_samples(submission_accession, content)
                elif file_type == FileType.Study:
                    result = _process_studies(
                        submission_accession, content, accessions_idx
                    )
                elif file_type == FileType.Experiment:
                    result = _process_experiments(submission_accession, content)
                elif file_type == FileType.Run:
                    result = _process_runs(submission_accession, content)
                else:
                    logging.error(
                        f"[transform] received invalid file type: {file_type}"
                    )

                if result is None:
                    continue

                rows = result if isinstance(result, list) else [result]
                for row in rows:
                    write_queue.put((file_type, row))

            except Exception as e:
                logging.error(
                    f"[transform] failed to process {submission_accession}: {e}"
                )


def _writer_worker(
    write_queue: queue.Queue,
    db_file: Path,
    sentinels_to_receive: int,
    write_batch_size: int = 500,
):
    conn = sqlite3.connect(str(db_file))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    cursor = conn.cursor()
    sentinels_received = 0
    rows_written = 0
    batch_by_type: dict[FileType, list] = {}

    sql_statements = {
        FileType.Submission: "INSERT OR IGNORE INTO sra_submissions (accession, alias, center_name, lab_name, broker_name, updated_at, published_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        FileType.Sample: "INSERT OR IGNORE INTO sra_samples (accession, alias, description, title, scientific_name, taxon_id, external_id, attributes_json, submission, links) VALUES (?,?,?,?,?,?,?,?,?,?)",
        FileType.Study: "INSERT OR IGNORE INTO sra_studies (accession, alias, center_name, abstract, title, external_id, study_type, attributes_json, links, center_project_name, submission, published_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        FileType.Experiment: "INSERT OR IGNORE INTO sra_experiments (accession, design_description, library_layout, library_name, library_selection, library_source, library_strategy, samples, platform, instrument_model, study, title, submission) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        FileType.Run: "INSERT OR IGNORE INTO sra_runs (accession, experiment, submission) VALUES (?,?,?)",
    }

    def _flush(batch: list, file_type: FileType):
        nonlocal rows_written
        if batch:
            cursor.executemany(sql_statements[file_type], batch)
            conn.commit()
            rows_written += len(batch)
            batch_by_type[file_type] = []

    while True:
        try:
            item = write_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        if item is _STOP:
            sentinels_received += 1
            if sentinels_received == sentinels_to_receive:
                for file_type, batch in batch_by_type.items():
                    _flush(batch, file_type)
                break
            continue

        file_type, record = item
        if file_type not in batch_by_type:
            batch_by_type[file_type] = []
        batch_by_type[file_type].append(record)

        if len(batch_by_type[file_type]) >= write_batch_size:
            _flush(batch_by_type[file_type], file_type)

    conn.close()


def _process_tarballs(
    tarballs: list[Path],
    accessions_idx: dict[str, str],
    db_file: Path,
    num_workers: int,
):
    num_readers = max(1, min(len(tarballs), num_workers // 4))
    num_writer = 1
    num_transform = max(1, num_workers - num_readers - num_writer)

    transform_queue: queue.Queue = queue.Queue()
    write_queue: queue.Queue = queue.Queue()

    pbar = tqdm(
        total=len(accessions_idx),
        unit="file",
        desc="processing tarballs",
        mininterval=2.0,
    )

    with (
        ThreadPoolExecutor(num_readers) as reader_pool,
        ThreadPoolExecutor(num_transform) as transform_pool,
        ThreadPoolExecutor(num_writer) as writer_pool,
    ):
        write_future = writer_pool.submit(
            _writer_worker, write_queue, db_file, num_transform
        )
        transform_futures = [
            transform_pool.submit(
                _transform_worker,
                transform_queue,
                write_queue,
                accessions_idx,
            )
            for _ in range(num_transform)
        ]
        reader_futures = [
            reader_pool.submit(_reader_worker, tb, transform_queue, pbar)
            for tb in tarballs
        ]

        for f in as_completed(reader_futures):
            try:
                f.result()
            except Exception as e:
                logging.exception(f"[reader] a reader thread failed: {e}")

        for _ in range(num_transform):
            transform_queue.put(_STOP)

        for f in as_completed(transform_futures):
            f.result()
        write_future.result()

    pbar.close()
