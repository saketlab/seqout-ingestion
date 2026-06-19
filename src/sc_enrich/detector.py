import csv
import dataclasses
import functools
import gzip
import json
import logging
import shutil
import sqlite3
import tarfile
import tempfile
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from enum import StrEnum
from pathlib import Path
from typing import Callable

import h5py
import pyreadr
from tqdm import tqdm

from helpers import simple_file_download


class FileType(StrEnum):
    Barcodes = "barcodes"
    H5 = "h5"
    H5ad = "h5ad"
    Mtx = "mtx"
    Csv = "csv"
    Tsv = "tsv"
    Txt = "txt"
    SpaceDelimited = "space_delimited"
    SemicolonDelimited = "semicolon_delimited"
    Rds = "rds"
    Tar = "tar"
    Unknown = "unknown"


@dataclasses.dataclass
class DownloadLink:
    type: FileType
    url: str

    def _parse_url(self) -> str:
        return self.url.replace("ftp://", "https://")

    def _extract_filename(self) -> str:
        return self.url.split("/")[-1]


SC_ENRICH_OUTPUT_DIR = "sc_enrich"
POSSIBLE_MISNOMER_FILE_TYPES = [FileType.Csv, FileType.Tsv, FileType.Txt]
FILE_TYPE_PRIORITY = [
    FileType.Barcodes,
    FileType.H5,
    FileType.H5ad,
    FileType.Tar,
    FileType.Txt,
    FileType.Mtx,
    FileType.Tsv,
    FileType.Csv,
    FileType.SpaceDelimited,
    FileType.SemicolonDelimited,
    FileType.Rds,
]


def sc_detector(
    base_output_dir: Path,
    unified_metadata_db_file: Path,
    geo_db_file: Path,
    ae_db_file: Path,
    num_workers: int,
    chunk_size: int,
    retries: int,
    timeout: int,
    max_timeout: int,
    batch_size: int = 100,
):
    output_dir = base_output_dir / SC_ENRICH_OUTPUT_DIR
    output_dir.mkdir(exist_ok=True, parents=True)

    conn = sqlite3.connect(unified_metadata_db_file)
    geo_conn = sqlite3.connect(geo_db_file)
    ae_conn = sqlite3.connect(ae_db_file)

    def _db_flush():
        ae_conn.close()
        geo_conn.close()
        conn.close()

    rows: list[tuple[str, str]] = conn.execute(
        "SELECT canonical_accession, source FROM unified_metadata WHERE is_single_cell = '1';"
    ).fetchall()

    accessions: list[str | tuple[str, str]] = []
    search_for_aliases: list[str] = []

    for accession, source in rows:
        if source != "geo" and source != "ae":
            search_for_aliases.append(accession)
        else:
            accessions.append(accession)

    placeholders = ", ".join(repr(acc) for acc in search_for_aliases)
    rows: list[tuple[str, str]] = conn.execute(
        f"SELECT canonical_accession, aliases FROM unified_metadata WHERE canonical_accession IN ({placeholders}) GROUP BY canonical_accession"
    ).fetchall()

    for accession, aliases_json in rows:
        aliases: dict[str, str] = json.loads(aliases_json)
        geo_alias = aliases.get("geo")
        arrayexpress_alias = aliases.get("arrayexpress")

        if geo_alias:
            accessions.append((accession, geo_alias))
        elif arrayexpress_alias:
            accessions.append((accession, arrayexpress_alias))

    accessions_to_process: set[str] = set()

    for v in accessions:
        if isinstance(v, str):
            accessions_to_process.add(v)
        elif isinstance(v, tuple):
            accessions_to_process.add(v[1])

    gsm_to_gse: dict[str, str] = {}

    placeholders = ", ".join(
        repr(acc) for acc in accessions_to_process if acc.startswith("GSE")
    )
    rows: list[tuple[str, str]] = geo_conn.execute(
        f"SELECT accession, samples_ref FROM geo_series WHERE accession IN ({placeholders}) GROUP BY accession;"
    ).fetchall()

    for accession, samples_ref_json in rows:
        if not samples_ref_json:
            continue

        samples_ref: list[dict[str, str]] = json.loads(samples_ref_json)
        for v in samples_ref:
            gsm_to_gse[v["@ref"]] = accession

    download_links_mapping: dict[str, list[DownloadLink]] = defaultdict(
        list[DownloadLink]
    )

    all_gsms = list(gsm_to_gse.keys())
    if all_gsms:
        placeholders = ", ".join(repr(g) for g in all_gsms)
        rows: list[tuple[str, str]] = geo_conn.execute(
            f"SELECT accession, supplementary_data FROM geo_samples WHERE accession IN ({placeholders})"
        ).fetchall()

        for gsm_accession, supl_data_json in rows:
            if not supl_data_json:
                continue

            gse = gsm_to_gse.get(gsm_accession)
            if gse is None:
                continue

            supl_data: list[dict[str, str]] = json.loads(supl_data_json)
            for item in supl_data:
                url = item.get("#text")
                if not url:
                    continue

                file_type = _classify_file(url)
                if file_type == FileType.Unknown:
                    continue

                download_links_mapping[gse].append(
                    DownloadLink(type=file_type, url=url)
                )

    for series_accession in accessions_to_process:
        if series_accession.startswith("GSE"):
            if len(download_links_mapping.get(series_accession, [])) > 0:
                continue

            rows = geo_conn.execute(
                "SELECT supplementary_data FROM geo_series WHERE accession = ?",
                (series_accession,),
            ).fetchone()
            if rows is None:
                continue

            supl_data_json = rows[0]
            if supl_data_json is None:
                continue

            supl_data = json.loads(supl_data_json)
            for item in supl_data:
                url = item.get("#text")
                if url is None:
                    continue

                file_type = _classify_file(url)
                if file_type == FileType.Tar:
                    download_links_mapping[series_accession].append(
                        DownloadLink(type=file_type, url=url)
                    )
                    break
        else:
            row = ae_conn.execute(
                "SELECT processed_data_files FROM arrayexpress_experiments WHERE accession = ?",
                (series_accession,),
            ).fetchone()
            if row is None:
                continue

            processed_data_files_json = row[0]
            if processed_data_files_json is None:
                continue

            processed_data_files = json.loads(processed_data_files_json)
            for item in processed_data_files:
                url = item.get("url")
                if url is None:
                    continue

                file_type = _classify_file(url)
                if file_type == FileType.Unknown:
                    continue

                download_links_mapping[series_accession].append(
                    DownloadLink(type=file_type, url=url)
                )

    for links in download_links_mapping.values():
        links.sort(key=lambda x: FILE_TYPE_PRIORITY.index(x.type))

    pbar = tqdm(
        total=len(download_links_mapping.keys()),
        desc="processing accessions",
        unit="acc",
    )
    pbar_accessions: set[str] = set()
    accession_links: dict[str, list[DownloadLink]] = dict(download_links_mapping)

    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        download_futures: dict[Future, tuple[str, int]] = {}
        process_futures: dict[Future, tuple[str, Path, FileType, int]] = {}
        downloaded_paths: set[Path] = set()
        downloader = functools.partial(
            simple_file_download,
            chunk_size=chunk_size,
            retries=retries,
            timeout=timeout,
            max_timeout=max_timeout,
            with_pbar=False,
        )
        accessions_to_update: list[str] = []
        done_accessions: set[str] = set()

        def _try_links(accession: str, start_idx: int):
            links = accession_links.get(accession) or []
            for idx in range(start_idx, len(links)):
                link = links[idx]
                dest_path = output_dir / link._extract_filename()

                if dest_path.exists():
                    result = _resolve_file_type(dest_path, link.type, chunk_size)
                    if result is None:
                        logging.debug("unable to find file type for %s", dest_path)
                        continue
                    file_type, sniffed_path = result
                    process_futures[
                        pool.submit(_is_sc, sniffed_path, file_type, chunk_size)
                    ] = (accession, dest_path, file_type, idx)
                    return

                future = pool.submit(
                    downloader, url=link._parse_url(), dest_path=dest_path
                )
                download_futures[future] = (accession, idx)
                return

            done_accessions.add(accession)
            accessions_to_update.append(accession)

            if len(accessions_to_update) >= batch_size:
                _flush_updates(conn, accessions_to_update)
                accessions_to_update.clear()

            if accession not in pbar_accessions:
                pbar_accessions.add(accession)
                pbar.update(1)

        for accession, links in sorted(
            accession_links.items(), key=lambda x: len(x[1])
        ):
            if not links:
                done_accessions.add(accession)
                continue
            if accession not in done_accessions:
                _try_links(accession, 0)

        while download_futures or process_futures:
            pending: dict[Future, object] = {}
            pending.update(download_futures)
            pending.update(process_futures)
            done, _ = wait(pending, return_when=FIRST_COMPLETED)

            for f in done:
                if f in download_futures:
                    accession, link_idx = download_futures.pop(f)
                    try:
                        dest_path = f.result()
                        link = accession_links[accession][link_idx]
                        result = _resolve_file_type(dest_path, link.type, chunk_size)
                        if result is None:
                            logging.debug("unable to find file type for %s", dest_path)
                            _try_links(accession, link_idx + 1)
                            continue

                        file_type, sniffed_path = result
                        process_futures[
                            pool.submit(_is_sc, sniffed_path, file_type, chunk_size)
                        ] = (accession, dest_path, file_type, link_idx)
                        downloaded_paths.add(dest_path)
                    except Exception as e:
                        logging.error("failed to download %s: %s", link.url, e)
                        _try_links(accession, link_idx + 1)

                if f in process_futures:
                    is_sc = f.result()
                    accession, dest_path, file_type, link_idx = process_futures.pop(f)

                    if dest_path in downloaded_paths:
                        dest_path.unlink(missing_ok=True)

                    if accession in done_accessions:
                        continue

                    if is_sc:
                        done_accessions.add(accession)
                        if accession not in pbar_accessions:
                            pbar_accessions.add(accession)
                            pbar.update(1)
                    else:
                        _try_links(accession, link_idx + 1)

        if accessions_to_update:
            _flush_updates(conn, accessions_to_update)

    pbar.close()
    _db_flush()
    shutil.rmtree(output_dir, ignore_errors=True)


def _flush_updates(conn: sqlite3.Connection, accessions: list[str]) -> None:
    placeholders = ", ".join(repr(acc) for acc in accessions)
    conn.execute(
        f"UPDATE unified_metadata SET is_single_cell = 0, single_cell_modality = '' WHERE accession IN ({placeholders})"
    )
    conn.commit()


def _contains_any(s: str, substrings: list[str]) -> bool:
    return any(sub in s for sub in substrings)


def _classify_file(path: str) -> FileType:
    ignore_words = [
        "barcode",
        "feature",
        "gene",
        "readme",
        "annotation",
        "metadata",
        "md5sum",
    ]

    def _is_type(ext: str) -> bool:
        return ext in path and not _contains_any(path, ignore_words)

    if "barcodes.tsv" in path:
        return FileType.Barcodes
    if ".h5ad" in path:
        return FileType.H5ad
    if _contains_any(path, [".h5", ".hdf5"]):
        return FileType.H5
    if "matrix.mtx" in path or _contains_any(path, [".mtx", ".mtx.gz"]):
        return FileType.Mtx
    if _is_type(".txt"):
        return FileType.Txt
    if _is_type(".csv"):
        return FileType.Csv
    if _is_type(".tsv"):
        return FileType.Tsv
    if ".rds" in path:
        return FileType.Rds
    if _contains_any(path, [".tar", ".tgz"]):
        return FileType.Tar
    return FileType.Unknown


def _resolve_file_type(
    path: Path, file_type: FileType, chunk_size: int
) -> tuple[FileType, Path] | None:
    if path.suffix == ".gz":
        tmp_path = path.with_suffix("")
        if not tmp_path.exists():
            with gzip.open(path, "rb") as f_in, open(tmp_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        path = tmp_path
    if file_type not in POSSIBLE_MISNOMER_FILE_TYPES:
        return file_type, path
    resolved_type, resolved_path = _file_type_sniffer(path, chunk_size)
    if resolved_type == FileType.Unknown:
        return None
    return resolved_type, resolved_path


def _file_type_sniffer(path: Path, chunk_size: int) -> tuple[FileType, Path]:
    try:
        with open(path, "r") as f:
            sample = f.read(chunk_size)
    except UnicodeDecodeError as e:
        logging.debug("failed to read %s: %s", path, e)
        return FileType.Unknown, path

    try:
        dialect = csv.Sniffer().sniff(sample)
        reader = csv.reader(sample.splitlines(), dialect)
        row = next(reader)
    except csv.Error:
        row = sample.splitlines()[0].split("\t")
        dialect = None

    row = [r for r in row if r.strip()]

    if dialect is None:
        return FileType.Unknown, path
    if len(row) == 1:
        return FileType.Txt, path
    if dialect.delimiter == "\t":
        return FileType.Tsv, path
    elif dialect.delimiter == ",":
        return FileType.Csv, path
    elif dialect.delimiter == " ":
        return FileType.SpaceDelimited, path
    elif dialect.delimiter == ";":
        return FileType.SemicolonDelimited, path
    else:
        return FileType.Txt, path


def _is_sc(dest_path: Path, file_type: FileType, chunk_size: int) -> bool:
    if file_type == FileType.Barcodes:
        return _process_barcodes(dest_path, chunk_size)
    elif file_type == FileType.H5:
        return _process_h5(dest_path, chunk_size)
    elif file_type == FileType.H5ad:
        return _process_h5ad(dest_path, chunk_size)
    elif file_type == FileType.Tar:
        return _process_tar(dest_path, chunk_size)
    elif file_type == FileType.Txt:
        return _process_txt(dest_path, chunk_size)
    elif file_type == FileType.Mtx:
        return _process_mtx(dest_path, chunk_size)
    elif file_type == FileType.Tsv:
        return _process_delimiter_file_types("\t")(dest_path, chunk_size)
    elif file_type == FileType.Csv:
        return _process_delimiter_file_types(",")(dest_path, chunk_size)
    elif file_type == FileType.SpaceDelimited:
        return _process_delimiter_file_types(" ")(dest_path, chunk_size)
    elif file_type == FileType.SemicolonDelimited:
        return _process_delimiter_file_types(";")(dest_path, chunk_size)
    elif file_type == FileType.Rds:
        return _process_rds(dest_path, chunk_size)

    return False


def safe_process(func: Callable[[Path, int], bool]) -> Callable[[Path, int], bool]:
    @functools.wraps(func)
    def wrapper(path: Path, chunk_size: int, *args, **kwargs):
        try:
            return func(path, chunk_size, *args, **kwargs)
        except Exception as e:
            logging.error("failed to process %s: %s", path, e)
            return True

    return wrapper


def _process_tar(path: Path, chunk_size: int) -> bool:
    try:
        with tarfile.open(path, "r:*") as tar:
            for member in tar.getmembers():
                name = member.name.lower()
                file_type = _classify_file(name)
                if file_type == FileType.Unknown:
                    continue

                f = tar.extractfile(member)
                if f is None:
                    continue

                member_path = Path(member.name)
                suffix = "".join(member_path.suffixes) or ".tmp"
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                    shutil.copyfileobj(f, tmp)
                    tmp_path = Path(tmp.name)

                result = _resolve_file_type(tmp_path, file_type, chunk_size)
                if result is None:
                    continue
                file_type, _ = result

                is_sc = _is_sc(tmp_path, file_type, chunk_size)
                Path(tmp_path).unlink(missing_ok=True)
                if is_sc:
                    return True

        return False
    except Exception as e:
        logging.error("failed to process %s: %s", path, e)
        return True


@safe_process
def _process_barcodes(path: Path, chunk_size: int) -> bool:
    with open(path, "rb") as f:
        chunk = f.read(chunk_size)
    idx = chunk.find(b"\n")
    return idx != -1 and len(chunk) > idx + 1


@safe_process
def _process_h5(path: Path, _: int) -> bool:
    n_cells = None

    with h5py.File(path, "r") as f:

        def find_barcodes(name, obj):
            nonlocal n_cells
            if n_cells is not None:
                return
            if obj is not None and name.endswith("/barcodes"):
                try:
                    n_cells = len(obj)
                except TypeError:
                    pass

        f.visititems(find_barcodes)

    return n_cells is not None and n_cells > 1


@safe_process
def _process_h5ad(path: Path, _: int) -> bool:
    n_cells = None

    with h5py.File(path, "r") as f:
        if "obs/_index" in f:
            n_cells = len(f["obs/_index"])
        elif "X" in f:
            n_cells = f["X"].shape[0]

    return n_cells is not None and n_cells > 1


@safe_process
def _process_txt(path: Path, chunk_size: int) -> bool:
    with open(path, "rb") as f:
        chunk = f.read(chunk_size)
    lines = chunk.strip().splitlines()
    return len(lines) > 2


@safe_process
def _process_mtx(path: Path, chunk_size: int) -> bool:
    with open(path, "rb") as f:
        header = f.read(chunk_size)

    for line in header.splitlines():
        if line.startswith(b"%"):
            continue
        parts = line.split()
        if len(parts) >= 2:
            try:
                n_cells = int(parts[1])
                return n_cells > 1
            except (ValueError, IndexError):
                return False

    return False


def _process_delimiter_file_types(delimiter: str) -> Callable[[Path, int], bool]:
    @safe_process
    def _wrapper(path: Path, chunk_size: int) -> bool:
        with open(path, "rb") as f:
            chunk = f.read(chunk_size)

        first_newline = chunk.find(b"\n")
        header = (
            chunk[:first_newline].decode().split(delimiter)
            if first_newline != -1
            else chunk.decode().split(delimiter)
        )
        return len(header) > 3

    return _wrapper


@safe_process
def _process_rds(path: Path, _: int) -> bool:
    result = pyreadr.read_r(path)
    df = list(result.values())[0]
    n_cells = df.shape[1]
    return n_cells > 1
