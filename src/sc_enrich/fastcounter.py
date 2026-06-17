import csv
import dataclasses
import functools
import gzip
import json
import logging
import shutil
import sqlite3
import tarfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import StrEnum
from pathlib import Path
from typing import Callable

import h5py

from helpers import simple_file_download


class _FileType(StrEnum):
    Barcodes = "barcodes"
    H5 = "h5"
    H5ad = "h5ad"
    Csv = "csv"  # comma delimited
    Tsv = "tsv"  # tab delimited
    Txt = "txt"  # newline delimited
    SpaceDelimited = "space_delimited"  # space delimited
    SemicolonDelimited = "semicolon_delimited"  # semicolon delimited
    Tar = "tar"
    Unknown = "unknown"


POSSIBLE_MINOMER_FILE_TYPES = [_FileType.Csv, _FileType.Tsv, _FileType.Txt]


@dataclasses.dataclass
class _DownloadLink:
    type: _FileType
    url: str

    def _parse_url(self) -> str:
        return self.url.replace("ftp://", "https://")

    def _extract_filename(self) -> str:
        return self.url.split("/")[-1]


_SC_ENRICH_OUTPUT_DIR = "sc_enrich"


def sc_fastcounter(
    base_output_dir: Path,
    unified_metadata_db_file: Path,
    geo_db_file: Path,
    ae_db_file: Path,
    num_workers: int,
    chunk_size: int,
    retries: int,
    timeout: int,
    max_timeout: int,
):
    output_dir = base_output_dir / _SC_ENRICH_OUTPUT_DIR
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

    # list containing geo/ae accession or a tuple of sra/ena accession with corresponding geo/ae alias
    accessions: list[str | tuple[str, str]] = []
    # list of sra/ena accessions for which aliases are need to be searched
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

    # set of unique geo/ae accessions which are utilized to fetch the download links for files with n_cell information
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

    download_links_mapping: dict[str, list[_DownloadLink]] = defaultdict(
        list[_DownloadLink]
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
                if file_type == _FileType.Unknown:
                    continue

                download_links_mapping[gse].append(
                    _DownloadLink(type=file_type, url=url)
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
                if file_type == _FileType.Tar:
                    download_links_mapping[series_accession].append(
                        _DownloadLink(type=file_type, url=url)
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
                if file_type == _FileType.Unknown:
                    continue

                download_links_mapping[series_accession].append(
                    _DownloadLink(type=file_type, url=url)
                )

    accession_sc_result: dict[str, bool] = {}
    accession_dest_path_mapping: dict[str, tuple[Path, _FileType]] = {}

    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        futures = {}
        downloader = functools.partial(
            simple_file_download,
            chunk_size=chunk_size,
            retries=retries,
            timeout=timeout,
            max_timeout=max_timeout,
            with_pbar=True,
        )

        for accession, links in download_links_mapping.items():
            if len(links) == 0:
                continue

            for link in links:
                dest_path = output_dir / link._extract_filename()
                file_type = link.type

                if dest_path.exists():
                    accession_dest_path_mapping[accession] = (dest_path, file_type)
                    continue

                future = pool.submit(
                    downloader,
                    url=link._parse_url(),
                    dest_path=dest_path,
                )
                futures[future] = (accession, link)

        for f in as_completed(futures):
            try:
                dest_path = f.result()
                accession, link = futures[f]
                accession_dest_path_mapping[accession] = (dest_path, link.type)
            except Exception as e:
                logging.error("failed to download %s: %s", link.url, e)

    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        futures = {}

        for accession, (dest_path, file_type) in accession_dest_path_mapping.items():
            if file_type in POSSIBLE_MINOMER_FILE_TYPES:
                file_type, dest_path = _file_type_sniffer(dest_path, chunk_size)
                if file_type == _FileType.Unknown:
                    logging.debug("unable to find file type for %s", dest_path)
                    continue

            future = pool.submit(_is_sc, dest_path, file_type, chunk_size)
            futures[future] = (accession, dest_path)

        for f in as_completed(futures):
            is_sc = f.result()
            accession, dest_path = futures[f]
            print(f"{accession}: {is_sc} ({dest_path})")
            accession_sc_result[accession] = is_sc

    _db_flush()


def _contains_any(s: str, substrings: list[str]) -> bool:
    return any(sub in s for sub in substrings)


def _classify_file(path: str) -> _FileType:
    # list of words to ignore while assignment csv, tsv and txt file types
    ignore_words = [
        "barcode",
        "feature",
        "gene",
        "readme",
        "annotation",
        "metadata",
        "md5sum",
    ]

    if "barcodes.tsv" in path:
        return _FileType.Barcodes
    elif ".ha5d" in path:
        return _FileType.H5ad
    elif ".h5" in path:
        return _FileType.H5
    elif ".csv" in path and not _contains_any(path, ignore_words):
        return _FileType.Csv
    elif ".tsv" in path and not _contains_any(path, ignore_words):
        return _FileType.Tsv
    elif ".txt" in path and not _contains_any(path, ignore_words):
        return _FileType.Txt
    elif (".tar" in path) or (".tar.gz" in path) or (".tgz" in path):
        return _FileType.Tar
    else:
        return _FileType.Unknown


# util function to read a sample of the file and extract the exact file type rather than just guessing it from the file extension
def _file_type_sniffer(path: Path, chunk_size: int) -> tuple[_FileType, Path]:
    if path.suffix == ".gz":
        tmp_path = path.with_suffix("")

        if not tmp_path.exists():
            with gzip.open(path, "rb") as f_in, open(tmp_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        path = tmp_path

    try:
        with open(path, "r") as f:
            sample = f.read(chunk_size)
    except UnicodeDecodeError as e:
        logging.warning("failed to read %s: %s", path, e)
        return _FileType.Unknown, path

    try:
        dialect = csv.Sniffer().sniff(sample)
        reader = csv.reader(sample.splitlines(), dialect)
        row = next(reader)
    except csv.Error:
        row = sample.splitlines()[0].split("\t")
        dialect = None

    row = [r for r in row if r.strip()]

    if dialect is None:
        return _FileType.Unknown, path
    if len(row) == 1:
        return _FileType.Txt, path
    elif dialect.delimiter == "\t":
        return _FileType.Tsv, path
    elif dialect.delimiter == ",":
        return _FileType.Csv, path
    elif dialect.delimiter == " ":
        return _FileType.SpaceDelimited, path
    elif dialect.delimiter == ";":
        return _FileType.SemicolonDelimited, path
    else:
        return _FileType.Unknown, path


def _is_sc(dest_path: Path, file_type: _FileType, chunk_size: int) -> bool:
    if file_type == _FileType.Tar:
        return _process_tar(dest_path, chunk_size)
    else:
        match file_type:
            case _FileType.Barcodes:
                return _process_barcodes(dest_path, chunk_size)
            case _FileType.H5:
                return _process_h5(dest_path, chunk_size)
            case _FileType.H5ad:
                return _process_h5ad(dest_path, chunk_size)
            case _FileType.Tsv:
                return _process_delimiter_file_types("\t")(dest_path, chunk_size)
            case _FileType.Csv:
                return _process_delimiter_file_types(",")(dest_path, chunk_size)
            case _FileType.SpaceDelimited:
                return _process_delimiter_file_types(" ")(dest_path, chunk_size)
            case _FileType.SemicolonDelimited:
                return _process_delimiter_file_types(";")(dest_path, chunk_size)

    return False


def safe_process(func):
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
                if file_type == _FileType.Unknown:
                    continue

                f = tar.extractfile(member)
                if f is None:
                    continue

                raw = f.read()
                if name.endswith(".gz"):
                    raw = gzip.decompress(raw)

                is_sc = _is_sc(path, file_type, chunk_size)
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
        # recursively try to find barcodes
        def find_barcodes(name, obj):
            nonlocal n_cells
            if n_cells is not None:
                return
            if isinstance(obj, h5py.Dataset) and name.endswith("/barcodes"):
                n_cells = len(obj)

        f.visititems(find_barcodes)

    return n_cells is not None and n_cells > 1


@safe_process
def _process_h5ad(path: Path, _: int) -> bool:
    n_cells = None

    with h5py.File(path, "r") as f:
        if "obs/_index" in f:
            n_cells = len(f["obs/_index"])

        if "X" in f:
            n_cells = f["X"].shape[0]

    return n_cells is not None and n_cells > 1


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
