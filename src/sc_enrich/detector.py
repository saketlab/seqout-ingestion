import csv
import dataclasses
import functools
import gzip
import json
import logging
import shutil
import signal
import sqlite3
import sys
import tarfile
import tempfile
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from enum import StrEnum
from pathlib import Path
from typing import Callable

import h5py
import pyreadr
import rpy2.robjects as ro
from pyreadr.custom_errors import LibrdataError
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
MIN_DISK_SPACE_BYTES = 5 * 1024**3
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
    batch_size: int,
):
    output_dir = base_output_dir / SC_ENRICH_OUTPUT_DIR
    if output_dir.exists():
        shutil.rmtree(output_dir, ignore_errors=True)
    output_dir.mkdir(exist_ok=True, parents=True)

    if not _check_disk_space(output_dir):
        logging.error(
            "insufficient disk space at %s: cannot start sc_enrich detector",
            output_dir,
        )
        return

    for db_path in (unified_metadata_db_file, geo_db_file, ae_db_file):
        if not _check_disk_space(db_path.parent):
            logging.error(
                "insufficient disk space at %s: database operations may fail",
                db_path.parent,
            )
            return

    def _cleanup(signum, frame):
        shutil.rmtree(output_dir, ignore_errors=True)
        sys.exit(signum)

    signal.signal(signal.SIGINT, _cleanup)
    signal.signal(signal.SIGTERM, _cleanup)

    conn = sqlite3.connect(unified_metadata_db_file)
    geo_conn = sqlite3.connect(geo_db_file)
    ae_conn = sqlite3.connect(ae_db_file)

    columns = [row[1] for row in conn.execute("PRAGMA table_info(unified_metadata)")]
    if "single_cell_count" not in columns:
        conn.execute(
            "ALTER TABLE unified_metadata ADD COLUMN single_cell_count INTEGER DEFAULT 0;"
        )

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

    rows = _query_in(
        conn,
        "SELECT canonical_accession, aliases FROM unified_metadata WHERE canonical_accession",
        search_for_aliases,
        batch_size,
        "GROUP BY canonical_accession",
    )

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

    gse_list = [acc for acc in accessions_to_process if acc.startswith("GSE")]
    try:
        rows = _query_in(
            geo_conn,
            "SELECT accession, samples_ref FROM geo_series WHERE accession",
            gse_list,
            batch_size,
            "GROUP BY accession",
        )
    except sqlite3.OperationalError:
        logging.error("database error querying geo_series (disk may be full)")
        rows = []

    for accession, samples_ref_json in rows:
        if not samples_ref_json:
            continue

        samples_ref = json.loads(samples_ref_json)
        for v in samples_ref:
            if isinstance(v, dict):
                gsm_to_gse[v["@ref"]] = accession
            elif isinstance(v, str):
                gsm_to_gse[v] = accession
            else:
                raise ValueError(
                    "samples_ref of geo_series table is of either list[str] of list[dict] type"
                )

    download_links_mapping: dict[str, list[DownloadLink]] = defaultdict(
        list[DownloadLink]
    )

    all_gsms = list(gsm_to_gse.keys())
    if all_gsms:
        rows = _query_in(
            geo_conn,
            "SELECT accession, supplementary_data FROM geo_samples WHERE accession",
            all_gsms,
            batch_size,
        )

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

    download_workers = min(
        num_workers // 2, 8
    )  # 8 parallel download workers works very well with NCBI and EBI FTP server without hitting much rate limit errors
    process_workers = num_workers - download_workers

    with (
        ThreadPoolExecutor(max_workers=download_workers) as download_pool,
        ThreadPoolExecutor(max_workers=process_workers) as process_pool,
    ):
        download_futures: dict[Future, tuple[str, int]] = {}
        process_futures: dict[Future, tuple[str, Path, Path, FileType, int]] = {}
        downloaded_paths: set[Path] = set()
        downloader = functools.partial(
            simple_file_download,
            chunk_size=chunk_size,
            num_retries=retries,
            timeout=timeout,
            max_wait=max_timeout,
            with_pbar=False,
        )
        updates: list[tuple[str, int]] = []
        done_accessions: set[str] = set()

        def _try_links(accession: str, start_idx: int):
            links = accession_links.get(accession) or []
            for idx in range(start_idx, len(links)):
                link = links[idx]
                dest_path = output_dir / accession / link._extract_filename()

                if dest_path.exists():
                    result = _resolve_file_type(dest_path, link.type, chunk_size)
                    if result is None:
                        logging.debug("unable to find file type for %s", dest_path)
                        decompressed = dest_path.with_suffix("")
                        if decompressed != dest_path:
                            decompressed.unlink(missing_ok=True)
                        continue

                    file_type, sniffed_path = result
                    process_futures[
                        process_pool.submit(_is_sc, sniffed_path, file_type, chunk_size)
                    ] = (accession, dest_path, sniffed_path, file_type, idx)
                    return

                if not _check_disk_space(output_dir):
                    logging.warning("skipping %s: insufficient disk space", link.url)
                    _try_links(accession, idx + 1)
                    return

                future = download_pool.submit(
                    downloader, url=link._parse_url(), dest_path=dest_path
                )
                download_futures[future] = (accession, idx)
                return

            done_accessions.add(accession)
            updates.append((accession, 0))

            if len(updates) >= batch_size:
                _flush_updates(conn, updates)
                updates.clear()

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
                    link = accession_links[accession][link_idx]

                    try:
                        dest_path = f.result()
                        result = _resolve_file_type(dest_path, link.type, chunk_size)
                        if result is None:
                            logging.debug("unable to find file type for %s", dest_path)
                            dest_path.unlink(missing_ok=True)
                            decompressed = dest_path.with_suffix("")
                            if decompressed != dest_path:
                                decompressed.unlink(missing_ok=True)
                            _try_links(accession, link_idx + 1)
                            continue

                        file_type, sniffed_path = result
                        process_futures[
                            process_pool.submit(
                                _is_sc, sniffed_path, file_type, chunk_size
                            )
                        ] = (accession, dest_path, sniffed_path, file_type, link_idx)
                        downloaded_paths.add(dest_path)
                    except Exception as e:
                        logging.error("failed to download %s: %s", link.url, e)
                        _try_links(accession, link_idx + 1)

                if f in process_futures:
                    single_cell_count = f.result()
                    accession, dest_path, sniffed_path, file_type, link_idx = (
                        process_futures.pop(f)
                    )

                    if dest_path in downloaded_paths:
                        dest_path.unlink(missing_ok=True)

                    if sniffed_path != dest_path:
                        sniffed_path.unlink(missing_ok=True)

                    if accession in done_accessions:
                        continue

                    if single_cell_count > 0:
                        done_accessions.add(accession)
                        updates.append((accession, single_cell_count))
                        if len(updates) >= batch_size:
                            _flush_updates(conn, updates)
                            updates.clear()
                        if accession not in pbar_accessions:
                            pbar_accessions.add(accession)
                            pbar.update(1)
                    else:
                        _try_links(accession, link_idx + 1)

        if updates:
            _flush_updates(conn, updates)

    pbar.close()
    _db_flush()
    shutil.rmtree(output_dir, ignore_errors=True)


def _check_disk_space(path: Path, min_bytes: int = MIN_DISK_SPACE_BYTES) -> bool:
    try:
        usage = shutil.disk_usage(path)
        return usage.free >= min_bytes
    except OSError:
        return False


def _query_in(
    conn: sqlite3.Connection,
    query_prefix: str,
    items: list[str],
    batch_size: int,
    query_suffix: str = "",
):
    results = []

    for i in range(0, len(items), batch_size):
        chunk = items[i : i + batch_size]
        ph = ", ".join(["?"] * len(chunk))
        results.extend(
            conn.execute(f"{query_prefix} IN ({ph}) {query_suffix}", chunk).fetchall()
        )

    return results


def _flush_updates(conn: sqlite3.Connection, updates: list[tuple[str, int]]) -> None:
    non_sc = [acc for acc, cnt in updates if cnt == 0]
    sc = [(acc, cnt) for acc, cnt in updates if cnt > 0]

    if non_sc:
        placeholders = ", ".join(repr(acc) for acc in non_sc)
        conn.execute(
            f"UPDATE unified_metadata SET is_single_cell = 0, single_cell_modality = '', single_cell_count = 0 WHERE canonical_accession IN ({placeholders})"
        )

    for accession, single_cell_count in sc:
        conn.execute(
            "UPDATE unified_metadata SET single_cell_count = ? WHERE canonical_accession = ?",
            (single_cell_count, accession),
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


def _is_gzipped(path: Path) -> bool:
    GZIP_MAGIC = b"\x1f\x8b"
    try:
        with open(path, "rb") as f:
            return f.read(2) == GZIP_MAGIC
    except OSError:
        return False


def _resolve_file_type(
    path: Path, file_type: FileType, chunk_size: int
) -> tuple[FileType, Path] | None:
    if path.suffix == ".gz":
        if not _is_gzipped(path):
            stripped = path.with_suffix("")
            if not stripped.exists():
                path.rename(stripped)
            return _resolve_file_type(stripped, file_type, chunk_size)

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


def _is_sc(dest_path: Path, file_type: FileType, chunk_size: int) -> int:
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

    return 0


def safe_process(
    func: Callable[[Path, int], int],
) -> Callable[[Path, int], int]:
    @functools.wraps(func)
    def wrapper(path: Path, chunk_size: int, *args, **kwargs):
        try:
            return func(path, chunk_size, *args, **kwargs)
        except Exception as e:
            logging.error("failed to process %s: %s", path, e)
            return -1

    return wrapper


def _process_tar(path: Path, chunk_size: int) -> int:
    try:
        with tarfile.open(path, "r:*") as tar:
            max_count = 0
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
                    Path(tmp_path).unlink(missing_ok=True)
                    decompressed = tmp_path.with_suffix("")
                    if decompressed != tmp_path:
                        decompressed.unlink(missing_ok=True)
                    continue

                file_type, sniffed_path = result
                single_cell_count = _is_sc(sniffed_path, file_type, chunk_size)
                Path(tmp_path).unlink(missing_ok=True)

                if sniffed_path != tmp_path:
                    sniffed_path.unlink(missing_ok=True)
                if single_cell_count > max_count:
                    max_count = single_cell_count

        return max_count
    except Exception as e:
        logging.error("failed to process %s: %s", path, e)
        return -1


@safe_process
def _process_barcodes(path: Path, chunk_size: int) -> int:
    with open(path, "rb") as f:
        chunk = f.read(chunk_size)
    idx = chunk.find(b"\n")
    if idx == -1 or len(chunk) <= idx + 1:
        return 0
    return len(chunk[idx + 1 :].splitlines())


@safe_process
def _process_h5(path: Path, _: int) -> int:
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

    return n_cells if n_cells is not None and n_cells > 1 else 0


@safe_process
def _process_h5ad(path: Path, _: int) -> int:
    n_cells = None

    with h5py.File(path, "r") as f:
        if "obs/_index" in f:
            n_cells = len(f["obs/_index"])
        elif "X" in f:
            x = f["X"]
            if hasattr(x, "shape"):
                n_cells = x.shape[0]
            elif "shape" in x:
                n_cells = x["shape"][0]

    return n_cells if n_cells is not None and n_cells > 1 else 0


@safe_process
def _process_txt(path: Path, chunk_size: int) -> int:
    with open(path, "rb") as f:
        chunk = f.read(chunk_size)
    lines = chunk.strip().splitlines()
    return len(lines) if len(lines) > 2 else 0


@safe_process
def _process_mtx(path: Path, chunk_size: int) -> int:
    with open(path, "rb") as f:
        header = f.read(chunk_size)

    for line in header.splitlines():
        if line.startswith(b"%"):
            continue
        parts = line.split()
        if len(parts) >= 2:
            try:
                n_cells = int(parts[1])
                return n_cells if n_cells > 1 else 0
            except (ValueError, IndexError):
                return 0

    return 0


def _process_delimiter_file_types(delimiter: str) -> Callable[[Path, int], int]:
    @safe_process
    def _wrapper(path: Path, chunk_size: int) -> int:
        with open(path, "rb") as f:
            chunk = f.read(chunk_size)

        first_newline = chunk.find(b"\n")
        header = (
            chunk[:first_newline].decode().split(delimiter)
            if first_newline != -1
            else chunk.decode().split(delimiter)
        )
        n_cols = len(header)
        return n_cols if n_cols > 3 else 0

    return _wrapper


@safe_process
def _process_rds(path: Path, _: int) -> int:
    try:
        result = pyreadr.read_r(path)
    except LibrdataError:
        return _process_rds_rpy2(path)

    if not result:
        logging.error("no objects found in rds %s", path)
        return 0

    df = list(result.values())[0]
    if not hasattr(df, "shape") or len(df.shape) != 2:
        logging.error("unexpected object type in rds %s: %s", path, type(df))
        return 0

    n_cells = df.shape[1]
    return n_cells if n_cells > 1 else 0


def _process_rds_rpy2(path: Path) -> int:
    try:
        ro.r(f'obj <- readRDS("{path}")')

        n_cells = int(
            ro.r("""
            n_cells <- 0
            if (inherits(obj, "Seurat")) {
                counts <- tryCatch(
                    Seurat::GetAssayData(obj, slot = "counts"),
                    error = function(e) NULL
                )
                if (!is.null(counts)) {
                    n_cells <- ncol(counts)
                }
            } else if (is.data.frame(obj) || is.matrix(obj)) {
                n_cells <- ncol(obj)
            }
            n_cells
        """)[0]
        )

        return n_cells if n_cells > 1 else 0
    except Exception as e:
        logging.error("rpy2 fallback failed for %s: %s", path, e)
        return 0
