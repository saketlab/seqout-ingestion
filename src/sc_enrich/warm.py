"""v0 warm counter: whole-study cell counts to unified_metadata; see sc_enrich.deep."""

import contextlib
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
import threading
from collections import defaultdict
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import h5py

try:  # optional 'rds' extra: only the .rds readers need R / rpy2 / pyreadr
    import pyreadr
    import rpy2.robjects as ro
    from pyreadr.custom_errors import LibrdataError
except ImportError:
    _R_HINT = (
        "reading .rds files needs the optional 'rds' extra (rpy2 + pyreadr and a "
        "system R install). install with: uv sync --extra rds"
    )

    class LibrdataError(Exception):  # placeholder so `except LibrdataError` resolves
        pass

    class _MissingR:  # any attribute access raises the friendly hint
        def __getattr__(self, _name: str) -> object:
            raise ImportError(_R_HINT)

    pyreadr = ro = _MissingR()  # type: ignore[assignment]

from tqdm import tqdm

from helpers import batch_query_in, download_file, sqlite_connect

logger = logging.getLogger(__name__)

rds_batch_processor: Any = None


if TYPE_CHECKING:
    from rpy2.robjects.vectors import Vector


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

    def parse_url(self) -> str:  # noqa: D102
        return self.url.replace("ftp://", "https://")

    def extract_filename(self) -> str:  # noqa: D102
        return self.url.split("/")[-1]


@dataclasses.dataclass
class _RdsBatchItem:
    path: Path
    result: tuple[int, str] | None
    event: threading.Event


SC_ENRICH_OUTPUT_DIR = "sc_enrich"
MIN_DISK_SPACE_BYTES = 5 * 1024**3
POSSIBLE_MISNOMER_FILE_TYPES = [FileType.Csv, FileType.Tsv, FileType.Txt]
_RDS_BATCH_SIZE = 20

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


def sc_counter(
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
    *,
    count_exact: bool = True,
    check_all: bool = False,
) -> None:
    output_dir = base_output_dir / SC_ENRICH_OUTPUT_DIR
    if output_dir.exists():
        shutil.rmtree(output_dir, ignore_errors=True)
    output_dir.mkdir(exist_ok=True, parents=True)

    global rds_batch_processor  # noqa: PLW0603
    rds_batch_processor = _RdsBatchProcessor()

    tmp_dir = output_dir / ".tmp"
    tmp_dir.mkdir(exist_ok=True, parents=True)

    if not _check_disk_space(output_dir):
        logger.error(
            "insufficient disk space at %s: cannot start sc_enrich detector",
            output_dir,
        )
        return

    def _cleanup(signum: int, _frame: object | None) -> None:
        shutil.rmtree(output_dir, ignore_errors=True)
        sys.exit(signum)

    signal.signal(signal.SIGINT, _cleanup)
    signal.signal(signal.SIGTERM, _cleanup)

    conn = sqlite_connect(unified_metadata_db_file)
    geo_conn = sqlite_connect(geo_db_file)
    ae_conn = sqlite_connect(ae_db_file)

    _update_table_schema(conn)

    def _db_flush() -> None:
        ae_conn.close()
        geo_conn.close()
        conn.close()

    if check_all:
        rows = conn.execute(
            "SELECT canonical_accession, source "
            "FROM unified_metadata WHERE is_single_cell = '1';",
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT canonical_accession, source "
            "FROM unified_metadata WHERE is_single_cell = '1' "
            "AND sc_count_last_checked IS NULL;",
        ).fetchall()

    accessions: list[str | tuple[str, str]] = []
    search_for_aliases: list[str] = []

    for accession, source in rows:
        if source not in {"geo", "ae"}:
            search_for_aliases.append(accession)
        else:
            accessions.append(accession)

    rows = batch_query_in(
        conn,
        "SELECT canonical_accession, aliases "
        "FROM unified_metadata WHERE canonical_accession",
        search_for_aliases,
        999,
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
        rows = batch_query_in(
            geo_conn,
            "SELECT accession, samples_ref FROM geo_series WHERE accession",
            gse_list,
            999,
            "GROUP BY accession",
        )
    except sqlite3.OperationalError:
        logger.exception("database error querying geo_series (disk may be full)")
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
                msg = (
                    "samples_ref of geo_series table is "
                    "of either list[str] of list[dict] type"
                )
                raise TypeError(
                    msg,
                )

    download_links_mapping: dict[str, list[DownloadLink]] = defaultdict(
        list[DownloadLink],
    )
    link_url_to_sample: dict[str, dict[str, str]] = defaultdict(dict[str, str])

    all_gsms = list(gsm_to_gse.keys())
    if all_gsms:
        rows = batch_query_in(
            geo_conn,
            "SELECT accession, supplementary_data FROM geo_samples WHERE accession",
            all_gsms,
            batch_size=999,
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
                    DownloadLink(type=file_type, url=url),
                )
                link_url_to_sample[gse][url] = gsm_accession

    ae_missing = [
        acc
        for acc in accessions_to_process
        if not acc.startswith("GSE") and acc not in download_links_mapping
    ]
    if ae_missing:
        try:
            rows = batch_query_in(
                ae_conn,
                "SELECT accession, processed_data_files "
                "FROM arrayexpress_experiments WHERE accession",
                ae_missing,
                999,
            )
        except sqlite3.OperationalError:
            logger.exception(
                "database error querying arrayexpress_experiments (disk may be full)",
            )
            rows = []

        for accession, processed_data_files_json in rows:
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
                download_links_mapping[accession].append(
                    DownloadLink(type=file_type, url=url),
                )
                link_url_to_sample[accession][url] = accession

    for links in download_links_mapping.values():
        links.sort(key=lambda x: FILE_TYPE_PRIORITY.index(x.type))

    link_sample_map: dict[str, dict[int, str]] = {}
    for acc, links in download_links_mapping.items():
        by_url = link_url_to_sample.get(acc, {})
        link_sample_map[acc] = {i: by_url[link.url] for i, link in enumerate(links)}

    pbar = tqdm(
        total=len(download_links_mapping.keys()),
        desc="processing accessions",
        unit="acc",
    )
    pbar_accessions: set[str] = set()
    accession_links: dict[str, list[DownloadLink]] = dict(download_links_mapping)

    download_workers = max(1, num_workers * 3 // 4)
    process_workers = max(1, num_workers - download_workers)

    with (
        ThreadPoolExecutor(max_workers=download_workers) as download_pool,
        ThreadPoolExecutor(max_workers=process_workers) as process_pool,
    ):
        download_futures: dict[Future, tuple[str, int]] = {}
        process_futures: dict[Future, tuple[str, Path, Path, FileType, int]] = {}
        downloaded_paths: set[Path] = set()
        downloader = functools.partial(
            download_file,
            chunk_size=chunk_size,
            num_retries=retries,
            timeout=timeout,
            max_wait=max_timeout,
            with_pbar=False,
            validate_content_length=False,
        )
        updates: list[tuple[str, str]] = []
        done_accessions: set[str] = set()
        accession_sample_counts: dict[str, dict[str, int]] = {}
        accession_best_type: dict[str, str] = {}
        accession_remaining: dict[str, int] = {}
        sc_found: int = 0

        def _on_link_done(accession: str) -> None:
            accession_remaining[accession] -= 1
            if accession_remaining[accession] > 0:
                return
            if accession in done_accessions:
                return
            done_accessions.add(accession)

            sample_counts = accession_sample_counts.get(accession, {})
            json_str = json.dumps(sample_counts, sort_keys=True)

            if not count_exact:
                if not sample_counts:
                    updates.append((accession, json_str))
                    if len(updates) >= batch_size:
                        _flush_updates(
                            conn, updates, accession_best_type, count_exact=False
                        )
                        updates.clear()
                pbar.set_description(f"sc: {sc_found}/{pbar.total}")
            else:
                updates.append((accession, json_str))
                if len(updates) >= batch_size:
                    _flush_updates(conn, updates, accession_best_type)
                    updates.clear()

            if accession not in pbar_accessions:
                pbar_accessions.add(accession)
                pbar.update(1)

            shutil.rmtree(output_dir / accession, ignore_errors=True)

        def _submit_accession(accession: str) -> None:
            links = accession_links.get(accession) or []
            if not links:
                done_accessions.add(accession)
                return

            accession_remaining[accession] = len(links)
            for idx, link in enumerate(links):
                dest_path = output_dir / accession / link.extract_filename()

                if dest_path.exists():
                    result = _resolve_file_type(dest_path, link.type, chunk_size)
                    if result is None:
                        logger.debug("unable to find file type for %s", dest_path)
                        decompressed = dest_path.with_suffix("")
                        if decompressed != dest_path:
                            decompressed.unlink(missing_ok=True)
                        _on_link_done(accession)
                        continue

                    file_type, sniffed_path = result
                    process_futures[
                        process_pool.submit(
                            functools.partial(_is_sc, tmp_dir=tmp_dir),
                            sniffed_path,
                            file_type,
                            chunk_size,
                        )
                    ] = (accession, dest_path, sniffed_path, file_type, idx)
                    continue

                if not _check_disk_space(output_dir):
                    logger.warning("skipping %s: insufficient disk space", link.url)
                    _on_link_done(accession)
                    continue

                future = download_pool.submit(
                    downloader,
                    url=link.parse_url(),
                    dest_path=dest_path,
                    num_workers=1,
                )
                download_futures[future] = (accession, idx)

        for accession, links in sorted(
            accession_links.items(),
            key=lambda x: len(x[1]),
        ):
            if not links:
                done_accessions.add(accession)
                continue
            if accession not in done_accessions:
                _submit_accession(accession)

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
                            logger.debug("unable to find file type for %s", dest_path)
                            dest_path.unlink(missing_ok=True)
                            decompressed = dest_path.with_suffix("")
                            if decompressed != dest_path:
                                decompressed.unlink(missing_ok=True)
                            _on_link_done(accession)
                            continue

                        file_type, sniffed_path = result
                        process_futures[
                            process_pool.submit(
                                functools.partial(_is_sc, tmp_dir=tmp_dir),
                                sniffed_path,
                                file_type,
                                chunk_size,
                            )
                        ] = (accession, dest_path, sniffed_path, file_type, link_idx)
                        downloaded_paths.add(dest_path)
                    except Exception:
                        logger.exception("failed to download %s", link.url)
                        _on_link_done(accession)

                if f in process_futures:
                    single_cell_counts, fmt = f.result()
                    accession, dest_path, sniffed_path, file_type, link_idx = (
                        process_futures.pop(f)
                    )

                    if dest_path in downloaded_paths:
                        dest_path.unlink(missing_ok=True)

                    if sniffed_path != dest_path:
                        sniffed_path.unlink(missing_ok=True)

                    if accession in done_accessions:
                        _on_link_done(accession)
                        continue

                    sample = link_sample_map.get(accession, {}).get(link_idx, accession)

                    if not count_exact and single_cell_counts > 0:
                        done_accessions.add(accession)
                        accession_sample_counts.setdefault(accession, {})[sample] = (
                            single_cell_counts
                        )
                        accession_best_type[accession] = fmt
                        sc_found += 1
                        pbar.set_description(f"SC: {sc_found}/{pbar.total}")
                        json_str = json.dumps(
                            accession_sample_counts[accession], sort_keys=True
                        )
                        updates.append((accession, json_str))
                        if len(updates) >= batch_size:
                            _flush_updates(
                                conn, updates, accession_best_type, count_exact=False
                            )
                            updates.clear()
                        if accession not in pbar_accessions:
                            pbar_accessions.add(accession)
                            pbar.update(1)
                        _on_link_done(accession)
                        continue

                    prev = accession_sample_counts.get(accession, {}).get(sample, 0)
                    if single_cell_counts > prev:
                        accession_sample_counts.setdefault(accession, {})[sample] = (
                            single_cell_counts
                        )
                        accession_best_type[accession] = fmt
                    _on_link_done(accession)

            if rds_batch_processor is not None:
                rds_batch_processor.flush_all()

        if updates:
            _flush_updates(conn, updates, accession_best_type, count_exact=count_exact)

    pbar.close()
    if not count_exact:
        logger.info(
            "check complete: %d / %d accessions have single-cell data",
            sc_found,
            len(download_links_mapping),
        )

    if rds_batch_processor is not None:
        rds_batch_processor.flush_all()
    _db_flush()
    shutil.rmtree(output_dir, ignore_errors=True)


def _check_disk_space(path: Path, min_bytes: int = MIN_DISK_SPACE_BYTES) -> bool:
    try:
        usage = shutil.disk_usage(path)
    except OSError:
        return False
    else:
        return usage.free >= min_bytes


def _flush_updates(
    conn: sqlite3.Connection,
    updates: list[tuple[str, str]],
    accession_best_type: dict[str, str] | None = None,
    *,
    count_exact: bool = True,
) -> None:
    if accession_best_type is None:
        accession_best_type = {}

    non_sc = [acc for acc, cnt_str in updates if cnt_str == "{}"]
    sc = [(acc, cnt_str) for acc, cnt_str in updates if cnt_str != "{}"]

    if non_sc:
        placeholders = ", ".join(repr(acc) for acc in non_sc)
        conn.execute(
            f"UPDATE unified_metadata "  # noqa: S608
            f"SET single_cell_modality = '', single_cell_counts = '{{}}', "
            f"single_cell_file_format = '', "
            f"sc_count_last_checked = datetime('now') "
            f"WHERE canonical_accession IN ({placeholders})",
        )

    if sc:
        if not count_exact:
            conn.executemany(
                "UPDATE unified_metadata "
                "SET single_cell_file_format = ?, "
                "sc_count_last_checked = datetime('now') "
                "WHERE canonical_accession = ?",
                [(accession_best_type.get(acc, ""), acc) for acc, _ in sc],
            )
        else:
            sc_params = [
                (cnt_str, accession_best_type.get(acc, ""), acc) for acc, cnt_str in sc
            ]
            conn.executemany(
                "UPDATE unified_metadata "
                "SET single_cell_counts = ?, single_cell_file_format = ?, "
                "sc_count_last_checked = datetime('now') "
                "WHERE canonical_accession = ?",
                sc_params,
            )

    conn.commit()


def _update_table_schema(conn: sqlite3.Connection) -> None:
    columns = [row[1] for row in conn.execute("PRAGMA table_info(unified_metadata)")]
    if "single_cell_counts" not in columns:
        conn.execute(
            "ALTER TABLE unified_metadata "
            "ADD COLUMN single_cell_counts TEXT DEFAULT '{}';",
        )

    if "single_cell_file_format" not in columns:
        conn.execute(
            "ALTER TABLE unified_metadata "
            "ADD COLUMN single_cell_file_format TEXT DEFAULT '';",
        )

    if "sc_count_last_checked" not in columns:
        conn.execute(
            "ALTER TABLE unified_metadata ADD COLUMN sc_count_last_checked TEXT;",
        )


def _contains_any(s: str, substrings: list[str]) -> bool:
    return any(sub in s for sub in substrings)


def _classify_file(path: str) -> FileType:  # noqa: PLR0911
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
    gzip_magic = b"\x1f\x8b"
    try:
        with path.open("rb") as f:
            return f.read(2) == gzip_magic
    except OSError:
        return False


def _resolve_file_type(
    path: Path,
    file_type: FileType,
    chunk_size: int,
) -> tuple[FileType, Path] | None:
    if path.suffix == ".gz":
        if not _is_gzipped(path):
            stripped = path.with_suffix("")
            if not stripped.exists():
                path.rename(stripped)
            return _resolve_file_type(stripped, file_type, chunk_size)

        tmp_path = path.with_suffix("")
        if not tmp_path.exists():
            with gzip.open(path, "rb") as f_in, tmp_path.open("wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        path = tmp_path

    if file_type not in POSSIBLE_MISNOMER_FILE_TYPES:
        return file_type, path

    resolved_type, resolved_path = _file_type_sniffer(path, chunk_size)
    if resolved_type == FileType.Unknown:
        return None

    return resolved_type, resolved_path


def _file_type_sniffer(path: Path, chunk_size: int) -> tuple[FileType, Path]:  # noqa: PLR0911
    try:
        with path.open() as f:
            sample = f.read(chunk_size)
    except UnicodeDecodeError as e:
        logger.debug("failed to read %s: %s", path, e)
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
    if dialect.delimiter == ",":
        return FileType.Csv, path
    if dialect.delimiter == " ":
        return FileType.SpaceDelimited, path
    if dialect.delimiter == ";":
        return FileType.SemicolonDelimited, path
    return FileType.Txt, path


def _is_sc(
    dest_path: Path,
    file_type: FileType,
    chunk_size: int,
    tmp_dir: Path | None = None,
) -> tuple[int, str]:
    if file_type == FileType.Tar:
        return (_process_tar(dest_path, chunk_size, tmp_dir=tmp_dir), file_type.value)
    if file_type == FileType.Rds:
        return _process_rds(dest_path, chunk_size)

    handlers = {
        FileType.Barcodes: _process_barcodes,
        FileType.H5: _process_h5,
        FileType.H5ad: _process_h5ad,
        FileType.Txt: _process_txt,
        FileType.Mtx: _process_mtx,
        FileType.Tsv: _process_delimiter_file_types("\t"),
        FileType.Csv: _process_delimiter_file_types(","),
        FileType.SpaceDelimited: _process_delimiter_file_types(" "),
        FileType.SemicolonDelimited: _process_delimiter_file_types(";"),
    }

    handler = handlers.get(file_type)
    if handler is not None:
        return (handler(dest_path, chunk_size), file_type.value)

    return (0, file_type.value)


def safe_process(
    func: Callable[[Path, int], int],
) -> Callable[[Path, int], int]:
    @functools.wraps(func)
    def wrapper(path: Path, chunk_size: int, *args: Any, **kwargs: Any) -> int:
        try:
            return func(path, chunk_size, *args, **kwargs)
        except Exception:
            logger.exception("failed to process %s", path)
            return -1

    return wrapper


def _process_tar(path: Path, chunk_size: int, tmp_dir: Path | None = None) -> int:
    try:
        with tarfile.open(path, "r:*") as tar:
            max_count = 0
            extract_dir = Path(tempfile.mkdtemp(dir=tmp_dir))

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
                with tempfile.NamedTemporaryFile(
                    delete=False,
                    suffix=suffix,
                    dir=extract_dir,
                ) as tmp:
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
                single_cell_counts, _ = _is_sc(
                    sniffed_path,
                    file_type,
                    chunk_size,
                    tmp_dir=extract_dir,
                )
                Path(tmp_path).unlink(missing_ok=True)

                if sniffed_path != tmp_path:
                    sniffed_path.unlink(missing_ok=True)
                max_count = max(max_count, single_cell_counts)

            shutil.rmtree(extract_dir, ignore_errors=True)

    except Exception:
        logger.exception("failed to process %s", path)
        return -1
    else:
        return max_count


@safe_process
def _process_barcodes(path: Path, chunk_size: int) -> int:
    with path.open("rb") as f:
        chunk = f.read(chunk_size)
    idx = chunk.find(b"\n")
    if idx == -1 or len(chunk) <= idx + 1:
        return 0
    return len(chunk[idx + 1 :].splitlines())


@safe_process
def _process_h5(path: Path, _: int) -> int:
    n_cells = None

    with h5py.File(path, "r") as f:
        for candidate in ("matrix/barcodes", "barcodes"):
            if candidate in f:
                with contextlib.suppress(TypeError):
                    n_cells = len(f[candidate])
                if n_cells is not None:
                    break

        if n_cells is None:

            def find_barcodes(name: str, obj: Any) -> None:
                nonlocal n_cells
                if n_cells is not None:
                    return
                if obj is not None and name.endswith("/barcodes"):
                    with contextlib.suppress(TypeError):
                        n_cells = len(obj)

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
    with path.open("rb") as f:
        chunk = f.read(chunk_size)
    lines = chunk.strip().splitlines()
    _min_sc_lines = 2
    return len(lines) if len(lines) > _min_sc_lines else 0


@safe_process
def _process_mtx(path: Path, chunk_size: int) -> int:
    with path.open("rb") as f:
        header = f.read(chunk_size)

    _min_parts = 2
    for line in header.splitlines():
        if line.startswith(b"%"):
            continue
        parts = line.split()
        if len(parts) >= _min_parts:
            try:
                n_cells = int(parts[1])
            except (ValueError, IndexError):
                return 0
            else:
                return n_cells if n_cells > 1 else 0

    return 0


def _process_delimiter_file_types(delimiter: str) -> Callable[[Path, int], int]:
    @safe_process
    def _wrapper(path: Path, chunk_size: int) -> int:
        with path.open("rb") as f:
            chunk = f.read(chunk_size)

        first_newline = chunk.find(b"\n")
        header = (
            chunk[:first_newline].decode().split(delimiter)
            if first_newline != -1
            else chunk.decode().split(delimiter)
        )
        _min_sc_cols = 3
        n_cols = len(header)
        return n_cols if n_cols > _min_sc_cols else 0

    return _wrapper


def _process_rds(path: Path, _: int) -> tuple[int, str]:
    try:
        result = pyreadr.read_r(path)
    except LibrdataError:
        if rds_batch_processor is not None:
            return rds_batch_processor.submit(path)
        return (0, "unknown")

    if not result:
        logger.error("no objects found in rds %s", path)
        return (0, "unknown")

    rds_df = next(iter(result.values()))
    _expected_ndim = 2
    if not hasattr(rds_df, "shape") or len(rds_df.shape) != _expected_ndim:
        logger.error("unexpected object type in rds %s: %s", path, type(rds_df))
        return (0, "unknown")

    n_cells = rds_df.shape[1]
    return (n_cells if n_cells > 1 else 0, "dataframe")


class _RdsBatchProcessor:
    def __init__(self, batch_size: int = _RDS_BATCH_SIZE) -> None:
        self.batch_size = batch_size
        self._lock = threading.Lock()
        self._pending: list[_RdsBatchItem] = []

    def submit(self, path: Path) -> tuple[int, str]:
        item = _RdsBatchItem(path=path, result=None, event=threading.Event())
        should_flush = False
        with self._lock:
            self._pending.append(item)
            if len(self._pending) >= self.batch_size:
                should_flush = True
        if should_flush:
            self._flush()
        item.event.wait()
        return item.result if item.result is not None else (0, "unknown")

    def _flush(self) -> bool:
        with self._lock:
            batch = self._pending[: self.batch_size]
            self._pending = self._pending[self.batch_size :]
        if not batch:
            return False
        paths = [item.path for item in batch]
        try:
            results = _rds_batch_rpy2(paths)
        except Exception:
            logger.exception("batch RDS rpy2 processing failed")
            results = [(0, "unknown")] * len(paths)
        with self._lock:
            for item, result in zip(batch, results, strict=True):
                item.result = result
                item.event.set()
        return True

    def flush_all(self) -> None:
        while self._flush():
            pass


def _rds_batch_rpy2(paths: list[Path]) -> list[tuple[int, str]]:
    paths_r = ", ".join(json.dumps(str(p)) for p in paths)
    r_script = f"""
    paths <- c({paths_r})
    results <- vector("list", length(paths))
    for (i in seq_along(paths)) {{
        obj <- tryCatch(readRDS(paths[i]), error = function(e) NULL)
        if (is.null(obj)) {{
            results[[i]] <- c(0L, "unknown")
            next
        }}
        n_cells <- 0L
        obj_type <- "unknown"
        if (inherits(obj, "Seurat")) {{
            obj_type <- "seurat"
            counts <- tryCatch(
                Seurat::GetAssayData(obj, layer = "counts"),
                error = function(e) tryCatch(
                    Seurat::GetAssayData(obj, slot = "counts"),
                    error = function(e2) NULL
                )
            )
            if (!is.null(counts)) n_cells <- ncol(counts)
        }} else if (inherits(obj, "SeuratObject")) {{
            obj_type <- "seurat"
            counts <- tryCatch(
                SeuratObject::GetAssayData(obj, layer = "counts"),
                error = function(e) NULL
            )
            if (!is.null(counts)) n_cells <- ncol(counts)
        }} else if (inherits(obj, "cell_data_set")) {{
            obj_type <- "cell_data_set"
            n_cells <- ncol(obj)
        }} else if (inherits(obj, "SpatialExperiment")) {{
            obj_type <- "SpatialExperiment"
            counts <- tryCatch(
                SpatialExperiment::counts(obj),
                error = function(e) NULL
            )
            if (!is.null(counts)) n_cells <- ncol(counts)
        }} else if (inherits(obj, "SingleCellExperiment")) {{
            obj_type <- "SingleCellExperiment"
            counts <- tryCatch(
                SingleCellExperiment::counts(obj),
                error = function(e) NULL
            )
            if (!is.null(counts)) n_cells <- ncol(counts)
        }} else if (inherits(obj, "CellDataSet")) {{
            obj_type <- "CellDataSet"
            exprs_mat <- tryCatch(
                Biobase::exprs(obj),
                error = function(e) NULL
            )
            if (!is.null(exprs_mat)) n_cells <- ncol(exprs_mat)
        }} else if (inherits(obj, "ExpressionSet")) {{
            obj_type <- "ExpressionSet"
            exprs_mat <- tryCatch(
                Biobase::exprs(obj),
                error = function(e) NULL
            )
            if (!is.null(exprs_mat)) n_cells <- ncol(exprs_mat)
        }} else if (inherits(obj, "liger")) {{
            obj_type <- "liger"
            raw_data <- tryCatch(
                obj@raw.data,
                error = function(e) NULL
            )
            if (!is.null(raw_data)) n_cells <- sum(sapply(raw_data, ncol))
        }} else if (inherits(obj, "Pagoda2")) {{
            obj_type <- "Pagoda2"
            n_cells <- nrow(obj$counts)
        }} else if (inherits(obj, "Conos")) {{
            obj_type <- "Conos"
            n_cells <- tryCatch(
                sum(sapply(obj$samples, ncol)),
                error = function(e) 0L
            )
        }} else if (is.data.frame(obj) || is.matrix(obj)) {{
            obj_type <- "dataframe"
            n_cells <- ncol(obj)
        }} else {{
            n_cells <- tryCatch(ncol(obj), error = function(e) 0L)
            if (is.null(n_cells) || is.na(n_cells)) n_cells <- 0L
        }}
        results[[i]] <- c(n_cells, obj_type)
    }}
    results
    """

    try:
        raw = cast("list", ro.r(r_script))
    except Exception:
        logger.exception("batch RDS rpy2 processing failed")
        return [(0, "unknown")] * len(paths)

    parsed = []
    for i in range(len(raw)):
        item = cast("Vector", raw[i])
        n_cells = int(item[0])
        obj_type = str(item[1])
        parsed.append((n_cells if n_cells > 1 else 0, obj_type))
    return parsed
