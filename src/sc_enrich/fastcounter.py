import dataclasses
import functools
import gzip
import json
import sqlite3
import tarfile
import tempfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import StrEnum
from pathlib import Path
from typing import Optional

import h5py

from helpers import simple_file_download


class _FileType(StrEnum):
    Barcodes = "barcodes"
    H5 = "h5"
    H5ad = "h5ad"
    Tar = "tar"
    Unknown = "unknown"


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
    del search_for_aliases

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

    gse_to_gsm: dict[str, list[str]] = defaultdict(list[str])
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
        gse_to_gsm[accession] = [v["@ref"] for v in samples_ref]

    download_links_mapping: dict[str, list[_DownloadLink]] = defaultdict(
        list[_DownloadLink]
    )

    for series_accession in accessions_to_process:
        if series_accession.startswith("GSE"):
            samples = gse_to_gsm[accession]
            placeholders = ", ".join(repr(s) for s in samples)
            rows: list[tuple[str]] = geo_conn.execute(
                f"SELECT supplementary_data FROM geo_samples WHERE accession IN ({placeholders})"
            ).fetchall()

            for (supl_data_json,) in rows:
                if not supl_data_json:
                    continue
                supl_data: list[dict[str, str]] = json.loads(supl_data_json)

                for item in supl_data:
                    url = item.get("#text")
                    if not url:
                        continue

                    file_type = _classify_file(url)
                    if file_type == _FileType.Unknown:
                        continue

                    download_links_mapping[series_accession].append(
                        _DownloadLink(type=file_type, url=url)
                    )

            if len(download_links_mapping[series_accession]) == 0:
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
                    url_type = _classify_file(url)

                    # if no supplementary data is found in samples-level then just analyze a single tar file on series-level
                    if url_type == _FileType.Tar:
                        download_links_mapping[series_accession].append(
                            _DownloadLink(type=url_type, url=url)
                        )
                        break
        else:
            pass

    accession_num_cells_mapping: dict[str, int] = {}

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

        def _process_file(dest_path: Path, file_type: _FileType):
            is_sc = _is_sc(dest_path, file_type)
            print(accession, is_sc, file_type, dest_path)

        for accession, links in download_links_mapping.items():
            if len(links) == 0:
                continue

            for link in links:
                dest_path = output_dir / link._extract_filename()
                file_type = link.type

                if dest_path.exists():
                    _process_file(dest_path, file_type)
                    continue

                future = pool.submit(
                    downloader,
                    url=link._parse_url(),
                    dest_path=dest_path,
                )
                futures[future] = (accession, link.type)

        for f in as_completed(futures):
            dest_path = f.result()
            accession, file_type = futures[f]
            _process_file(dest_path, file_type)

    conn.close()


def _classify_file(path: str) -> _FileType:
    if "barcodes.tsv" in path:
        return _FileType.Barcodes
    elif ".ha5d" in path:
        return _FileType.H5ad
    elif ".h5" in path:
        return _FileType.H5
    elif (".tar" in path) or (".tar.gz" in path) or (".tgz" in path):
        return _FileType.Tar
    else:
        return _FileType.Unknown


def _is_sc(dest_path: Path, file_type: _FileType, raw: Optional[bytes] = None) -> bool:
    if file_type == _FileType.Tar:
        return _process_tar(dest_path)
    else:
        if raw is None:
            with open(dest_path, "rb") as f:
                raw = f.read()

        match file_type:
            case _FileType.Barcodes:
                return _process_barcodes(raw)
            case _FileType.H5:
                return _process_h5(raw)

    return False


def _process_tar(dest_path: Path) -> bool:
    with tarfile.open(dest_path, "r:*") as tar:
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

            is_sc = _is_sc(dest_path, file_type, raw)
            if is_sc:
                return True

    return False


def _process_barcodes(raw: bytes) -> bool:
    lines = raw.decode().splitlines()
    return len(lines) > 1


def _process_h5(raw: bytes) -> bool:
    with tempfile.NamedTemporaryFile(suffix=".h5", delete=False) as tmp:
        tmp.write(raw)
        tmp_path = tmp.name

    try:
        n_cells = None

        with h5py.File(tmp_path, "r") as f:
            # recursively try to find barcodes
            def find_barcodes(name, obj):
                nonlocal n_cells
                if n_cells is not None:
                    return
                if isinstance(obj, h5py.Dataset) and name.endswith("/barcodes"):
                    n_cells = len(obj)

            f.visititems(find_barcodes)

            if n_cells is None and "obs/_index" in f:
                n_cells = len(f["obs/_index"])

            if n_cells is None and "X" in f:
                n_cells = f["X"].shape[0]

        return n_cells > 1 if n_cells is not None else False
    finally:
        Path(tmp_path).unlink(missing_ok=True)
