import dataclasses
import functools
import json
import logging
import sqlite3
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from enum import StrEnum
from pathlib import Path

from helpers import better_downloader


class _FileType(StrEnum):
    barcodes = "barcodes"
    h5 = "h5"
    h5ad = "h5ad"
    tar = "tar"


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

                    if "barcodes.tsv" in url:
                        download_links_mapping[series_accession].append(
                            _DownloadLink(type=_FileType.barcodes, url=url)
                        )
                    elif ".h5ad" in url:
                        download_links_mapping[series_accession].append(
                            _DownloadLink(type=_FileType.h5ad, url=url)
                        )
                    elif ".h5" in url:
                        download_links_mapping[series_accession].append(
                            _DownloadLink(type=_FileType.h5, url=url)
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

                    if ".tar" in url or ".tar.gz" in url or ".tgz" in url:
                        download_links_mapping[series_accession].append(
                            _DownloadLink(type=_FileType.tar, url=url)
                        )
                        break
        else:
            pass

    accession_num_cells_mapping: dict[str, int] = {}

    for accession, links in download_links_mapping.items():
        if len(links) == 0:
            logging.warning(
                "could not find any relevant single-cell countable files for %s accession",
                accession,
            )
            continue

        num_workers_per_link = num_workers // len(links)

        downloader = functools.partial(
            better_downloader,
            num_workers=num_workers_per_link,
            chunk_size=chunk_size,
            retries=retries,
            timeout=timeout,
            max_timeout=max_timeout,
        )

        if len(links) == 1:
            link = links[0]
            url = link._parse_url()
            dest_path = output_dir / link._extract_filename()

            downloader(
                url=url,
                dest_path=dest_path,
            )
        else:
            with ThreadPoolExecutor(max_workers=len(links)) as pool:
                futures = []

                for link in links:
                    url = link._parse_url()
                    dest_path = output_dir / link._extract_filename()

                    futures.append(
                        pool.submit(
                            downloader,
                            url=url,
                            dest_path=dest_path,
                        )
                    )

                for f in futures:
                    f.result()

    conn.close()
