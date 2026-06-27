import datetime
import functools
import json
import logging
import re
import sqlite3
import tarfile
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import StrEnum
from pathlib import Path
from typing import IO, Optional

import humanfriendly
import lxml.etree as ET
from bs4 import BeautifulSoup
from tqdm import tqdm

from helpers import exec_query_from_file, send_req, simple_file_download


class TableType(StrEnum):
    Platforms = "Platforms"
    Samples = "Samples"
    Series = "Series"
    Contributors = "Contributors"


GEO_OUTPUT_DIR = "geo"
MINIML_NAMESPACE = "http://www.ncbi.nlm.nih.gov/geo/info/MINiML"

GEO_SQL_STATEMENTS: dict[TableType, str] = {
    TableType.Platforms: "INSERT OR REPLACE INTO geo_platforms (accession, description, title, technology, published_at, updated_at, organism, manufacturer, relation) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    TableType.Samples: "INSERT OR REPLACE INTO geo_samples (accession, channel_count, channels, description, platform_ref, published_at,updated_at, supplementary_data, title, sample_type, hybridization_protocol, scan_protocol) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    TableType.Series: "INSERT OR REPLACE INTO geo_series (accession, overall_design, pubmed_id, relation, samples_ref, published_at, updated_at, summary, supplementary_data, title, series_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    TableType.Contributors: "INSERT INTO geo_contributors (accession, person, department, organization, address_line, address_city, address_state, address_zipcode, address_country) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
}


def ingest_geo(
    base_output_dir: Path,
    db_file: Optional[Path],
    start_date: Optional[datetime.date],
    end_date: Optional[datetime.date],
    num_workers: int,
    chunk_size: int,
    batch_size: int,
    series_limit: Optional[int],
    retries: int,
    timeout: int,
    max_timeout: int,
):
    output_dir = Path(base_output_dir) / GEO_OUTPUT_DIR
    output_dir.mkdir(exist_ok=True, parents=True)

    data_dir = output_dir / "data"
    data_dir.mkdir(exist_ok=True, parents=True)

    date_str = datetime.datetime.now().strftime("%Y-%m-%d")
    db_dir = output_dir / "db" / date_str
    db_dir.mkdir(exist_ok=True, parents=True)
    db_file = db_file or (db_dir / f"geo_metadata_{date_str}.db")

    logging.info(
        """starting geo ingest with following config:
- num workers: %d
- chunk size: %s
- batch size: %d
- series limit: %s
- start date: %s
- end date: %s
- retries: %d
- timeout: %s
- max timeout: %s""",
        num_workers,
        humanfriendly.format_size(chunk_size),
        batch_size,
        str(series_limit) or "None",
        str(start_date) or "None",
        str(end_date) or "None",
        retries,
        humanfriendly.format_timespan(timeout),
        humanfriendly.format_timespan(max_timeout),
    )

    geo_dict = _build_geo_dict(
        num_workers,
        series_limit,
        start_date,
        end_date,
        retries,
        timeout,
        max_timeout,
    )

    downloader = functools.partial(
        simple_file_download,
        num_retries=retries,
        timeout=timeout,
        max_wait=max_timeout,
    )

    exec_query_from_file("tables/geo.sql", db_file)
    _run_pipeline(
        downloader,
        geo_dict,
        db_file,
        data_dir,
        num_workers,
        batch_size,
        chunk_size,
        retries,
        timeout,
        max_timeout,
    )
    exec_query_from_file("indexes/geo.sql", db_file)


def _build_geo_dict(
    num_workers: int,
    series_limit: int | None,
    start_date: datetime.date | None,
    end_date: datetime.date | None,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> dict[str, list[str]]:
    resp = send_req(
        "GET",
        "https://ftp.ncbi.nlm.nih.gov/geo/series/",
        num_retries=retries,
        timeout=timeout,
        max_wait=max_timeout,
    )
    assert resp is not None, "failed to fetch geo series"

    soup = BeautifulSoup(resp.text, "html.parser")
    anchors = soup.find_all("a")
    series_list = [
        str(a["href"][:-1]) for a in anchors if str(a["href"]).startswith("GSE")
    ]

    if series_limit:
        series_list = series_list[:series_limit]

    all_series_accession_mapping: dict[str, list[str]] = {}

    with (
        ThreadPoolExecutor(num_workers) as pool,
        tqdm(total=len(series_list), desc="fetching all accessions") as pbar,
    ):
        futures = {
            pool.submit(
                _fetch_all_accessions_worker, s, retries, timeout, max_timeout
            ): s
            for s in series_list
        }

        for f in as_completed(futures):
            accessions = f.result()
            series = futures[f]
            if not accessions:
                pbar.update(1)
                continue

            all_series_accession_mapping[series] = accessions
            pbar.update(1)

    if start_date is None and end_date is None:
        return all_series_accession_mapping

    total_accessions = sum([len(x) for x in all_series_accession_mapping.values()])
    to_process_series_accession_mapping: dict[str, list[str]] = defaultdict(list[str])

    with (
        ThreadPoolExecutor(num_workers) as pool,
        tqdm(total=total_accessions, desc="filtering accessions to process") as pbar,
    ):
        futures = {}

        for series, accessions in all_series_accession_mapping.items():
            for accession in accessions:
                futures[
                    pool.submit(
                        _filter_accessions_worker,
                        accession,
                        series,
                        start_date,
                        end_date,
                        retries,
                        timeout,
                        max_timeout,
                    )
                ] = (
                    series,
                    accession,
                )

        for f in as_completed(futures):
            to_process = f.result()
            if not to_process:
                pbar.update(1)
                continue

            (series, accession) = futures[f]
            to_process_series_accession_mapping[series].append(accession)
            pbar.update(1)

    return to_process_series_accession_mapping


def _fetch_all_accessions_worker(
    series: str,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> list[str] | None:
    resp = send_req(
        method="GET",
        url=f"https://ftp.ncbi.nlm.nih.gov/geo/series/{series}",
        num_retries=retries,
        timeout=timeout,
        max_wait=max_timeout,
    )

    if resp is None:
        logging.error("failed to fetch geo accession ids for %s series", series)
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    anchors = soup.find_all("a")
    accessions = [
        str(a["href"]).rstrip("/") for a in anchors if str(a["href"]).startswith("GSE")
    ]

    return accessions


def _filter_accessions_worker(
    accession: str,
    series: str,
    start_date: datetime.date | None,
    end_date: datetime.date | None,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> bool:
    resp = send_req(
        method="GET",
        url=f"https://ftp.ncbi.nlm.nih.gov/geo/series/{series}/{accession}",
        num_retries=retries,
        timeout=timeout,
        max_wait=max_timeout,
    )

    if resp is None:
        logging.error("failed to fetch %s geo accession dir", accession)
        return False

    soup = BeautifulSoup(resp.text, "html.parser")

    for a in soup.find_all("a"):
        href = a.get("href", "")
        if not href:
            continue

        next = a.next_sibling
        if next is None:
            continue

        date_match = re.match(r"\s*(\d{4}-\d{2}-\d{2})", str(next))
        if date_match is None:
            continue

        modified_date = datetime.date.fromisoformat(date_match.group(1))
        if start_date and modified_date < start_date:
            continue
        if end_date and modified_date > end_date:
            continue

        return True
    return False


def _download_worker(
    downloader: functools.partial,
    series_group: str,
    accession: str,
    output_dir: Path,
    chunk_size: int,
):
    dest_path = output_dir / f"{accession}_family.xml.tgz"
    url = f"https://ftp.ncbi.nlm.nih.gov/geo/series/{series_group}/{accession}/miniml/{accession}_family.xml.tgz"

    if dest_path.exists():
        return

    try:
        downloader(url, dest_path, chunk_size, with_pbar=False)
    except Exception:
        logging.exception("failed to download %s from %s", accession, url)


def _wrap_ns(s: str) -> str:
    return f"{{{MINIML_NAMESPACE}}}{s}"


def _findtext(root: ET._Element, s: str) -> str | None:
    v = root.findtext(_wrap_ns(s)) or ""
    return (v or "").strip() or None


def _parse_dates(root: ET._Element, s: str) -> datetime.date | None:
    date_raw = root.findtext(s)
    parsed_date: datetime.date | None = None

    if date_raw:
        parsed_date = datetime.date.fromisoformat(date_raw)

    return parsed_date


def _et_to_dict(elem: ET._Element | list[ET._Element]) -> list | dict | str:
    if isinstance(elem, list):
        return [_et_to_dict(v) for v in elem]
    else:
        result: dict | str = {}

        for key, value in elem.attrib.items():
            result[f"@{key}"] = value

        if elem.text and elem.text.strip():
            if len(elem.attrib.items()) > 0:
                result["#text"] = elem.text.strip()
            else:
                result = elem.text.strip()

        for child in elem:
            child_tag = str(child.tag).split("}")[-1]
            child_dict = _et_to_dict(child)

            if isinstance(result, dict):
                if child_tag in result:
                    if not isinstance(result[child_tag], list):
                        result[child_tag] = [result[child_tag], child_dict]
                    else:
                        result[child_tag].append(child_dict)
                else:
                    result[child_tag] = child_dict

        return result


def _get_platforms(root: ET._Element) -> list[tuple] | None:
    platform_elems = root.findall(_wrap_ns("Platform"))
    if not platform_elems:
        return None

    platforms = []
    for platform in platform_elems:
        accession = platform.get("iid")
        title = _findtext(platform, "Title")
        description = _findtext(platform, "Description")
        technology = _findtext(platform, "Technology")
        manufacturer = _findtext(platform, "Manufacturer")

        organisms_list = _et_to_dict(platform.findall(_wrap_ns("Organism")))
        organisms = json.dumps(organisms_list) if organisms_list else None

        relations_list = _et_to_dict(platform.findall(_wrap_ns("Relation")))
        relations = json.dumps(relations_list) if relations_list else None

        published_at = _parse_dates(
            platform, f"{_wrap_ns('Status')}/{_wrap_ns('Release-Date')}"
        )
        updated_at = _parse_dates(
            platform, f"{_wrap_ns('Status')}/{_wrap_ns('Last-Update-Date')}"
        )

        platforms.append(
            (
                accession,
                description,
                title,
                technology,
                published_at,
                updated_at,
                organisms,
                manufacturer,
                relations,
            )
        )

    return platforms


def _get_samples(root: ET._Element) -> list[tuple] | None:
    samples = []
    samples_elems = root.findall(_wrap_ns("Sample"))
    if not samples_elems:
        return None

    for sample in samples_elems:
        accession = sample.get("iid")
        channel_count_raw = sample.findtext(_wrap_ns("Channel-Count"))
        channel_count = int(channel_count_raw) if channel_count_raw else None

        channels_raw = _et_to_dict(sample.findall(_wrap_ns("Channel")))
        channels_list = json.dumps(channels_raw) if channels_raw else None

        description = _findtext(sample, "Description")
        platform_ref_elem = sample.find(_wrap_ns("Platform-Ref"))
        platform_ref = (
            platform_ref_elem.get("ref") if platform_ref_elem is not None else None
        )

        supplementary_raw = _et_to_dict(sample.findall(_wrap_ns("Supplementary-Data")))
        supplementary_data = (
            json.dumps(supplementary_raw) if supplementary_raw else None
        )

        title = _findtext(sample, "Title")
        sample_type = sample.findtext(_wrap_ns("Type"))
        hybridization_protocol = _findtext(sample, "Hybridization-Protocol")
        scan_protocol = _findtext(sample, "Scan-Protocol")

        published_at = _parse_dates(
            sample, f"{_wrap_ns('Status')}/{_wrap_ns('Release-Date')}"
        )
        updated_at = _parse_dates(
            sample, f"{_wrap_ns('Status')}/{_wrap_ns('Last-Update-Date')}"
        )

        samples.append(
            (
                accession,
                channel_count,
                channels_list,
                description,
                platform_ref,
                published_at,
                updated_at,
                supplementary_data,
                title,
                sample_type,
                hybridization_protocol,
                scan_protocol,
            )
        )

    return samples


def _get_series(root: ET._Element) -> list[tuple] | None:
    series = root.find(_wrap_ns("Series"))
    if not series:
        return None

    accession = series.get("iid")
    overall_design = _findtext(series, "Overall-Design")
    pubmed_ids_list = [p.text for p in series.findall(_wrap_ns("Pubmed-ID"))]
    pubmed_ids = json.dumps(pubmed_ids_list) if pubmed_ids_list else None
    relations_raw = _et_to_dict(series.findall(_wrap_ns("Relation")))
    relations = json.dumps(relations_raw) if relations_raw else None
    sample_refs_raw = _et_to_dict(series.findall(_wrap_ns("Sample-Ref")))
    sample_refs = json.dumps(sample_refs_raw) if sample_refs_raw else None

    title = _findtext(series, "Title")
    summary = _findtext(series, "Summary")
    supplementary_data_raw = _et_to_dict(series.findall(_wrap_ns("Supplementary-Data")))
    supplementary_data = (
        json.dumps(supplementary_data_raw) if supplementary_data_raw else None
    )
    series_type_raw = series.findtext(_wrap_ns("Type"))
    series_type = json.dumps([series_type_raw]) if series_type_raw else None

    published_at = _parse_dates(
        series, f"{_wrap_ns('Status')}/{_wrap_ns('Release-Date')}"
    )
    updated_at = _parse_dates(
        series, f"{_wrap_ns('Status')}/{_wrap_ns('Last-Update-Date')}"
    )

    return [
        (
            accession,
            overall_design,
            pubmed_ids,
            relations,
            sample_refs,
            published_at,
            updated_at,
            summary,
            supplementary_data,
            title,
            series_type,
        )
    ]


def _get_contributors(root: ET._Element, accession: str) -> list[tuple] | None:
    contributors_elems = root.findall(_wrap_ns("Contributor"))
    if not contributors_elems:
        return None

    contributors = []
    for contrib in contributors_elems:
        person_name: str | None = None
        person_elem = contrib.find(_wrap_ns("Person"))
        if person_elem is not None:
            first_name = person_elem.findtext(_wrap_ns("First")) or ""
            last_name = person_elem.findtext(_wrap_ns("Last")) or ""
            person_name = (
                f"{first_name} {last_name}".strip() if first_name or last_name else None
            )

        dept = contrib.findtext(_wrap_ns("Department"))
        org = contrib.findtext(_wrap_ns("Organization"))

        address_line: str | None = None
        address_city: str | None = None
        address_state: str | None = None
        address_zipcode: str | None = None
        address_country: str | None = None
        address_elem = contrib.find(_wrap_ns("Address"))
        if address_elem is not None:
            address_line = address_elem.findtext(_wrap_ns("Line"))
            address_city = address_elem.findtext(_wrap_ns("City"))
            address_state = address_elem.findtext(_wrap_ns("State"))
            address_zipcode = address_elem.findtext(
                _wrap_ns("Zip-Code")
            ) or address_elem.findtext(_wrap_ns("Postal-Code"))
            address_country = address_elem.findtext(_wrap_ns("Country"))

        contributors.append(
            (
                accession,
                person_name,
                dept,
                org,
                address_line,
                address_city,
                address_state,
                address_zipcode,
                address_country,
            )
        )

    return contributors


def _parse_miniml(f: IO[bytes], accession: str) -> dict[TableType, list | None]:
    tree = ET.parse(f)
    root = tree.getroot()

    platforms = _get_platforms(root)
    samples = _get_samples(root)
    series = _get_series(root)
    contributors = _get_contributors(root, accession)

    return {
        TableType.Platforms: platforms,
        TableType.Samples: samples,
        TableType.Series: series,
        TableType.Contributors: contributors,
    }


def _process_one(
    accession: str,
    dest_path: Path,
    db_file: Path,
    batch_size: int,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> int:
    conn = sqlite3.Connection(db_file, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    cursor = conn.cursor()

    batch_by_type: dict[TableType, list] = defaultdict(list)
    item_count = 0

    def _flush(batch: list, table_type: TableType):
        if not batch:
            return

        delay = timeout
        for attempt in range(retries):
            try:
                cursor.executemany(GEO_SQL_STATEMENTS[table_type], batch)
                conn.commit()
                batch_by_type[table_type] = []
                return
            except sqlite3.OperationalError as e:
                if "locked" in str(e) and attempt < retries - 1:
                    time.sleep(delay)
                    delay = min(timeout * 2, max_timeout)
                else:
                    raise

    try:
        with tarfile.open(dest_path, "r:gz") as tar:
            for member in tar.getmembers():
                if not member.name.endswith(".xml"):
                    continue
                f = tar.extractfile(member)
                if not f:
                    logging.error("failed to extract file (path=%s)", dest_path)
                    break

                parsed_data = _parse_miniml(f, accession)
                for table_type, rows in parsed_data.items():
                    if not rows:
                        continue
                    item_count += 1
                    batch_by_type[table_type].extend(rows)
                    if len(batch_by_type[table_type]) >= batch_size:
                        _flush(batch_by_type[table_type], table_type)
                break
    except Exception:
        logging.exception("failed to process %s (path=%s)", accession, dest_path)
        conn.close()
        return 0

    for table_type, batch in batch_by_type.items():
        _flush(batch, table_type)

    conn.close()
    return item_count


def _run_pipeline(
    downloader: functools.partial,
    geo_dict: dict[str, list[str]],
    db_file: Path,
    output_dir: Path,
    num_workers: int,
    batch_size: int,
    chunk_size: int,
    retries: int,
    timeout: int,
    max_timeout: int,
):
    num_accessions = sum(len(v) for v in geo_dict.values())
    logging.info("loaded %d groups and %d accessions", len(geo_dict), num_accessions)

    if num_accessions == 0:
        logging.warning("geo_dict is empty, nothing to process")
        return

    download_tasks = [
        (series_group, acc) for series_group, accs in geo_dict.items() for acc in accs
    ]

    existing = sum(
        1
        for _, acc in download_tasks
        if (output_dir / f"{acc}_family.xml.tgz").exists()
    )
    cached_ratio = existing / num_accessions if num_accessions else 0
    logging.info(
        "cached files: %d/%d (%.0f%%)", existing, num_accessions, cached_ratio * 100
    )

    to_download = num_accessions - existing

    if to_download > 0:
        with ThreadPoolExecutor(num_workers) as pool:
            futures = [
                pool.submit(
                    _download_worker,
                    downloader,
                    series_group,
                    acc,
                    output_dir,
                    chunk_size,
                )
                for series_group, acc in download_tasks
                if not (output_dir / f"{acc}_family.xml.tgz").exists()
            ]
            pbar = tqdm(total=len(futures), unit="files", desc="downloading accessions")
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception:
                    logging.exception("downloader future raised an exception")
                pbar.update(1)
            pbar.close()

    file_list = [
        (output_dir / f"{acc}_family.xml.tgz", acc) for _, acc in download_tasks
    ]

    pbar = tqdm(total=num_accessions, unit="acc", desc="processing accessions")

    with ThreadPoolExecutor(num_workers) as pool:
        futures = {
            pool.submit(
                _process_one,
                acc,
                path,
                db_file,
                batch_size,
                retries,
                timeout,
                max_timeout,
            ): acc
            for path, acc in file_list
            if path.exists()
        }

        for f in as_completed(futures):
            try:
                f.result()
            except Exception:
                logging.exception("failed to process accession %s", futures[f])
            pbar.update(1)

    pbar.close()
