import asyncio
import datetime
import functools
import json
import logging
import queue
import re
import sqlite3
import tarfile
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import StrEnum
from pathlib import Path
from typing import IO

import httpx
import humanfriendly
import lxml.etree as ET  # noqa: N812
from bs4 import BeautifulSoup, Tag
from tqdm.asyncio import tqdm

from helpers import download_file, exec_query_from_file

logger = logging.getLogger(__name__)


class TableType(StrEnum):
    Platforms = "Platforms"
    Samples = "Samples"
    Series = "Series"
    Contributors = "Contributors"


GEO_OUTPUT_DIR = "geo"
MINIML_NAMESPACE = "http://www.ncbi.nlm.nih.gov/geo/info/MINiML"

GEO_SQL_STATEMENTS: dict[TableType, str] = {
    TableType.Platforms: (
        "INSERT OR REPLACE INTO geo_platforms "
        "(accession, description, title, technology, published_at, updated_at, "
        "organism, manufacturer, relation) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ),
    TableType.Samples: (
        "INSERT OR REPLACE INTO geo_samples "
        "(accession, channel_count, channels, description, platform_ref, "
        "published_at, updated_at, supplementary_data, title, sample_type, "
        "hybridization_protocol, scan_protocol) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ),
    TableType.Series: (
        "INSERT OR REPLACE INTO geo_series "
        "(accession, overall_design, pubmed_id, relation, samples_ref, "
        "published_at, updated_at, summary, supplementary_data, title, series_type) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ),
    TableType.Contributors: (
        "INSERT OR REPLACE INTO geo_contributors "
        "(accession, person, department, organization, address_line, address_city, "
        "address_state, address_zipcode, address_country) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    ),
}


async def ingest_geo(
    base_output_dir: Path,
    db_file: Path | None,
    start_date: datetime.date | None,
    end_date: datetime.date | None,
    num_workers: int,
    chunk_size: int,
    batch_size: int,
    check_accession_subdirs: bool,
    series_limit: int | None,
    retries: int,
    timeout_: int,
    max_timeout: int,
) -> None:
    output_dir = Path(base_output_dir) / GEO_OUTPUT_DIR
    output_dir.mkdir(exist_ok=True, parents=True)

    data_dir = output_dir / "data"
    data_dir.mkdir(exist_ok=True, parents=True)

    date_str = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
    (output_dir / "db").mkdir(exist_ok=True, parents=True)
    db_dir = output_dir / "db" / date_str
    db_dir.mkdir(exist_ok=True, parents=True)
    db_file = db_file or (db_dir / f"geo_metadata_{date_str}.db")

    logger.info(
        "starting geo ingest with following config:\n"
        "- num workers: %d\n"
        "- chunk size: %s\n"
        "- batch size: %d\n"
        "- series limit: %s\n"
        "- check accession subdirs: %s\n"
        "- start date: %s\n"
        "- end date: %s\n"
        "- retries: %d\n"
        "- timeout: %s\n"
        "- max timeout: %s",
        num_workers,
        humanfriendly.format_size(chunk_size),
        batch_size,
        str(series_limit) or "None",
        check_accession_subdirs,
        str(start_date) or "None",
        str(end_date) or "None",
        retries,
        humanfriendly.format_timespan(timeout_),
        humanfriendly.format_timespan(max_timeout),
    )

    geo_dict = await _build_geo_dict(
        num_workers,
        series_limit,
        check_accession_subdirs,
        start_date,
        end_date,
        retries,
        timeout_,
        max_timeout,
    )

    downloader = functools.partial(
        download_file,
        num_retries=retries,
        timeout=timeout_,
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
        timeout_,
        max_timeout,
    )
    exec_query_from_file("indexes/geo.sql", db_file)


async def _build_geo_dict(
    num_workers: int,
    series_limit: int | None,
    check_accession_subdirs: bool,
    start_date: datetime.date | None,
    end_date: datetime.date | None,
    retries: int,
    timeout_: int,
    max_timeout: int,
) -> dict[str, list[str]]:
    sem = asyncio.Semaphore(num_workers)
    limits = httpx.Limits(max_connections=num_workers)
    timeout_config = httpx.Timeout(max_timeout, connect=timeout_)

    async with httpx.AsyncClient(
        limits=limits,
        timeout=timeout_config,
    ) as session:
        resp_text = await _get(
            session,
            sem,
            "https://ftp.ncbi.nlm.nih.gov/geo/series",
            retries,
        )
        if resp_text is None:
            msg = "failed to fetch geo series"
            raise ValueError(msg)

        soup = BeautifulSoup(resp_text, "html.parser")
        anchors = soup.find_all("a")
        series_list = [
            str(a["href"][:-1]) for a in anchors if str(a["href"]).startswith("GSE")
        ]

        if series_limit:
            series_list = series_list[:series_limit]

        fetch_all_tasks = [
            _fetch_all_accessions_worker(
                session,
                sem,
                start_date,
                end_date,
                check_accession_subdirs,
                s,
                retries,
            )
            for s in series_list
        ]
        fetch_all_results: list[list[str]] = await tqdm.gather(
            *fetch_all_tasks,
            desc="fetching all accessions",
        )

        all_series_accession_mapping: dict[str, list[str]] = {
            series: accessions
            for series, accessions in zip(series_list, fetch_all_results, strict=False)
            if accessions
        }

        if not check_accession_subdirs or (start_date is None and end_date is None):
            return all_series_accession_mapping

        flat: list[tuple[str, str]] = [
            (series, acc)
            for series, accs in all_series_accession_mapping.items()
            for acc in accs
        ]

        filter_tasks = [
            _filter_accessions_worker(
                session,
                sem,
                acc,
                series,
                start_date,
                end_date,
                retries,
            )
            for series, acc in flat
        ]
        filter_results = await tqdm.gather(*filter_tasks, desc="filtering accessions")

        to_process: dict[str, list[str]] = defaultdict(list)
        for (series, acc), keep in zip(flat, filter_results, strict=False):
            if keep:
                to_process[series].append(acc)

        return dict(to_process)


async def _get(
    session: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    url: str,
    retries: int,
) -> str | None:
    for attempt in range(retries + 1):
        try:
            async with sem:
                resp = await session.get(url)
                resp.raise_for_status()
                return resp.text
        except Exception:
            if attempt == retries:
                logger.exception(
                    "failed to fetch %s after %d retries",
                    url,
                    retries,
                )
                return None
            await asyncio.sleep(2**attempt)
    return None


async def _fetch_all_accessions_worker(
    session: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    start_date: datetime.date | None,
    end_date: datetime.date | None,
    check_accession_subdirs: bool,
    series: str,
    retries: int,
) -> list[str] | None:
    resp_text = await _get(
        session,
        sem,
        f"https://ftp.ncbi.nlm.nih.gov/geo/series/{series}",
        retries,
    )
    if resp_text is None:
        logger.error("failed to fetch geo accession ids for %s series", series)
        return None

    soup = BeautifulSoup(resp_text, "html.parser")
    anchors = soup.find_all("a")
    accessions: list[str] = []

    if check_accession_subdirs:
        accessions = [
            str(a["href"]).rstrip("/")
            for a in anchors
            if str(a["href"]).startswith("GSE")
        ]
    else:
        for a in anchors:
            accession = str(a["href"]).rstrip("/")

            if (
                start_date is not None
                and end_date is not None
                and not _process_anchor_tag(a, start_date, end_date)
            ):
                continue

            accessions.append(accession)
    return accessions


async def _filter_accessions_worker(
    session: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    accession: str,
    series: str,
    start_date: datetime.date | None,
    end_date: datetime.date | None,
    retries: int,
) -> bool:
    resp_text = await _get(
        session,
        sem,
        f"https://ftp.ncbi.nlm.nih.gov/geo/series/{series}/{accession}",
        retries,
    )
    if resp_text is None:
        logger.error("failed to fetch %s geo accession dir", accession)
        return False

    soup = BeautifulSoup(resp_text, "html.parser")

    return any(_process_anchor_tag(a, start_date, end_date) for a in soup.find_all("a"))


def _process_anchor_tag(
    anchor: Tag,
    start_date: datetime.date | None,
    end_date: datetime.date | None,
) -> bool:
    next_sib = anchor.next_sibling
    if next_sib is None:
        return False

    date_match = re.match(r"\s*(\d{4}-\d{2}-\d{2})", str(next_sib))
    if date_match is None:
        return False

    modified_date = datetime.date.fromisoformat(date_match.group(1))
    if start_date and modified_date < start_date:
        return False
    return not (end_date and modified_date > end_date)


def _download_worker(
    downloader: functools.partial,
    series_group: str,
    accession: str,
    output_dir: Path,
    chunk_size: int,
) -> None:
    dest_path = output_dir / f"{accession}_family.xml.tgz"
    url = f"https://ftp.ncbi.nlm.nih.gov/geo/series/{series_group}/{accession}/miniml/{accession}_family.xml.tgz"

    if dest_path.exists():
        return

    try:
        downloader(url, dest_path, chunk_size=chunk_size, with_pbar=False)
    except Exception:
        logger.exception("failed to download %s from %s", accession, url)


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
            platform,
            f"{_wrap_ns('Status')}/{_wrap_ns('Release-Date')}",
        )
        updated_at = _parse_dates(
            platform,
            f"{_wrap_ns('Status')}/{_wrap_ns('Last-Update-Date')}",
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
            ),
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
            sample,
            f"{_wrap_ns('Status')}/{_wrap_ns('Release-Date')}",
        )
        updated_at = _parse_dates(
            sample,
            f"{_wrap_ns('Status')}/{_wrap_ns('Last-Update-Date')}",
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
            ),
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
        series,
        f"{_wrap_ns('Status')}/{_wrap_ns('Release-Date')}",
    )
    updated_at = _parse_dates(
        series,
        f"{_wrap_ns('Status')}/{_wrap_ns('Last-Update-Date')}",
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
        ),
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
                _wrap_ns("Zip-Code"),
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
            ),
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
    conn = sqlite3.connect(str(db_file))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA cache_size=-800000")

    cursor = conn.cursor()
    rows_written = 0
    buffer: dict[TableType, list] = {}

    def _flush(table_type: TableType) -> None:
        nonlocal rows_written
        batch = buffer.get(table_type)
        if batch:
            safe_batch = [tuple(_coerce(v) for v in row) for row in batch]
            cursor.executemany(GEO_SQL_STATEMENTS[table_type], safe_batch)
            conn.commit()
            rows_written += len(batch)
            buffer[table_type] = []

    while True:
        try:
            item = write_queue.get(timeout=1.0)
        except queue.Empty:
            if parse_done.is_set():
                for ft in list(buffer.keys()):
                    _flush(ft)
                break
            continue

        table_type, rows = item
        lst = buffer.setdefault(table_type, [])
        lst.extend(rows)
        if len(lst) >= write_batch_size:
            _flush(table_type)

    conn.close()


def _process_one(
    dest_path: Path,
    accession: str,
    write_queue: queue.Queue,
    write_batch_size: int,
) -> None:
    buffer: dict[TableType, list] = {}

    def _flush(table_type: TableType) -> None:
        rows = buffer.get(table_type)
        if rows:
            write_queue.put((table_type, rows))
            buffer[table_type] = []

    try:
        with tarfile.open(dest_path, "r:gz") as tar:
            for member in tar.getmembers():
                if not member.name.endswith(".xml"):
                    continue
                f = tar.extractfile(member)
                if not f:
                    logger.error("failed to extract file (path=%s)", dest_path)
                    return

                parsed_data = _parse_miniml(f, accession)
                for table_type, rows in parsed_data.items():
                    if not rows:
                        continue
                    lst = buffer.setdefault(table_type, [])
                    lst.extend(rows)
                    if len(lst) >= write_batch_size:
                        _flush(table_type)
                break
    except Exception:
        logger.exception("failed to process %s (path=%s)", accession, dest_path)

    for table_type in buffer:
        _flush(table_type)


def _run_pipeline(
    downloader: functools.partial,
    geo_dict: dict[str, list[str]],
    db_file: Path,
    output_dir: Path,
    num_workers: int,
    batch_size: int,
    chunk_size: int,
    _retries: int,
    _timeout: int,
    _max_timeout: int,
) -> None:
    num_accessions = sum(len(v) for v in geo_dict.values())
    logger.info("loaded %d groups and %d accessions", len(geo_dict), num_accessions)

    if num_accessions == 0:
        logger.warning("geo_dict is empty, nothing to process")
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
    logger.info(
        "cached files: %d/%d (%.0f%%)",
        existing,
        num_accessions,
        cached_ratio * 100,
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
                    logger.exception("downloader future raised an exception")
                pbar.update(1)
            pbar.close()

    file_list = [
        (output_dir / f"{acc}_family.xml.tgz", acc) for _, acc in download_tasks
    ]

    num_parse = max(1, num_workers - 1)
    num_writer = 1

    logger.debug(
        "worker distribution: %d parse, %d writer",
        num_parse,
        num_writer,
    )

    write_queue: queue.Queue = queue.Queue()
    parse_done = threading.Event()

    existing_files = [(path, acc) for path, acc in file_list if path.exists()]

    with (
        ThreadPoolExecutor(max_workers=num_parse) as parse_pool,
        ThreadPoolExecutor(max_workers=num_writer) as writer_pool,
    ):
        write_future = writer_pool.submit(
            _writer_worker,
            write_queue,
            db_file,
            batch_size,
            parse_done,
        )

        parse_futures = [
            parse_pool.submit(_process_one, path, acc, write_queue, batch_size)
            for path, acc in existing_files
        ]

        for f in tqdm(
            as_completed(parse_futures),
            total=len(parse_futures),
            unit="acc",
            desc="processing accessions",
        ):
            try:
                f.result()
            except Exception:
                logger.exception("failed to process accession")

        parse_done.set()
        write_future.result()
