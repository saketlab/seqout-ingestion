import datetime
import json
import logging
import pickle
import queue
import sqlite3
import tarfile
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import StrEnum
from pathlib import Path
from typing import IO, Optional

from bs4 import BeautifulSoup
from tqdm import tqdm

from helpers import exec_query_from_file, send_req, simple_file_download

_GEO_OUTPUT_DIR = "geo"
_STOP = object()
_MINIML_NAMESPACE = "http://www.ncbi.nlm.nih.gov/geo/info/MINiML"


class TableType(StrEnum):
    Platforms = "Platforms"
    Samples = "Samples"
    Series = "Series"
    Contributors = "Contributors"


def ingest_geo(
    base_output_dir: Path,
    db_file: Optional[Path],
    num_workers: int,
    chunk_size: int,
    batch_size: int,
    series_pickle: Optional[Path],
    series_limit: Optional[int],
):
    output_dir = Path(base_output_dir) / _GEO_OUTPUT_DIR
    output_dir.mkdir(exist_ok=True, parents=True)

    data_dir = output_dir / "data"
    data_dir.mkdir(exist_ok=True, parents=True)

    date_str = datetime.datetime.now().strftime("%Y-%m-%d")
    db_dir = output_dir / "db" / date_str
    db_dir.mkdir(exist_ok=True, parents=True)
    db_file = db_file or (db_dir / f"geo_metadata_{date_str}.db")

    geo_series_pkl_file = series_pickle or (output_dir / "geo_series.pkl")
    geo_dict = _build_geo_dict(num_workers, geo_series_pkl_file, series_limit)

    exec_query_from_file("tables/geo_tables.sql", db_file)
    _run_pipeline(geo_dict, db_file, data_dir, num_workers, batch_size, chunk_size)
    exec_query_from_file("indexes/geo_indexes.sql", db_file)


def _build_geo_dict(
    num_workers: int, pkl_file: Path, series_limit: int | None
) -> dict[str, list[str]]:
    if pkl_file.exists():
        with open(pkl_file, "rb") as f:
            return pickle.load(f)

    geo_dict: dict[str, list[str]] = {}

    series_resp = send_req("GET", "https://ftp.ncbi.nlm.nih.gov/geo/series/")
    assert series_resp is not None, "failed to fetch geo series"

    soup = BeautifulSoup(series_resp.text, "html.parser")
    anchors = soup.find_all("a")
    series_list = [
        str(a["href"][:-1]) for a in anchors if str(a["href"]).startswith("GSE")
    ]

    if series_limit:
        series_list = series_list[:series_limit]

    def _worker(s: str) -> list[str] | None:
        dir_resp = send_req("GET", f"https://ftp.ncbi.nlm.nih.gov/geo/series/{s}")
        if dir_resp is None:
            logging.error("failed to fetch geo accession ids (series=%s)", s)
            return None

        soup = BeautifulSoup(dir_resp.text, "html.parser")
        anchors = soup.find_all("a")
        accessions = [
            str(a["href"][:-1]) for a in anchors if str(a["href"]).startswith("GSE")
        ]

        return accessions

    with (
        ThreadPoolExecutor(num_workers) as pool,
        tqdm(total=len(series_list), desc="building geo series dict") as pbar,
    ):
        futures = {pool.submit(_worker, s): s for s in series_list}

        for future in as_completed(futures):
            series_group = futures[future]
            try:
                accessions = future.result()
            except Exception:
                logging.exception(
                    "failed to fetch accessions for series group %s", series_group
                )
                pbar.update(1)
                continue

            if not accessions:
                pbar.update(1)
                continue

            geo_dict[series_group] = accessions
            pbar.update(1)

    with open(pkl_file, "wb") as f:
        pickle.dump(geo_dict, f)

    return geo_dict


def _download_worker(
    series_group: str,
    accession: str,
    transform_queue: queue.Queue,
    pbar: tqdm,
    output_dir: Path,
    chunk_size: int,
):
    dest_path = output_dir / f"{accession}_family.xml.tgz"
    url = f"https://ftp.ncbi.nlm.nih.gov/geo/series/{series_group}/{accession}/miniml/{accession}_family.xml.tgz"

    if dest_path.exists():
        transform_queue.put((accession, dest_path))
        pbar.update(1)
        return

    try:
        simple_file_download(url, dest_path, chunk_size, (10, 60), False)
    except Exception:
        logging.exception("failed to download %s from %s", accession, url)
        pbar.update(1)
        return

    pbar.update(1)
    transform_queue.put((accession, dest_path))


def _wrap_ns(s: str) -> str:
    return f"{{{_MINIML_NAMESPACE}}}{s}"


def _findtext(root: ET.Element, s: str) -> str | None:
    v = root.findtext(_wrap_ns("Title")) or ""
    return (v or "").strip() or None


def _et_to_dict(elem: ET.Element | list[ET.Element]) -> list | dict | str:
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


def _get_platforms(root: ET.Element) -> list[tuple] | None:
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

        release_date_raw = platform.findtext(
            f"{_wrap_ns('Status')}/{_wrap_ns('Release-Date')}"
        )
        last_update_date_raw = platform.findtext(
            f"{_wrap_ns('Status')}/{_wrap_ns('Last-Update-Date')}"
        )
        published_at: datetime.date | None = None
        updated_at: datetime.date | None = None

        if release_date_raw:
            published_at = datetime.date.fromisoformat(release_date_raw)
        if last_update_date_raw:
            updated_at = datetime.date.fromisoformat(last_update_date_raw)

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


def _get_samples(root: ET.Element) -> list[tuple] | None:
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
        platform_ref = platform_ref_elem.get("ref") if platform_ref_elem else None

        supplementary_raw = _et_to_dict(sample.findall(_wrap_ns("Supplementary-Data")))
        supplementary_data = (
            json.dumps(supplementary_raw) if supplementary_raw else None
        )

        title = _findtext(sample, "Title")
        sample_type = sample.findtext(_wrap_ns("Type"))
        hybridization_protocol = sample.findtext(_wrap_ns("Hybridization-Protocol"))
        scan_protocol = sample.findtext(_wrap_ns("Scan-Protocol"))

        release_date_raw = sample.findtext(
            f"{_wrap_ns('Status')}/{_wrap_ns('Release-Date')}"
        )
        last_update_date_raw = sample.findtext(
            f"{_wrap_ns('Status')}/{_wrap_ns('Last-Update-Date')}"
        )
        published_at: datetime.date | None = None
        updated_at: datetime.date | None = None

        if release_date_raw:
            published_at = datetime.date.fromisoformat(release_date_raw)
        if last_update_date_raw:
            updated_at = datetime.date.fromisoformat(last_update_date_raw)

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


def _get_series(root: ET.Element) -> list[tuple] | None:
    series = root.find(_wrap_ns("Series"))
    if not series:
        return None

    accession = series.get("iid")
    overall_design = _findtext(series, "Overall-Design")
    pubmed_ids_list = [p.text for p in series.findall(_wrap_ns("Pubmed-ID"))]
    pubmed_ids = json.dumps(pubmed_ids_list) if pubmed_ids_list else None
    relations_raw = _et_to_dict(series.findall(_wrap_ns("Relation")))
    relations = json.dumps(relations_raw) if relations_raw else None
    sample_refs_raw = _et_to_dict(series.findall(_wrap_ns("Sample-Refs")))
    sample_refs = json.dumps(sample_refs_raw) if sample_refs_raw else None

    title = _findtext(series, "Title")
    summary = _findtext(series, "Description")
    supplementary_data = json.dumps(
        [
            {"@type": sd.get("type"), "#text": sd.text}
            for sd in series.findall(_wrap_ns("Supplementary-Data"))
        ]
    )
    series_type_raw = series.findtext(_wrap_ns("Type"))
    series_type = json.dumps([series_type_raw]) if series_type_raw else None

    release_date_raw = series.findtext(
        f"{_wrap_ns('Status')}/{_wrap_ns('Release-Date')}"
    )
    last_update_date_raw = series.findtext(
        f"{_wrap_ns('Status')}/{_wrap_ns('Last-Update-Date')}"
    )
    published_at: datetime.date | None = None
    updated_at: datetime.date | None = None

    if release_date_raw:
        published_at = datetime.date.fromisoformat(release_date_raw)
    if last_update_date_raw:
        updated_at = datetime.date.fromisoformat(last_update_date_raw)

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


def _get_contributors(root: ET.Element, accession: str) -> list[tuple] | None:
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


def _transform_worker(
    transform_queue: queue.Queue, write_queue: queue.Queue, output_dir: Path
):
    while True:
        try:
            item = transform_queue.get(timeout=10)
        except queue.Empty:
            continue

        if item is _STOP:
            write_queue.put(_STOP)
            break

        accession, dest_path = item

        try:
            with tarfile.open(dest_path, "r:gz") as tar:
                for member in tar.getmembers():
                    if member.name.endswith(".xml"):
                        f = tar.extractfile(member)
                        if not f:
                            logging.error("failed to extract file (path=%s)", dest_path)
                            continue

                        parsed_data = _parse_miniml(f, accession)
                        for k, v in parsed_data.items():
                            write_queue.put((k, v))

                        break
        except Exception:
            logging.exception("failed to transform %s (path=%s)", accession, dest_path)


def _write_to_sqlite_worker(
    write_queue: queue.Queue, db_file: Path, batch_size: int, sentinels_to_receive: int
):
    conn = sqlite3.Connection(db_file)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    cursor = conn.cursor()

    sentinels_received = 0
    batch_by_type: dict[TableType, list] = {}
    sql_statements: dict[TableType, str] = {
        TableType.Platforms: "INSERT OR REPLACE INTO geo_platforms (accession, description, title, technology, published_at, updated_at, organism, manufacturer, relation) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        TableType.Samples: "INSERT OR REPLACE INTO geo_samples (accession, channel_count, channels, description, platform_ref, published_at, updated_at, supplementary_data, title, sample_type, hybridization_protocol, scan_protocol) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        TableType.Series: "INSERT OR REPLACE INTO geo_series (accession, overall_design, pubmed_id, relation, samples_ref, published_at, updated_at, summary, supplementary_data, title, series_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        TableType.Contributors: "INSERT INTO geo_contributors (accession, person, department, organization, address_line, address_city, address_state, address_zipcode, address_country) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    }

    def _flush(batch: list, table_type: TableType):
        if batch:
            try:
                cursor.executemany(sql_statements[table_type], batch)
                conn.commit()
            except Exception:
                logging.exception("failed to flush batch for table %s", table_type)
            finally:
                batch_by_type[table_type] = []

    while True:
        try:
            item = write_queue.get(timeout=10)
        except queue.Empty:
            continue

        if item is _STOP:
            sentinels_received += 1
            if sentinels_received == sentinels_to_receive:
                for table_type, batch in batch_by_type.items():
                    _flush(batch, table_type)
                break
            continue

        table_type, parsed_data = item
        if not parsed_data:
            continue

        if table_type not in batch_by_type:
            batch_by_type[table_type] = []
        batch_by_type[table_type].extend(parsed_data)

        if len(batch_by_type[table_type]) >= batch_size:
            _flush(batch_by_type[table_type], table_type)

    conn.close()


def _run_pipeline(
    geo_dict: dict[str, list[str]],
    db_file: Path,
    output_dir: Path,
    num_workers: int,
    batch_size: int,
    chunk_size: int,
):
    num_accessions = sum(len(v) for v in geo_dict.values())
    logging.info("loaded %d groups and %d accessions", len(geo_dict), num_accessions)

    if num_accessions == 0:
        logging.warning("geo_dict is empty, nothing to process")
        return

    num_downloaders = max(1, min(num_accessions, int(num_workers * 0.6)))
    num_writer = 1
    num_transformers = max(1, num_workers - num_downloaders - num_writer)

    logging.info(
        "pipeline workers: %d downloaders, %d transformers, %d writer",
        num_downloaders,
        num_transformers,
        num_writer,
    )

    transform_queue: queue.Queue = queue.Queue()
    write_queue: queue.Queue = queue.Queue()

    pbar = tqdm(
        total=num_accessions,
        unit=" accession",
        desc="processing accessions",
        mininterval=2.0,
    )

    with (
        ThreadPoolExecutor(num_downloaders) as downloader_pool,
        ThreadPoolExecutor(num_transformers) as transformer_pool,
        ThreadPoolExecutor(num_writer) as writer_pool,
    ):
        transformer_futures = [
            transformer_pool.submit(
                _transform_worker, transform_queue, write_queue, output_dir
            )
            for _ in range(num_transformers)
        ]

        writer_future = writer_pool.submit(
            _write_to_sqlite_worker, write_queue, db_file, batch_size, num_transformers
        )

        downloader_futures = [
            downloader_pool.submit(
                _download_worker,
                series_group,
                acc,
                transform_queue,
                pbar,
                output_dir,
                chunk_size,
            )
            for series_group, accs in geo_dict.items()
            for acc in accs
        ]

        logging.info("submitted %d download futures", len(downloader_futures))

        for f in as_completed(downloader_futures):
            try:
                f.result()
            except Exception:
                logging.exception("downloader future raised an exception")
                raise

        for _ in range(num_transformers):
            transform_queue.put(_STOP)

        for f in as_completed(transformer_futures):
            try:
                f.result()
            except Exception:
                logging.exception("transformer future raised an exception")
                raise

        try:
            writer_future.result()
        except Exception:
            logging.exception("writer future raised an exception")
            raise

    pbar.close()
