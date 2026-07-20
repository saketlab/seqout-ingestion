"""Ingest open (non-human) GSA — CNCB-NGDC Genome Sequence Archive, CRA studies."""

from __future__ import annotations

import datetime
import functools
import json
import logging
import re
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING

import humanfriendly
from bs4 import BeautifulSoup
from tqdm import tqdm

from helpers import exec_query_from_file, send_req, sqlite_connect
from ingest.gsa_common import (
    GSA_BASE_URL,
    GSA_DOWNLOAD_URL,
    apply_write_pragmas,
    bulk_upsert,
    bulk_upsert_preserve,
    coerce,
    date_filter_active,
    download_studies,
    in_range,
    make_client,
    map_row,
    parse_workbook,
)

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

GSA_OUTPUT_DIR = "gsa"
# CRA accessions are spread across these storage roots; the union is the full set.
GSA_STORAGE_ROOTS = ["gsa", "gsa2", "gsa3", "gsa4", "gsa5"]
GSA_EXPORT_URL = f"{GSA_BASE_URL}/gsa/file/exportExcelFile"
GSA_BROWSE_URL = f"{GSA_BASE_URL}/gsa/browse/"
GSA_SEARCH_URL = f"{GSA_BASE_URL}/gsa/search?searchTerm="
_CRA_RE = re.compile(r"CRA\d+")
# The page renders Chinese or English by client. parse_study_page reads both label sets.
_ENGLISH_HEADERS = {"Accept-Language": "en-US,en;q=0.9"}

# sheet header -> db column maps, matched by name

SAMPLE_MAP = {
    "Accession": "accession",
    "Sample name": "sample_name",
    "Public description": "public_description",
    "Project accession": "project_accession",
    "Sample title": "sample_title",
    "Organism": "organism",
    "Cultivar": "cultivar",
    "Biomaterial provider": "biomaterial_provider",
    "Tissue": "tissue",
    "Age": "age",
    "Age unit": "age_unit",
    "Dev stage": "dev_stage",
    "Cell line": "cell_line",
    "Cell type": "cell_type",
    "Collected by": "collected_by",
    "Collection date": "collection_date",
    "Culture collection": "culture_collection",
    "Disease": "disease",
    "Disease stage": "disease_stage",
    "Genotype": "genotype",
    "Growth protocol": "growth_protocol",
    "Height length": "height_length",
    "Isolation source": "isolation_source",
    "Latitude longitude": "latitude_longitude",
    "Phenotype": "phenotype",
    "Population": "population",
    "Type": "type",
    "Sex": "sex",
    "Specimen voucher": "specimen_voucher",
    "Temperature": "temperature",
    "Treatment": "treatment",
}
SAMPLE_COLUMNS = ["accession", "study_accession", *SAMPLE_MAP.values()]

EXPERIMENT_MAP = {
    "Accession": "accession",
    "Experiment title": "title",
    "BioProject accession": "project_accession",
    "BioSample name": "biosample_name",
    "BioSample accession": "sample_accession",
    "Platform": "platform",
    "Library Construction / Experimental Design": "library_construction",
    "Library name": "library_name",
    "Strategy": "library_strategy",
    "Source": "library_source",
    "Selection": "library_selection",
    "Layout": "library_layout",
    "Read length for mate1(bp)": "read_length_1",
    "Read length for mate 2(bp)": "read_length_2",
    "Insert size (bp)": "insert_size",
    "Nominal size (bp)": "nominal_size",
    "Nominal standard deviation (bp)": "nominal_sdev",
    "Planned number of cycles": "planned_cycles",
    "other_db": "other_db",
    "accession_in_other_db": "accession_in_other_db",
    "other_db_url": "other_db_url",
}
EXPERIMENT_COLUMNS = ["accession", "study_accession", *EXPERIMENT_MAP.values()]

RUN_MAP = {
    "Accession": "accession",
    "Run title": "run_title",
    "Experiment accession": "experiment_accession",
    "BioProject accession": "project_accession",
    "Run data file type": "data_file_type",
    "Read filename 1": "file1_name",
    "Read file1 MD5": "file1_md5",
    "DownLoad Read file1": "file1_url",
    "Read filename 2": "file2_name",
    "Read file2 MD5": "file2_md5",
    "DownLoad Read file2": "file2_url",
    "Index filename 1": "index1_name",
    "Index file1 MD5": "index1_md5",
    "DownLoad Index file1": "index1_url",
    "Index filename 2": "index2_name",
    "Index file2 MD5": "index2_md5",
    "DownLoad Index file2": "index2_url",
    "Reference file name": "reference_name",
    "MD5 for reference file": "reference_md5",
    "Assembly Name or Accession": "assembly_accession",
    "Assembly Accession URL": "assembly_url",
    "other_db": "other_db",
    "accession_in_other_db": "accession_in_other_db",
    "other_db_url": "other_db_url",
}
RUN_COLUMNS = ["accession", "study_accession", *RUN_MAP.values()]

STUDY_COLUMNS = [
    "accession",
    "alias",
    "title",
    "organization",
    "project_accession",
    "organism",
    "n_projects",
    "n_samples",
    "n_experiments",
    "n_runs",
    "release_time",
    "pubmed_id",
]
FETCH_LOG_COLUMNS = [
    "study_accession",
    "status",
    "n_samples",
    "n_experiments",
    "n_runs",
    "fetched_at",
]


def ingest_gsa(
    base_output_dir: Path,
    db_file: Path | None,
    start_date: datetime.date,
    end_date: datetime.date,
    num_workers: int,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> None:
    output_dir = base_output_dir / GSA_OUTPUT_DIR
    data_dir = output_dir / "data" / "studies"
    data_dir.mkdir(parents=True, exist_ok=True)
    meta_dir = output_dir / "data" / "study_meta"
    meta_dir.mkdir(parents=True, exist_ok=True)

    date_str = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
    db_dir = output_dir / "db" / date_str
    db_dir.mkdir(parents=True, exist_ok=True)
    db_file = db_file or (db_dir / f"gsa_metadata_{date_str}.db")

    logger.info(
        "starting gsa (open/CRA) ingest:\n- workers: %d\n- retries: %d\n- timeout: %s",
        num_workers,
        retries,
        humanfriendly.format_timespan(timeout),
    )

    studies = _enumerate_studies(retries, timeout, max_timeout)
    if not studies:
        msg = "no CRA accessions found in the GSA storage indexes"
        raise ValueError(msg)
    logger.info("enumerated %d CRA studies", len(studies))

    study_meta = _scrape_all_study_meta(
        studies,
        meta_dir,
        num_workers,
        retries,
        timeout,
        max_timeout,
    )
    scraped = len(study_meta)
    logger.info("scraped study metadata for %d/%d studies", scraped, len(studies))
    if not study_meta:
        msg = "study-page scrape returned nothing; refusing to ingest null studies"
        raise ValueError(msg)

    if date_filter_active(start_date, end_date):
        studies = [
            acc
            for acc in studies
            if in_range(
                study_meta.get(acc, {}).get("release_time"),
                start_date,
                end_date,
            )
        ]
        logger.info(
            "date filter %s..%s -> %d studies",
            start_date,
            end_date,
            len(studies),
        )

    fetch = functools.partial(
        _fetch_study_xlsx,
        retries=retries,
        timeout=timeout,
        max_timeout=max_timeout,
    )
    statuses = download_studies(
        studies,
        data_dir,
        fetch,
        num_workers,
        desc="downloading gsa workbooks",
    )
    _log_status_counts(statuses)

    exec_query_from_file("tables/gsa.sql", db_file)
    _ingest(db_file, data_dir, studies, study_meta, statuses, date_str)
    exec_query_from_file("indexes/gsa.sql", db_file)
    logger.info("gsa ingest complete: %s", db_file)


def _enumerate_studies(retries: int, timeout: int, max_timeout: int) -> list[str]:
    accessions: set[str] = set()
    with make_client(timeout) as client:
        for root in GSA_STORAGE_ROOTS:
            r = send_req(
                "GET",
                f"{GSA_DOWNLOAD_URL}/{root}/",
                num_retries=retries,
                timeout=timeout,
                max_wait=max_timeout,
                client=client,
            )
            accessions.update(_CRA_RE.findall(r.text))
    return sorted(accessions)


def _fetch_study_xlsx(
    accession: str,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> bytes | None:
    with make_client(timeout) as client:
        r = send_req(
            "POST",
            GSA_EXPORT_URL,
            data={"type": "3", "dlAcession": accession, "fileName": ""},
            headers={"Referer": f"{GSA_BROWSE_URL}{accession}"},
            num_retries=retries,
            timeout=timeout,
            max_wait=max_timeout,
            client=client,
        )
        content_type = r.headers.get("content-type", "")
        if "excel" not in content_type and "spreadsheet" not in content_type:
            logger.warning(
                "unexpected content-type for %s: %s",
                accession,
                content_type,
            )
            return None
        return r.content


_STUDY_FIELDS = {
    "title": ("标题", "Title"),
    "release_time": ("发布日期", "Release date"),
    "organization": ("单位", "Organization"),
}
# Values run to the next label ("单位: X 提交日期: Y"). Multi-word labels are spelled
# out; a one-token terminator would split "Submitted date:" at "date:".
_NEXT_LABEL = (
    r"(?=\s*(?:Release date|Submission date|Submitted date|File size|\S{1,12})"
    r"\s*[:：]|$)"  # noqa: RUF001
)
# Titles carry their own "<token>:" ("...3g/kg and 12g/kg: a comparative analysis").
# Only a known label ends a title.
_PAGE_LABELS = (
    "Release date|Submission date|Submitted date|Organization|Submitter"
    "|BioProject|Platform|Status|Files|File size"
    "|标题|发布日期|提交日期|项目编号|文件个数|文件大小|单位|提交者"
)
_NEXT_KNOWN_LABEL = rf"(?=\s*(?:{_PAGE_LABELS})\s*[:：]|$)"  # noqa: RUF001
_STRICT_FIELDS = frozenset({"title"})
# the study page's 出版信息 / publication block lists the submitter-declared papers
_PUBMED_RE = re.compile(r"(?:PubMed ID|PubMed)\s*[:：]?\s*(\d{6,9})")  # noqa: RUF001

# A <th>label</th><td>value</td> table. Declares no publications; pmids come from
# `backfill pm-ids`, as they do for HRA.
_SEARCH_FIELDS = {
    "title": ("Title", "标题"),
    "release_time": ("Release date", "发布日期"),
    "organization": ("Organization", "单位"),
}


def _scrape_all_study_meta(
    studies: list[str],
    meta_dir: Path,
    num_workers: int,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> dict[str, dict]:
    """
    Scrape every study's page. Studies whose page fails are simply absent.

    Each page is cached to disk. CNCB throttles a crawl this size (18k pages) down
    to 503s and dropped connections, so a run that dies partway must resume rather
    than pay for the whole scrape again.
    """
    meta: dict[str, dict] = {}
    todo: list[str] = []
    for acc in studies:
        cached = _cached_study_meta(meta_dir / f"{acc}.json")
        if cached is None:
            todo.append(acc)
        else:
            meta[acc] = cached

    if meta:
        logger.info("study meta: %d/%d already cached", len(meta), len(studies))
    if not todo:
        return meta

    use_search = not _browse_is_up(todo[0], timeout, max_timeout)
    if use_search:
        logger.warning("gsa /browse/ is down -- falling back to /search (no pmids)")

    with (
        ThreadPoolExecutor(num_workers) as pool,
        tqdm(total=len(todo), unit="study", desc="scraping gsa study pages") as pbar,
    ):
        futures = {
            pool.submit(
                _scrape_study_meta,
                acc,
                meta_dir / f"{acc}.json",
                retries,
                timeout,
                max_timeout,
                use_search=use_search,
            ): acc
            for acc in todo
        }
        for f in as_completed(futures):
            acc = futures[f]
            got = f.result()
            if got:
                meta[acc] = got
            pbar.update(1)

    return meta


def _cached_study_meta(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        path.unlink(missing_ok=True)
        return None


def _scrape_study_meta(
    accession: str,
    cache_path: Path,
    retries: int,
    timeout: int,
    max_timeout: int,
    *,
    use_search: bool = False,
) -> dict | None:
    """
    Study metadata: title / organization / release_time, which live nowhere else.

    The bulk listing stopped returning CRA rows, so per-study is the only source.
    /browse/ and /search serve the same fields and CNCB drops them independently;
    /browse/ is preferred because it alone declares the submitter's publications.
    """
    base = GSA_SEARCH_URL if use_search else GSA_BROWSE_URL
    url = f"{base}{accession}"
    try:
        with make_client(timeout) as client:
            r = send_req(
                "GET",
                url,
                headers=_ENGLISH_HEADERS,
                num_retries=retries,
                timeout=timeout,
                max_wait=max_timeout,
                client=client,
            )
        meta = (
            parse_search_page(r.text, accession)
            if use_search
            else parse_study_page(r.text)
        )
    except Exception:
        logger.debug("study meta scrape failed for %s", accession, exc_info=True)
        return None

    if meta:
        cache_path.write_text(json.dumps(meta))
    return meta


######################################################################
def _browse_is_up(accession: str, timeout: int, max_timeout: int) -> bool:
    """One probe per run: browse and search are both throttled, so don't try both."""
    try:
        with make_client(timeout) as client:
            send_req(
                "GET",
                f"{GSA_BROWSE_URL}{accession}",
                headers=_ENGLISH_HEADERS,
                num_retries=1,
                timeout=timeout,
                max_wait=max_timeout,
                client=client,
            )
    except Exception:
        return False
    return True


def _pubmed_json(pmids: list[str] | None) -> str | None:
    return json.dumps(pmids) if pmids else None


def parse_study_page(html: str) -> dict:
    """Per-study page -> {title, organization, release_time, pubmed_ids}."""
    text = " ".join(BeautifulSoup(html, "html.parser").get_text(" ").split())

    meta: dict = {}
    for key, labels in _STUDY_FIELDS.items():
        terminator = _NEXT_KNOWN_LABEL if key in _STRICT_FIELDS else _NEXT_LABEL
        for label in labels:
            m = re.search(
                rf"{re.escape(label)}\s*[:：]\s*(.*?){terminator}",  # noqa: RUF001
                text,
            )
            if not m:
                continue
            value = m.group(1).strip()
            if value:
                meta[key] = value
                break

    pmids = list(dict.fromkeys(_PUBMED_RE.findall(text)))
    if pmids:
        meta["pubmed_ids"] = pmids

    return meta


######################################################################
def parse_search_page(html: str, accession: str) -> dict:
    """Search result -> {title, organization, release_time}. No publications."""
    cells: dict[str, str] = {}
    for row in BeautifulSoup(html, "html.parser").find_all("tr"):
        th, td = row.find("th"), row.find("td")
        if th and td:
            label = " ".join(th.get_text(" ").split())
            cells[label] = " ".join(td.get_text(" ").split())

    # a miss renders a result list -- never attribute another study's title
    if not any(v.upper() == accession.upper() for v in cells.values()):
        return {}

    meta: dict = {}
    for key, labels in _SEARCH_FIELDS.items():
        for label in labels:
            value = cells.get(label)
            if value:
                meta[key] = value
                break

    return meta


def _ingest(
    db_file: Path,
    data_dir: Path,
    studies: list[str],
    study_meta: dict[str, dict],
    statuses: dict[str, str],
    date_str: str,
) -> None:
    study_rows: list[tuple] = []
    fetch_log_rows: list[tuple] = []
    with sqlite_connect(db_file) as conn:
        apply_write_pragmas(conn)
        for acc in tqdm(studies, unit="study", desc="loading gsa workbooks"):
            xlsx_path = data_dir / f"{acc}.xlsx"
            samples: list[dict] = []
            experiments: list[dict] = []
            runs: list[dict] = []
            status = statuses.get(acc, "missing")
            parsed_ok = False
            if xlsx_path.exists():
                try:
                    sheets = parse_workbook(xlsx_path.read_bytes())
                    # some CRA workbooks split samples across Sample1/Sample2/Sample3
                    # sheets instead of a single "Sample".
                    samples = [
                        r
                        for name, rows in sheets.items()
                        if name.startswith("Sample")
                        for r in rows
                    ]
                    experiments = sheets.get("Experiment", [])
                    runs = sheets.get("Run", [])
                    status = "ok" if (samples or experiments or runs) else "empty"
                    parsed_ok = True
                except Exception:
                    # drop a corrupt cached workbook so the next run refetches it.
                    logger.exception(
                        "failed to parse %s; removing to refetch", xlsx_path
                    )
                    xlsx_path.unlink(missing_ok=True)
                    status = "failed"

            if parsed_ok:
                # clear this study's prior child rows before reinserting, so rows
                # dropped upstream don't linger.
                static = {"study_accession": acc}
                for table in ("gsa_samples", "gsa_experiments", "gsa_runs"):
                    conn.execute(
                        f"DELETE FROM {table} WHERE study_accession = ?",  # noqa: S608
                        (acc,),
                    )
                bulk_upsert(
                    conn,
                    "gsa_samples",
                    SAMPLE_COLUMNS,
                    [map_row(r, SAMPLE_MAP, SAMPLE_COLUMNS, static) for r in samples],
                )
                bulk_upsert(
                    conn,
                    "gsa_experiments",
                    EXPERIMENT_COLUMNS,
                    [
                        map_row(r, EXPERIMENT_MAP, EXPERIMENT_COLUMNS, static)
                        for r in experiments
                    ],
                )
                bulk_upsert(
                    conn,
                    "gsa_runs",
                    RUN_COLUMNS,
                    [map_row(r, RUN_MAP, RUN_COLUMNS, static) for r in runs],
                )

            meta = study_meta.get(acc, {})
            project = _first(samples, "Project accession") or _first(
                experiments,
                "BioProject accession",
            )
            projects = {p for p in _column(samples, "Project accession") if p}
            study_rows.append(
                (
                    acc,
                    meta.get("alias"),
                    meta.get("title"),
                    meta.get("organization"),
                    project,
                    _most_common(samples, "Organism"),
                    len(projects) or None,
                    len(samples) or None,
                    len(experiments) or None,
                    len(runs) or None,
                    meta.get("release_time"),
                    _pubmed_json(meta.get("pubmed_ids")),
                ),
            )
            fetch_log_rows.append(
                (acc, status, len(samples), len(experiments), len(runs), date_str),
            )

        # preserve backfill-owned pubmed columns
        bulk_upsert_preserve(
            conn,
            "gsa_studies",
            STUDY_COLUMNS,
            study_rows,
            "accession",
        )
        bulk_upsert(conn, "gsa_fetch_log", FETCH_LOG_COLUMNS, fetch_log_rows)
        conn.commit()


def _column(rows: list[dict], key: str) -> list:
    return [r.get(key) for r in rows]


def _first(records: list[dict], header: str) -> str | None:
    for record in records:
        value = coerce(record.get(header))
        if value:
            return value
    return None


def _most_common(records: list[dict], header: str) -> str | None:
    values = [v for r in records if (v := coerce(r.get(header)))]
    if not values:
        return None
    return Counter(values).most_common(1)[0][0]


def _log_status_counts(statuses: dict[str, str]) -> None:
    counts: dict[str, int] = {}
    for status in statuses.values():
        counts[status] = counts.get(status, 0) + 1
    logger.info("download status: %s", counts)
