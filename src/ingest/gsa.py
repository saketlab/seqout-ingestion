"""Ingest open (non-human) GSA — CNCB-NGDC Genome Sequence Archive, CRA studies."""

from __future__ import annotations

import datetime
import functools
import logging
import re
from collections import Counter
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
    to_int,
)

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

GSA_OUTPUT_DIR = "gsa"
# CRA accessions are spread across these storage roots; the union is the full set.
GSA_STORAGE_ROOTS = ["gsa", "gsa2", "gsa3", "gsa4", "gsa5"]
GSA_EXPORT_URL = f"{GSA_BASE_URL}/gsa/file/exportExcelFile"
GSA_BROWSE_URL = f"{GSA_BASE_URL}/gsa/browse/"
_CRA_RE = re.compile(r"CRA\d+")
_BROWSE_PAGE_SIZE = 100

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

    study_meta, scrape_complete = _scrape_browse_meta(retries, timeout, max_timeout)
    if date_filter_active(start_date, end_date):
        # only trust the date filter if the scrape was exhaustive; a partial scrape
        # would wrongly drop in-range studies it never saw.
        if scrape_complete:
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
        else:
            logger.warning(
                "browse scrape incomplete; ignoring --start-date/--end-date and "
                "ingesting all %d studies",
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


def _scrape_browse_meta(
    retries: int,
    timeout: int,
    max_timeout: int,
) -> tuple[dict[str, dict], bool]:
    """Page the browse listing per CRA; complete=False if paging stopped early."""
    meta: dict[str, dict] = {}
    try:
        with make_client(timeout) as client:
            page = 1
            while True:
                r = send_req(
                    "POST",
                    GSA_BROWSE_URL,
                    data={"pageNo": str(page), "pageSize": str(_BROWSE_PAGE_SIZE)},
                    num_retries=retries,
                    timeout=timeout,
                    max_wait=max_timeout,
                    client=client,
                )
                found = _parse_browse_page(r.text, meta)
                if not found:
                    break
                page += 1
    except Exception:
        logger.warning("browse scrape failed after %d studies; continuing", len(meta))
        logger.debug("browse scrape error", exc_info=True)
        return meta, False
    else:
        logger.info("scraped browse metadata for %d studies", len(meta))
        return meta, True


def _parse_browse_page(html: str, meta: dict[str, dict]) -> int:
    """Parse one browse page into ``meta``; return the number of new rows found."""
    soup = BeautifulSoup(html, "html.parser")
    added = 0
    for table in soup.find_all("table"):
        if "CRA" not in table.get_text():
            continue
        for row in table.find_all("tr"):
            cells = [c.get_text(strip=True) for c in row.find_all("td")]
            if not cells:
                continue
            match = _CRA_RE.search(" ".join(cells))
            if not match:
                continue
            acc = match.group(0)
            if acc in meta:
                continue
            # cell order: accession, alias, project/sample/exp/run counts, release
            padded = cells + [""] * (7 - len(cells))
            meta[acc] = {
                "alias": coerce(padded[1]),
                "n_projects": to_int(padded[2]),
                "n_samples": to_int(padded[3]),
                "n_experiments": to_int(padded[4]),
                "n_runs": to_int(padded[5]),
                "release_time": coerce(padded[6]),
            }
            added += 1
    return added


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
            study_rows.append(
                (
                    acc,
                    meta.get("alias"),
                    meta.get("title"),
                    meta.get("organization"),
                    project,
                    _most_common(samples, "Organism"),
                    meta.get("n_projects"),
                    meta.get("n_samples"),
                    meta.get("n_experiments"),
                    meta.get("n_runs"),
                    meta.get("release_time"),
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
