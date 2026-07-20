"""
Ingest GSA-Human — CNCB-NGDC Genome Sequence Archive for Human, HRA studies.

Only open-access studies get per-study detail; controlled-access studies keep a
study row but no detail.
"""

from __future__ import annotations

import datetime
import functools
import logging
import re
from typing import TYPE_CHECKING

import httpx
import humanfriendly
from tqdm import tqdm

from helpers import batch_query_in, exec_query_from_file, send_req, sqlite_connect
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

GSA_HUMAN_OUTPUT_DIR = "gsa_human"
STUDY_LIST_URL = f"{GSA_BASE_URL}/gsa-human/file/downLoadFilterFiles"
HUMAN_EXPORT_URL = f"{GSA_BASE_URL}/gsa-human/file/exportExcelFile"
HUMAN_BROWSE_URL = f"{GSA_BASE_URL}/gsa-human/browse/"
# md5sum.txt lines: md5<TAB>/study/[batch/]run/filename
MANIFEST_URL_TMPL = f"{GSA_DOWNLOAD_URL}/gsa-human/{{acc}}/md5sum.txt"
GSA_HUMAN_FILE_BASE = f"{GSA_DOWNLOAD_URL}/gsa-human"
_FILENAME_TMPL = "/data/gsa-Human/webApp/gsa-human/batchExcel/human/{acc}/{acc}.xlsx"
_STUDY_ID_RE = re.compile(r"study_id\s*=\s*'(\d+)'")
_RUN_ACC_RE = re.compile(r"^HRR\d+$")
_MIN_RUN_PATH_SEGMENTS = 2  # run + filename

RUN_FILES_COLUMNS = [
    "run_accession",
    "study_accession",
    "file_ord",
    "file_name",
    "file_md5",
    "file_url",
]

STUDY_COLUMNS = [
    "accession",
    "title",
    "organization",
    "dac",
    "access",
    "n_requests",
    "n_effective_requests",
    "n_approved",
    "project_accession",
    "strategy",
    "study_type",
    "disease",
    "n_individuals",
    "n_samples",
    "n_experiments",
    "release_time",
    "last_processed",
]

# per-study sheet header -> db column maps, matched by name.
# HRA workbooks come in three layouts. Individual and Cell Line sheets both carry
# HRI accessions and fold into gsa_human_individuals; the older
# "Individuals & samples" + "Files" layout is handled separately below.

# Individual sheet -> gsa_human_individuals (entity_type='individual')
INDIVIDUAL_MAP = {
    "Accession": "accession",
    "Individual Identifier": "identifier",
    "Gender": "gender",
    "Ethnicity": "ethnicity",
    "Disease Name": "disease_name",
    "Disease Stage": "disease_stage",
    "Treatment": "treatment",
    "Experiment Group": "experiment_group",
}
# Cell Line sheet -> gsa_human_individuals (entity_type='cell_line')
CELL_LINE_MAP = {
    "Accession": "accession",
    "Cell Line Name": "identifier",
    "Gender": "gender",
    "Tissue": "tissue",
    "Ethnicity": "ethnicity",
    "Web Url": "web_url",
}
INDIVIDUAL_COLUMNS = [
    "accession",
    "study_accession",
    "entity_type",
    "identifier",
    "gender",
    "tissue",
    "ethnicity",
    "disease_name",
    "disease_stage",
    "treatment",
    "experiment_group",
    "web_url",
]

SAMPLE_MAP = {
    "Accession": "accession",
    "Sample Name": "sample_name",
    "Individual Accession": "individual_accession",
    "Tissue": "tissue",
    "Age": "age",
    "Age Unit": "age_unit",
    "Public Description": "public_description",
    "Sample Title": "sample_title",
    "Sample Description": "sample_description",
    "Collection Date": "collection_date",
    "Biomaterial Provider": "biomaterial_provider",
    "Geographic Location": "geographic_location",
    "Culture Collection": "culture_collection",
    "Karyotype": "karyotype",
    "Phenotype": "phenotype",
    "Population": "population",
}
SAMPLE_COLUMNS = ["accession", "study_accession", *SAMPLE_MAP.values()]

EXPERIMENT_MAP = {
    "Accession": "accession",
    "Experiment title": "title",
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
}
EXPERIMENT_COLUMNS = ["accession", "study_accession", *EXPERIMENT_MAP.values()]

RUN_MAP = {
    "Accession": "accession",
    "Run title": "run_title",
    "Experiment accession": "experiment_accession",
    "Run data file type": "data_file_type",
    "Read filename 1": "file1_name",
    "Read file1 MD5": "file1_md5",
    "DownLoad Read file1": "file1_url",
    "Read filename 2": "file2_name",
    "Read file2 MD5": "file2_md5",
    "DownLoad Read file2": "file2_url",
    "Read filename 3": "file3_name",
    "Read file3 MD5": "file3_md5",
    "DownLoad Read file3": "file3_url",
    "Index filename 1": "index1_name",
    "Index file1 MD5": "index1_md5",
    "Index filename 2": "index2_name",
    "Index file2 MD5": "index2_md5",
    "DownLoad Index file2": "index2_url",
    "Reference file name": "reference_name",
    "MD5 for reference file": "reference_md5",
    "Assembly Name or Accession": "assembly_accession",
    "Omics type": "omics_type",
}
# unions RUN_MAP (Run sheet) and FILES_MAP (Files sheet) so both layouts share one
# column list; columns absent from a row map to NULL.
RUN_COLUMNS = [
    "accession",
    "study_accession",
    *RUN_MAP.values(),
    "files",
    "platform",
    "sample_accession",
]

# Older combined layout: one "Individuals & samples" sheet + a thin "Files" sheet.
INDIV_SAMPLE_INDIVIDUAL_MAP = {
    "Individual Accession": "accession",
    "Individual Identifier": "identifier",
    "Gender": "gender",
}
INDIV_SAMPLE_SAMPLE_MAP = {
    "Sample Accession": "accession",
    "Sample Name": "sample_name",
    "Sample Description": "sample_description",
    "Individual Accession": "individual_accession",
}
FILES_MAP = {
    "Run accession": "accession",
    "Files": "files",
    "Experiment": "experiment_accession",
    "Platform": "platform",
    "Sample": "sample_accession",
}
# synthesise experiment rows from the Files sheet (no Experiment sheet in that layout).
FILES_EXPERIMENT_MAP = {
    "Experiment": "accession",
    "Sample": "sample_accession",
    "Platform": "platform",
}

FETCH_LOG_COLUMNS = [
    "study_accession",
    "status",
    "study_id",
    "n_individuals",
    "n_samples",
    "n_experiments",
    "n_runs",
    "fetched_at",
]


def ingest_gsa_human(
    base_output_dir: Path,
    db_file: Path | None,
    start_date: datetime.date,
    end_date: datetime.date,
    num_workers: int,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> None:
    output_dir = base_output_dir / GSA_HUMAN_OUTPUT_DIR
    data_dir = output_dir / "data"
    studies_dir = data_dir / "studies"
    studies_dir.mkdir(parents=True, exist_ok=True)

    date_str = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
    db_dir = output_dir / "db" / date_str
    db_dir.mkdir(parents=True, exist_ok=True)
    db_file = db_file or (db_dir / f"gsa_human_metadata_{date_str}.db")

    logger.info(
        "starting gsa-human (HRA) ingest:\n- workers: %d\n- retries: %d\n- timeout: %s",
        num_workers,
        retries,
        humanfriendly.format_timespan(timeout),
    )

    # 1. study list + study-level metadata, all in one export
    study_list_bytes = _fetch_study_list(retries, timeout, max_timeout)
    (data_dir / "Study.xlsx").write_bytes(study_list_bytes)
    sheets = parse_workbook(study_list_bytes)
    study_records = next(iter(sheets.values()), [])
    if not study_records:
        msg = "gsa-human study list export was empty"
        raise ValueError(msg)
    logger.info("enumerated %d HRA studies", len(study_records))

    # 2. detail subset: open-access studies only, optionally date-filtered
    detail_records = [
        r for r in study_records if (coerce(r.get("Access")) or "").lower() == "open"
    ]
    if date_filter_active(start_date, end_date):
        detail_records = [
            r
            for r in detail_records
            if in_range(coerce(r.get("Release time")), start_date, end_date)
        ]
    detail_accs = [acc for r in detail_records if (acc := coerce(r.get("Study")))]
    logger.info(
        "%d open-access studies selected for per-study detail fetch",
        len(detail_accs),
    )

    fetch = functools.partial(
        _fetch_study_detail,
        studies_dir=studies_dir,
        retries=retries,
        timeout=timeout,
        max_timeout=max_timeout,
    )
    statuses = download_studies(
        detail_accs,
        studies_dir,
        fetch,
        num_workers,
        desc="downloading gsa-human workbooks",
    )
    _log_status_counts(statuses)

    # 2b. per-study md5sum.txt manifests -> authoritative run download files
    fetch_manifest = functools.partial(
        _fetch_manifest, retries=retries, timeout=timeout, max_timeout=max_timeout
    )
    download_studies(
        detail_accs,
        studies_dir,
        fetch_manifest,
        num_workers,
        desc="downloading gsa-human manifests",
        suffix=".md5sum.txt",
    )

    # 3. load
    exec_query_from_file("tables/gsa_human.sql", db_file)
    _ingest(db_file, studies_dir, study_records, detail_accs, statuses, date_str)
    exec_query_from_file("indexes/gsa_human.sql", db_file)
    logger.info("gsa-human ingest complete: %s", db_file)


def _fetch_study_list(retries: int, timeout: int, max_timeout: int) -> bytes:
    with make_client(timeout) as client:
        # GET the browse page first to obtain the JSESSIONID cookie the export needs.
        send_req(
            "GET",
            HUMAN_BROWSE_URL,
            num_retries=retries,
            timeout=timeout,
            max_wait=max_timeout,
            client=client,
        )
        r = send_req(
            "POST",
            STUDY_LIST_URL,
            data={"pageNo": "1", "pageSize": "10"},  # empty filters => whole archive
            headers={"Referer": HUMAN_BROWSE_URL},
            num_retries=retries,
            timeout=timeout,
            max_wait=max_timeout,
            client=client,
        )
        content_type = r.headers.get("content-type", "")
        if "excel" not in content_type and "spreadsheet" not in content_type:
            msg = f"gsa-human study list: unexpected content-type {content_type}"
            raise ValueError(msg)
        return r.content


def _fetch_study_detail(
    accession: str,
    studies_dir: Path,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> bytes | None:
    with make_client(timeout) as client:
        # GET the study page: yields the internal study_id and a session cookie.
        page = send_req(
            "GET",
            f"{HUMAN_BROWSE_URL}{accession}",
            num_retries=retries,
            timeout=timeout,
            max_wait=max_timeout,
            client=client,
        )
        match = _STUDY_ID_RE.search(page.text)
        if not match:
            logger.warning("no study_id on browse page for %s (controlled?)", accession)
            return None
        study_id = match.group(1)
        # sidecar so a resumed run (cached xlsx) can still log study_id.
        (studies_dir / f"{accession}.study_id").write_text(study_id)
        r = send_req(
            "POST",
            HUMAN_EXPORT_URL,
            params={
                "fileName": _FILENAME_TMPL.format(acc=accession),
                "study_id": study_id,
                "requestFlag": "0",
            },
            headers={"Referer": f"{HUMAN_BROWSE_URL}{accession}"},
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


def _fetch_manifest(
    accession: str,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> bytes | None:
    # download host has no WAF, so no browser-UA client needed.
    try:
        r = send_req(
            "GET",
            MANIFEST_URL_TMPL.format(acc=accession),
            num_retries=retries,
            timeout=timeout,
            max_wait=max_timeout,
        )
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == httpx.codes.NOT_FOUND:
            return None
        raise
    return r.content


def _parse_manifest(text: str, study_acc: str) -> list[tuple]:
    """Parse 'md5<TAB>/study/[batch/]run/filename' lines into run-file rows."""
    rows: list[tuple] = []
    ord_by_run: dict[str, int] = {}
    for line in text.splitlines():
        md5, tab, path = line.partition("\t")
        if not tab:
            continue
        md5, path = md5.strip(), path.strip()
        segments = [s for s in path.split("/") if s]
        if len(segments) < _MIN_RUN_PATH_SEGMENTS:
            continue
        run = segments[-2]  # /study/[batch/]run/filename
        if not _RUN_ACC_RE.match(run):
            continue
        order = ord_by_run.get(run, 0)
        ord_by_run[run] = order + 1
        url = f"{GSA_HUMAN_FILE_BASE}{path}"
        rows.append((run, study_acc, order, segments[-1], md5 or None, url))
    return rows


def _ingest(
    db_file: Path,
    studies_dir: Path,
    study_records: list[dict],
    detail_accs: list[str],
    statuses: dict[str, str],
    date_str: str,
) -> None:
    with sqlite_connect(db_file) as conn:
        apply_write_pragmas(conn)

        study_rows = [_study_row(r) for r in study_records if coerce(r.get("Study"))]
        # preserve backfill-owned pubmed columns
        bulk_upsert_preserve(
            conn,
            "gsa_human_studies",
            STUDY_COLUMNS,
            study_rows,
            "accession",
        )

        fetch_log_rows: list[tuple] = []
        for acc in tqdm(detail_accs, unit="study", desc="loading gsa-human workbooks"):
            xlsx_path = studies_dir / f"{acc}.xlsx"
            parsed: dict[str, list[tuple]] | None = None
            status = statuses.get(acc, "missing")
            if xlsx_path.exists():
                try:
                    parsed = _parse_study(parse_workbook(xlsx_path.read_bytes()), acc)
                    status = "ok" if any(parsed.values()) else "empty"
                except Exception:
                    # drop a corrupt cached workbook so the next run refetches it.
                    logger.exception(
                        "failed to parse %s; removing to refetch", xlsx_path
                    )
                    xlsx_path.unlink(missing_ok=True)
                    status = "failed"

            counts = dict.fromkeys(_CHILD_TABLES, 0)
            if parsed is not None:
                # clear prior child rows for this study before reinserting.
                for table, columns in _CHILD_TABLES.items():
                    conn.execute(
                        f"DELETE FROM {table} WHERE study_accession = ?",  # noqa: S608
                        (acc,),
                    )
                    bulk_upsert(conn, table, columns, parsed[table])
                    counts[table] = len(parsed[table])

            # run files come from the manifest, independent of the workbook.
            manifest_path = studies_dir / f"{acc}.md5sum.txt"
            if manifest_path.exists():
                conn.execute(
                    "DELETE FROM gsa_human_run_files WHERE study_accession = ?",
                    (acc,),
                )
                bulk_upsert(
                    conn,
                    "gsa_human_run_files",
                    RUN_FILES_COLUMNS,
                    _parse_manifest(manifest_path.read_text(), acc),
                )

            sid_path = studies_dir / f"{acc}.study_id"
            study_id = sid_path.read_text().strip() if sid_path.exists() else None
            fetch_log_rows.append(
                (
                    acc,
                    status,
                    study_id,
                    counts["gsa_human_individuals"],
                    counts["gsa_human_samples"],
                    counts["gsa_human_experiments"],
                    counts["gsa_human_runs"],
                    date_str,
                ),
            )

        # controlled-access boundary: a study flipped Open -> Controlled drops out of
        # detail_accs, so purge child rows for every currently non-open study.
        controlled = [
            acc
            for r in study_records
            if (acc := coerce(r.get("Study")))
            and (coerce(r.get("Access")) or "").lower() != "open"
        ]
        if controlled:
            for table in _CHILD_TABLES:
                batch_query_in(
                    conn,
                    f"DELETE FROM {table} WHERE study_accession",  # noqa: S608
                    controlled,
                    batch_size=900,
                )

        bulk_upsert(conn, "gsa_human_fetch_log", FETCH_LOG_COLUMNS, fetch_log_rows)
        conn.commit()


# child table -> its column list, in insert order.
_CHILD_TABLES = {
    "gsa_human_individuals": INDIVIDUAL_COLUMNS,
    "gsa_human_samples": SAMPLE_COLUMNS,
    "gsa_human_experiments": EXPERIMENT_COLUMNS,
    "gsa_human_runs": RUN_COLUMNS,
}


def _parse_study(sheets: dict[str, list[dict]], acc: str) -> dict[str, list[tuple]]:
    """Fold whichever of the three HRA layouts this workbook uses into child rows."""
    individuals = [
        row
        for r in sheets.get("Individual", [])
        if (row := _child_row(r, INDIVIDUAL_MAP, INDIVIDUAL_COLUMNS, acc, "individual"))
    ]
    individuals += [
        row
        for r in sheets.get("Cell Line", [])
        if (row := _child_row(r, CELL_LINE_MAP, INDIVIDUAL_COLUMNS, acc, "cell_line"))
    ]
    samples = [
        row
        for r in sheets.get("Sample", [])
        if (row := _child_row(r, SAMPLE_MAP, SAMPLE_COLUMNS, acc))
    ]
    experiments = [
        row
        for r in sheets.get("Experiment", [])
        if (row := _child_row(r, EXPERIMENT_MAP, EXPERIMENT_COLUMNS, acc))
    ]
    runs = [
        row
        for r in sheets.get("Run", [])
        if (row := _child_row(r, RUN_MAP, RUN_COLUMNS, acc))
    ]

    # older combined layout: "Individuals & samples" carries both, "Files" the runs.
    for r in sheets.get("Individuals & samples", []):
        if row := _child_row(
            r, INDIV_SAMPLE_INDIVIDUAL_MAP, INDIVIDUAL_COLUMNS, acc, "individual"
        ):
            individuals.append(row)
        if row := _child_row(r, INDIV_SAMPLE_SAMPLE_MAP, SAMPLE_COLUMNS, acc):
            samples.append(row)
    files_sheet = sheets.get("Files", [])
    runs += [
        row for r in files_sheet if (row := _child_row(r, FILES_MAP, RUN_COLUMNS, acc))
    ]
    # the Files layout has no Experiment sheet; synthesise experiment rows from the
    # Files sheet's Experiment/Platform/Sample columns.
    experiments += [
        row
        for r in files_sheet
        if (row := _child_row(r, FILES_EXPERIMENT_MAP, EXPERIMENT_COLUMNS, acc))
    ]

    # dedupe repeated individual/experiment rows on the (accession, study_accession) PK.
    individuals = list({(row[0], row[1]): row for row in individuals}.values())
    experiments = list({(row[0], row[1]): row for row in experiments}.values())
    return {
        "gsa_human_individuals": individuals,
        "gsa_human_samples": samples,
        "gsa_human_experiments": experiments,
        "gsa_human_runs": runs,
    }


def _study_row(record: dict) -> tuple:
    return (
        coerce(record.get("Study")),
        coerce(record.get("Title")),
        coerce(record.get("Organization")),
        coerce(record.get("DAC")),
        coerce(record.get("Access")),
        to_int(record.get("Requests")),
        to_int(record.get("Effective Requests")),
        to_int(record.get("Approved")),
        coerce(record.get("Project")),
        coerce(record.get("Strategy")),
        coerce(record.get("Study type")),
        coerce(record.get("Disease")),
        to_int(record.get("Individuals")),
        to_int(record.get("Samples")),
        to_int(record.get("Experiments")),
        coerce(record.get("Release time")),
        coerce(record.get("Last processed")),
    )


def _child_row(
    record: dict,
    header_map: dict[str, str],
    columns: list[str],
    study_acc: str,
    entity_type: str | None = None,
) -> tuple | None:
    static: dict[str, str] = {"study_accession": study_acc}
    if entity_type is not None:
        static["entity_type"] = entity_type
    row = map_row(record, header_map, columns, static)
    # first two columns are the composite primary key and must be present.
    if row[0] is None or row[1] is None:
        return None
    return row


def _log_status_counts(statuses: dict[str, str]) -> None:
    counts: dict[str, int] = {}
    for status in statuses.values():
        counts[status] = counts.get(status, 0) + 1
    logger.info("download status: %s", counts)
