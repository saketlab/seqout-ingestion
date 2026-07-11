"""
Ingest CNCB-NGDC BioProject (PRJCA) project-level metadata into gsa_projects.
"""

from __future__ import annotations

import datetime
import functools
import json
import logging
import re
from typing import TYPE_CHECKING

import httpx
import lxml.html
from tqdm import tqdm

from helpers import exec_query_from_file, send_req, sqlite_connect
from ingest.gsa_common import (
    GSA_BASE_URL,
    apply_write_pragmas,
    bulk_upsert,
    download_studies,
    make_client,
)

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

BIOPROJECT_OUTPUT_DIR = "gsa_bioproject"
BROWSE_URL = f"{GSA_BASE_URL}/bioproject/browse/{{acc}}"
_ENGLISH_HEADERS = {"Accept-Language": "en-US,en;q=0.9"}
FETCH_LOG_COLUMNS = ["accession", "status", "fetched_at"]

# SQLite column order for gsa_projects (JSON columns serialized on write).
PROJECT_COLUMNS = [
    "accession",
    "title",
    "summary",
    "relevance",
    "data_type",
    "organism",
    "sample_scope",
    "pubmed_id",
    "publications",
    "funding",
    "submitter",
    "submitter_email",
    "submitting_org",
    "published_at",
    "submitted_at",
]

_PUB_HEADERS = ["PubMed ID", "Article title", "Journal name", "DOI", "Year"]
_GRANT_HEADERS = ["Agency", "Grant ID", "Grant title"]
_EMAIL_RE = re.compile(r"\(([^)]+@[^)]+)\)")


def _clean(text: str | None) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _scalar_map(doc: lxml.html.HtmlElement) -> dict[str, str]:
    """Map each <th> label to its sibling <td> value."""
    out: dict[str, str] = {}
    for th in doc.xpath("//th"):
        td = th.getnext()
        if td is not None and td.tag == "td":
            out[_clean(th.text_content())] = _clean(td.text_content())
    return out


def _table_rows(
    doc: lxml.html.HtmlElement,
    required: list[str],
) -> tuple[list[list[str]], dict[str, int]]:
    """
    Return (data rows, header->index) for the table matching `required` labels.

    The header row is the first `<tr>` containing `<th>` cells. Data rows whose
    cell count differs from the header are skipped (guards column drift / label rows).
    """
    for table in doc.xpath("//table"):
        header: list[str] | None = None
        for tr in table.xpath(".//tr"):
            ths = tr.xpath("./th")
            if ths:
                header = [_clean(t.text_content()) for t in ths]
                break
        if not header or not all(col in header for col in required):
            continue
        idx = {label: i for i, label in enumerate(header)}
        rows: list[list[str]] = []
        for tr in table.xpath(".//tr"):
            tds = tr.xpath("./td")
            if not tds:
                continue
            if len(tds) != len(header):
                continue
            rows.append([_clean(td.text_content()) for td in tds])
        return rows, idx
    return [], {}


def parse_bioproject(html: str, accession: str) -> dict | None:
    """
    Parse an English BioProject detail page into a gsa_projects row dict.

    Returns None if the page is not the English variant (no 'Accession' label),
    so the caller can record status 'no-english' rather than store garbage.
    """
    doc = lxml.html.fromstring(html)
    scal = _scalar_map(doc)
    if "Accession" not in scal:
        return None

    pub_rows, pidx = _table_rows(doc, _PUB_HEADERS)
    publications = [
        {
            "pmid": r[pidx["PubMed ID"]],
            "title": r[pidx["Article title"]],
            "journal": r[pidx["Journal name"]],
            "doi": r[pidx["DOI"]],
            "year": r[pidx["Year"]],
        }
        for r in pub_rows
        if r[pidx["PubMed ID"]].isdigit()
    ]

    grant_rows, gidx = _table_rows(doc, _GRANT_HEADERS)
    funding = [
        {
            "agency": r[gidx["Agency"]],
            "program": r[gidx["program"]] if "program" in gidx else "",
            "grant_id": r[gidx["Grant ID"]],
            "grant_title": r[gidx["Grant title"]],
        }
        for r in grant_rows
        if any(r)
    ]

    submitter_raw = scal.get("Submitter", "")
    email_match = _EMAIL_RE.search(submitter_raw)

    return {
        "accession": accession,
        "title": scal.get("Title") or None,
        "summary": scal.get("Description") or None,
        "relevance": scal.get("Relevance") or None,
        "data_type": scal.get("Data types") or None,
        "organism": scal.get("Organisms") or None,
        "sample_scope": scal.get("Sample scope") or None,
        "pubmed_id": [p["pmid"] for p in publications],
        "publications": publications,
        "funding": funding,
        "submitter": _clean(re.sub(r"\([^)]*\)", "", submitter_raw)) or None,
        "submitter_email": email_match.group(1) if email_match else None,
        "submitting_org": scal.get("Organization") or None,
        "published_at": scal.get("Release date") or None,
        "submitted_at": scal.get("Submission date") or None,
    }


def _row_tuple(parsed: dict) -> tuple:
    """Serialize a parsed dict to a gsa_projects insert tuple (JSON columns dumped)."""
    json_cols = {"pubmed_id", "publications", "funding"}
    return tuple(
        json.dumps(parsed[c]) if c in json_cols else parsed[c] for c in PROJECT_COLUMNS
    )


def _fetch_bioproject(
    accession: str,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> bytes | None:
    """GET the English detail page; 404 -> None (unknown/removed accession)."""
    with make_client(timeout) as client:
        client.headers.update(_ENGLISH_HEADERS)
        try:
            r = send_req(
                "GET",
                BROWSE_URL.format(acc=accession),
                num_retries=retries,
                timeout=timeout,
                max_wait=max_timeout,
                client=client,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == httpx.codes.NOT_FOUND:
                return None
            raise
        return r.content


def _enumerate_accessions(
    gsa_db_file: Path | None,
    gsa_human_db_file: Path | None,
) -> list[str]:
    """Distinct PRJCA from both GSA source DBs' project_accession columns."""
    accs: set[str] = set()
    sources = [
        (gsa_db_file, "gsa_studies"),
        (gsa_human_db_file, "gsa_human_studies"),
    ]
    for db_file, table in sources:
        if not db_file or not db_file.exists():
            continue
        with sqlite_connect(db_file) as conn:
            for (acc,) in conn.execute(
                f"SELECT DISTINCT project_accession FROM {table} "  # noqa: S608
                "WHERE project_accession LIKE 'PRJCA%'",
            ):
                if acc:
                    accs.add(acc)
    return sorted(accs)


def _ingest(
    db_file: Path,
    pages_dir: Path,
    accessions: list[str],
    date_str: str,
) -> None:
    with sqlite_connect(db_file) as conn:
        apply_write_pragmas(conn)
        project_rows: list[tuple] = []
        log_rows: list[tuple] = []
        for acc in tqdm(accessions, unit="project", desc="loading bioproject pages"):
            page = pages_dir / f"{acc}.html"
            if not page.exists():
                log_rows.append((acc, "empty", date_str))
                continue
            try:
                parsed = parse_bioproject(page.read_text(), acc)
            except Exception:
                logger.exception("failed to parse %s", acc)
                log_rows.append((acc, "failed", date_str))
                continue
            if parsed is None:
                log_rows.append((acc, "no-english", date_str))
                continue
            project_rows.append(_row_tuple(parsed))
            log_rows.append((acc, "ok", date_str))
        bulk_upsert(conn, "gsa_projects", PROJECT_COLUMNS, project_rows)
        bulk_upsert(conn, "gsa_bioproject_fetch_log", FETCH_LOG_COLUMNS, log_rows)
        conn.commit()


def ingest_gsa_bioproject(
    base_output_dir: Path,
    gsa_db_file: Path | None,
    gsa_human_db_file: Path | None,
    db_file: Path | None,
    num_workers: int,
    retries: int,
    timeout: int,
    max_timeout: int,
    limit: int | None = None,
) -> None:
    output_dir = base_output_dir / BIOPROJECT_OUTPUT_DIR
    pages_dir = output_dir / "data" / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)

    date_str = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
    db_dir = output_dir / "db" / date_str
    db_dir.mkdir(parents=True, exist_ok=True)
    db_file = db_file or (db_dir / f"gsa_bioproject_metadata_{date_str}.db")

    accessions = _enumerate_accessions(gsa_db_file, gsa_human_db_file)
    if limit:
        accessions = accessions[:limit]
    logger.info("enumerated %d PRJCA bioprojects to ingest", len(accessions))

    fetch = functools.partial(
        _fetch_bioproject, retries=retries, timeout=timeout, max_timeout=max_timeout
    )
    download_studies(
        accessions,
        pages_dir,
        fetch,
        num_workers,
        desc="downloading bioproject pages",
        suffix=".html",
    )

    exec_query_from_file("tables/gsa_bioproject.sql", db_file)
    _ingest(db_file, pages_dir, accessions, date_str)
    exec_query_from_file("indexes/gsa_bioproject.sql", db_file)
    logger.info("gsa-bioproject ingest complete: %s", db_file)
