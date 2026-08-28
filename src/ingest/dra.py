"""
DDBJ Sequence Read Archive (DRA) ingest.

DRA publishes the same INSDC XML as SRA, so parsing is entirely ingest.insdc_xml
(shared with src/ingest/sra.py) and dra_* is column-identical to sra_*. Four
divergences drove the code here:

- discovery: a livelist TSV, not SRA_Accessions.tab plus monthly tarballs;
- layout: metadata XML lives under dra/fastq/<bucket>/<submission>/, not under
  dra/ -- the top-level dra/DRA000/ tree is a legacy leftover that 404s for all
  but the oldest submissions;
- identifiers: <PRIMARY_ID label="BioProject ID">. DDBJ emits no <EXTERNAL_ID> at
  all, so reusing SRA's extractor would null out every bioproject
  (insdc_xml.ddbj_id_extractor);
- dates: the livelist has no published column, so published_at falls back to
  SUBMISSION@submission_date.

NCBI mirrors ~93% of DRA into SRA. All of public DRA is ingested anyway, with
is_only_ddbj marking the rest; --novelty-filter instead skips the mirrored ones.
Either way the check runs against Postgres given --pg-url and the local SQLite
dumps otherwise, and dra_ingest_provenance records which.
"""

import datetime
import logging
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import httpx
import lxml.etree as ET  # noqa: N812
import psycopg
from tqdm import tqdm

from helpers import download_file, exec_query_from_file, send_req, sqlite_connect
from ingest.insdc_xml import (
    FileType,
    convert_file_name_to_type,
    ddbj_id_extractor,
    process_experiments,
    process_runs,
    process_samples,
    process_studies,
    process_submission,
)

logger = logging.getLogger(__name__)

DRA_OUTPUT_DIR = "dra"
DDBJ_BASE_URL = "https://ddbj.nig.ac.jp/public/ddbj_database"
DRA_LIVELIST_URL = f"{DDBJ_BASE_URL}/dra/meta/list/livelist"
LIVELIST_FILE_NAME = "livelist"

_HTTP_NOT_FOUND = 404
_LIVELIST_MIN_COLS = 6
_ALIAS_COL = 6

# DRP accessions sort inside ['DRP', 'DRQ'), which lets the primary-key index
# do the work instead of scanning ~600k SRP rows with LIKE 'DRP%'.
_DRP_LO = "DRP"
_DRP_HI = "DRQ"

DRA_SQL_STATEMENTS = {
    FileType.Submission: (
        "INSERT OR REPLACE INTO dra_submissions "
        "(accession, alias, center_name, lab_name, broker_name, "
        "updated_at, published_at) "
        "VALUES (?,?,?,?,?,?,?)"
    ),
    FileType.Sample: (
        "INSERT OR REPLACE INTO dra_samples "
        "(accession, alias, description, title, scientific_name, taxon_id, "
        "external_id, attributes_json, submission, links) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)"
    ),
    # Upsert, not INSERT OR REPLACE: REPLACE is a delete+insert, which would null the
    # columns this statement does not name: pubmed_id_pysradb, pubmed_last_checked
    # come from backfill, not from the XML, and a re-ingest must not discard them.
    FileType.Study: (
        "INSERT INTO dra_studies "
        "(accession, alias, center_name, abstract, title, external_id, "
        "study_type, attributes_json, links, center_project_name, "
        "submission, published_at, updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(accession) DO UPDATE SET "
        "alias = excluded.alias, center_name = excluded.center_name, "
        "abstract = excluded.abstract, title = excluded.title, "
        "external_id = excluded.external_id, study_type = excluded.study_type, "
        "attributes_json = excluded.attributes_json, links = excluded.links, "
        "center_project_name = excluded.center_project_name, "
        "submission = excluded.submission, published_at = excluded.published_at, "
        "updated_at = excluded.updated_at"
    ),
    FileType.Experiment: (
        "INSERT OR REPLACE INTO dra_experiments "
        "(accession, design_description, library_layout, library_name, "
        "library_selection, library_source, library_strategy, samples, "
        "platform, instrument_model, study, title, submission) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
    ),
    FileType.Run: (
        "INSERT OR REPLACE INTO dra_runs "
        "(accession, experiment, submission) VALUES (?,?,?)"
    ),
}


@dataclass(frozen=True)
class LivelistStudy:
    accession: str
    submission: str
    updated: str
    center: str
    alias: str


def submission_dir_url(submission_accession: str) -> str:
    """DRA metadata XML lives under dra/fastq/<first 6 chars>/<accession>."""
    bucket = submission_accession[:6]
    return f"{DDBJ_BASE_URL}/dra/fastq/{bucket}/{submission_accession}"


def parse_livelist(path: Path) -> dict[str, LivelistStudy]:
    """Livelist TSV -> {DRP accession: LivelistStudy} for public STUDY rows."""
    studies: dict[str, LivelistStudy] = {}

    with path.open(encoding="utf-8", errors="replace") as f:
        header = f.readline()
        if not header.startswith("Accession"):
            msg = f"unexpected livelist header: {header[:80]!r}"
            raise ValueError(msg)

        for line in f:
            cols = line.rstrip("\n").split("\t")
            if len(cols) < _LIVELIST_MIN_COLS:
                continue

            accession, submission, visibility, updated, row_type, center = cols[:6]
            if row_type != "STUDY" or visibility != "public":
                continue

            studies[accession] = LivelistStudy(
                accession=accession,
                submission=submission,
                updated=updated,
                center=center,
                alias=cols[_ALIAS_COL] if len(cols) > _ALIAS_COL else "",
            )

    return studies


def in_date_window(
    value: str,
    start_date: datetime.date,
    end_date: datetime.date,
) -> bool:
    try:
        day = datetime.date.fromisoformat(value[:10])
    except ValueError:
        return False
    return start_date <= day <= end_date


def _have_from_sqlite(sra_db_file: Path | None, ena_db_file: Path | None) -> set[str]:
    have: set[str] = set()

    if sra_db_file and Path(sra_db_file).exists():
        with sqlite_connect(sra_db_file) as conn:
            rows = conn.execute(
                "SELECT accession FROM sra_studies "
                "WHERE accession >= ? AND accession < ?",
                (_DRP_LO, _DRP_HI),
            ).fetchall()
        have.update(acc for (acc,) in rows if acc)

    if ena_db_file and Path(ena_db_file).exists():
        with sqlite_connect(ena_db_file) as conn:
            rows = conn.execute(
                "SELECT study_accession, secondary_study_accession FROM ena_studies",
            ).fetchall()
        for study_acc, secondary in rows:
            _add_ena_accessions(have, study_acc, secondary)

    return have


def _add_ena_accessions(
    have: set[str],
    study_acc: str | None,
    secondary: str | None,
) -> None:
    """ENA joins co-registered secondary accessions with ';' ("DRP018273;SRP696703")."""
    if study_acc:
        have.add(study_acc)
    if secondary:
        have.update(part.strip() for part in str(secondary).split(";") if part.strip())


def _have_from_postgres(pg_url: str) -> set[str]:
    have: set[str] = set()

    with psycopg.connect(pg_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT accession FROM sra_studies "
            "WHERE accession >= %s AND accession < %s",
            (_DRP_LO, _DRP_HI),
        )
        have.update(acc for (acc,) in cur.fetchall() if acc)

        cur.execute(
            "SELECT study_accession, secondary_study_accession FROM ena_studies "
            "WHERE study_accession LIKE 'DRP%%' "
            "   OR secondary_study_accession LIKE '%%DRP%%'",
        )
        for study_acc, secondary in cur.fetchall():
            _add_ena_accessions(have, study_acc, secondary)

    return have


def existing_study_accessions(
    sra_db_file: Path | None,
    ena_db_file: Path | None,
    pg_url: str | None = None,
) -> set[str]:
    """
    DRA studies we already hold via SRA or ENA.

    Postgres is the live source of truth; the per-source SQLite dumps under
    DATA_DIR are the offline fallback. Returns both the DRP form and, from ENA,
    the PRJDB form -- a DDBJ study can be recorded either way.
    """
    if pg_url:
        have = _have_from_postgres(pg_url)
        logger.info("novelty filter: %d existing accessions from postgres", len(have))
    else:
        have = _have_from_sqlite(sra_db_file, ena_db_file)
        logger.info(
            "novelty filter: %d existing accessions from sqlite dumps",
            len(have),
        )

    return have


def _download_livelist(
    data_dir: Path,
    ttl: int,
    retries: int,
    timeout: int,
    max_timeout: int,
    chunk_size: int,
    num_workers: int,
) -> Path:
    path = data_dir / LIVELIST_FILE_NAME

    if path.exists() and (time.time() - path.stat().st_mtime) < ttl:
        logger.info("reusing cached livelist at %s (within ttl)", path)
        return path

    logger.info("downloading livelist from %s", DRA_LIVELIST_URL)
    download_file(
        DRA_LIVELIST_URL,
        path,
        chunk_size=chunk_size,
        num_workers=num_workers,
        num_retries=retries,
        timeout=timeout,
        max_wait=max_timeout,
        with_pbar=True,
        validate_content_length=True,
    )
    return path


def _fetch_submission_xml(
    submission: str,
    out_dir: Path,
    retries: int,
    timeout: int,
    max_timeout: int,
) -> None:
    base = submission_dir_url(submission)
    sub_dir = out_dir / submission
    sub_dir.mkdir(exist_ok=True, parents=True)

    for file_type in FileType:
        name = f"{submission}.{file_type.value}.xml"
        target = sub_dir / name

        if target.exists() and target.stat().st_size > 0:
            continue

        try:
            r = send_req(
                "GET",
                f"{base}/{name}",
                num_retries=retries,
                timeout=timeout,
                max_wait=max_timeout,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == _HTTP_NOT_FOUND:
                continue
            raise

        target.write_bytes(r.content)
        r.close()


def _submission_date(sub_dir: Path, submission: str) -> str | None:
    """DDBJ's livelist has no published column; the submission XML has a date."""
    path = sub_dir / f"{submission}.{FileType.Submission.value}.xml"
    if not path.exists():
        return None

    try:
        root = ET.parse(str(path)).getroot()
    except ET.ParseError:
        logger.warning("unparseable submission xml for %s", submission)
        return None

    value = root.get("submission_date")
    return value[:10] if value else None


def _load_submission(
    conn: sqlite3.Connection,
    sub_dir: Path,
    submission: str,
    livelist: dict[str, LivelistStudy],
) -> None:
    published = _submission_date(sub_dir, submission)

    def dates(accession: str) -> tuple[str, str] | None:
        entry = livelist.get(accession)
        updated = entry.updated[:10] if entry else published
        if not updated and not published:
            return None
        return (updated or published or "", published or updated or "")

    cursor = conn.cursor()
    for path in sorted(sub_dir.glob("*.xml")):
        file_type = convert_file_name_to_type(path.name)
        if file_type is None:
            continue

        with path.open("rb") as content:
            if file_type == FileType.Submission:
                rows = process_submission(submission, content, dates)
            elif file_type == FileType.Study:
                rows = process_studies(submission, content, dates, ddbj_id_extractor)
            elif file_type == FileType.Sample:
                rows = process_samples(submission, content, ddbj_id_extractor)
            elif file_type == FileType.Experiment:
                rows = process_experiments(submission, content)
            else:
                rows = process_runs(submission, content)

            cursor.executemany(DRA_SQL_STATEMENTS[file_type], list(rows))


def _mtime(path: Path | None) -> str | None:
    if path is None or not Path(path).exists():
        return None
    return datetime.datetime.fromtimestamp(
        Path(path).stat().st_mtime,
        tz=datetime.UTC,
    ).isoformat()


def _stamp_is_only_ddbj(
    db_file: Path,
    have: set[str],
    *,
    can_determine: bool,
) -> None:
    """
    Flag the studies SRA/ENA hold no mirror of.

    Left NULL when there was nothing to check against, so "undetermined" stays
    distinct from "mirrored". A temp table rather than IN (...): `have` runs to
    hundreds of thousands of accessions.
    """
    if not can_determine:
        logger.warning("is_only_ddbj left NULL: nothing to check against")
        return

    with sqlite_connect(db_file) as conn:
        conn.execute("UPDATE dra_studies SET is_only_ddbj = 1")
        conn.execute("CREATE TEMP TABLE mirrored (accession TEXT PRIMARY KEY)")
        conn.executemany(
            "INSERT OR IGNORE INTO mirrored VALUES (?)",
            [(acc,) for acc in have],
        )
        # Match on the BioProject as well as the DRP. ENA records some mirrors under
        # the project accession with no secondary study accession, so a DRP-only check
        # calls them DDBJ-exclusive when SRA/ENA plainly hold them.
        conn.execute(
            "UPDATE dra_studies SET is_only_ddbj = 0 "
            "WHERE accession IN (SELECT accession FROM mirrored) "
            "   OR (json_valid(external_id) "
            "       AND json_extract(external_id, '$.BioProject') "
            "           IN (SELECT accession FROM mirrored))",
        )
        conn.commit()

        exclusive, mirrored = conn.execute(
            """SELECT
                sum(is_only_ddbj = 1),
                sum(is_only_ddbj = 0)
            FROM dra_studies""",
        ).fetchone()

    logger.info(
        "is_only_ddbj: %s ddbj-exclusive, %s mirrored in sra/ena",
        exclusive or 0,
        mirrored or 0,
    )


def _record_provenance(
    db_file: Path,
    start_date: datetime.date,
    end_date: datetime.date,
    livelist_path: Path,
    source: str,
    sra_db_file: Path | None,
    ena_db_file: Path | None,
    novelty_filter: bool,
    n_candidates: int,
    n_kept: int,
) -> None:
    with sqlite_connect(db_file) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO dra_ingest_provenance VALUES "
            "(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                datetime.datetime.now(datetime.UTC).isoformat(),
                start_date.isoformat(),
                end_date.isoformat(),
                _mtime(livelist_path),
                source or None,
                _mtime(sra_db_file),
                str(ena_db_file) if ena_db_file else None,
                _mtime(ena_db_file),
                int(novelty_filter),
                n_candidates,
                n_kept,
                n_candidates - n_kept,
            ),
        )
        conn.commit()


def ingest_dra(
    base_output_dir: Path,
    db_file: Path | None,
    start_date: datetime.date,
    end_date: datetime.date,
    num_workers: int,
    retries: int,
    timeout: int,
    max_timeout: int,
    chunk_size: int,
    sra_db_file: Path | None,
    ena_db_file: Path | None,
    pg_url: str | None,
    novelty_filter: bool,
    livelist_ttl: int,
    dry_run: bool,
) -> None:
    output_dir = Path(base_output_dir) / DRA_OUTPUT_DIR
    data_dir = output_dir / "data"
    data_dir.mkdir(exist_ok=True, parents=True)

    date_str = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
    db_dir = output_dir / "db" / date_str
    db_dir.mkdir(exist_ok=True, parents=True)
    db_file = db_file or (db_dir / f"dra_metadata_{date_str}.db")

    livelist_path = _download_livelist(
        data_dir,
        livelist_ttl,
        retries,
        timeout,
        max_timeout,
        chunk_size,
        num_workers,
    )
    livelist = parse_livelist(livelist_path)
    logger.info("livelist: %d public studies", len(livelist))

    candidates = {
        acc: entry
        for acc, entry in livelist.items()
        if in_date_window(entry.updated, start_date, end_date)
    }
    n_candidates = len(candidates)
    logger.info(
        "%d studies updated in [%s, %s]",
        n_candidates,
        start_date,
        end_date,
    )

    have: set[str] = set()
    source = ""
    if pg_url or sra_db_file or ena_db_file:
        source = pg_url.split("@")[-1] if pg_url else str(sra_db_file)
        have = existing_study_accessions(sra_db_file, ena_db_file, pg_url)
    elif novelty_filter:
        msg = "novelty filter needs --pg-url or --sra-db-file/--ena-db-file"
        raise ValueError(msg)
    else:
        logger.warning(
            "no --sra-db-file/--ena-db-file/--pg-url given: is_only_ddbj will be "
            "NULL for every study (undetermined, not 'mirrored')",
        )

    n_already_held = sum(1 for acc in candidates if acc in have)
    if novelty_filter:
        candidates = {a: e for a, e in candidates.items() if a not in have}

    submissions = sorted({e.submission for e in candidates.values()})

    logger.info(
        "\n"
        "  DRA ingest plan\n"
        "  ---------------\n"
        "  novelty filter      : %s\n"
        "  checked against     : %s\n"
        "  date window         : %s .. %s\n"
        "  studies in window   : %d\n"
        "  mirrored in sra/ena : %d   <- is_only_ddbj = 0\n"
        "  ddbj-exclusive      : %d   <- is_only_ddbj = 1\n"
        "  studies to ingest   : %d\n"
        "  submissions to fetch: %d",
        "on (skipping the mirrored ones)"
        if novelty_filter
        else "OFF (ingesting all of public DRA)",
        source or "n/a -- is_only_ddbj will be NULL",
        start_date,
        end_date,
        n_candidates,
        n_already_held,
        n_candidates - n_already_held,
        len(candidates),
        len(submissions),
    )

    if dry_run:
        logger.info("--dry-run: stopping before any download")
        return

    if not candidates:
        logger.warning("no studies to ingest")
        return

    xml_dir = data_dir / "submissions"
    xml_dir.mkdir(exist_ok=True, parents=True)

    with (
        ThreadPoolExecutor(num_workers) as pool,
        tqdm(total=len(submissions), unit="sub", desc="downloading dra xml") as pbar,
    ):
        futures = [
            pool.submit(
                _fetch_submission_xml,
                sub,
                xml_dir,
                retries,
                timeout,
                max_timeout,
            )
            for sub in submissions
        ]
        for f in as_completed(futures):
            f.result()
            pbar.update(1)

    exec_query_from_file("tables/dra.sql", db_file)

    with sqlite_connect(db_file) as conn:
        for sub in tqdm(submissions, unit="sub", desc="loading dra xml"):
            sub_dir = xml_dir / sub
            if sub_dir.exists():
                _load_submission(conn, sub_dir, sub, livelist)
        conn.commit()

    _stamp_is_only_ddbj(db_file, have, can_determine=bool(source))

    exec_query_from_file("indexes/dra.sql", db_file)

    _record_provenance(
        db_file,
        start_date,
        end_date,
        livelist_path,
        source,
        sra_db_file,
        ena_db_file,
        novelty_filter,
        n_candidates,
        len(candidates),
    )

    logger.info("dra ingest complete: %s", db_file)
