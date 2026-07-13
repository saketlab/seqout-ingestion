"""
Transfer GSA (CRA) and GSA-Human (HRA) SQLite into the shared gsa_* Postgres tables.

Both sources are merged under 'gsa'. Postgres tables are created by setup_gsa.sh.
"""

from __future__ import annotations

import datetime
import logging
import re
from itertools import groupby
from typing import TYPE_CHECKING, Any, LiteralString, cast

import psycopg
from tqdm import tqdm

from helpers import sqlite_connect
from ingest.gsa_common import GSA_DOWNLOAD_URL

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

logger = logging.getLogger(__name__)

GSA_COUNTRY = "China"
GSA_HUMAN_ORGANISM = "Homo sapiens"

# Column lists must match the setup_gsa.sh DDL.
# GSA names most studies after their own accession ("CRA_10", "HRA_123") rather than
# after the science; the submitter's real title is on the parent BioProject.
_PLACEHOLDER_TITLE_RE = re.compile(r"(?:CRA|HRA)_\d+")

_GSA_STUDIES_COLS = [
    "study_accession",
    "study_title",
    "study_description",
    "study_name",
    "project_name",
    "center_name",
    "scientific_name",
    "first_public",
    "last_updated",
    "pubmed_id_pysradb",
    "pubmed_last_checked",
]
_GSA_STUDIES_SEARCH_COLS = [
    "accession",
    "study_title",
    "study_description",
    "study_name",
    "center_name",
    "project_name",
    "last_updated",
    "first_public",
]
_GSA_EXPERIMENTS_COLS = [
    "run_accession",
    "experiment_accession",
    "sample_accession",
    "study_accession",
    "experiment_title",
    "library_strategy",
    "library_source",
    "library_selection",
    "library_layout",
    "library_name",
    "instrument_platform",
    "instrument_model",
    "scientific_name",
    "country",
    "first_public",
    "base_count",
    "read_count",
    "run_alias",
]
_GSA_SAMPLES_COLS = [
    "sample_accession",
    "study_accession",
    "scientific_name",
    "sample_title",
    "sample_description",
    "sample_alias",
    "tissue",
    "disease",
    "cell_type",
    "sex",
    "biomaterial_provider",
    "collection_date",
]
_GSA_RUN_DOWNLOAD_COLS = [
    "run_accession",
    "study_accession",
    "experiment_accession",
    "library_layout",
    "fastq_ftp",
    "fastq_bytes",
    "fastq_md5",
]
_GSA_PROJECTS_COLS = [
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
_BYTES_IN_NAME_RE = re.compile(r"\((\d+)\s*bytes\)")


def gsa_transfer_tasks(
    pg_url: str,
    gsa_db_file: Path | None,
    gsa_human_db_file: Path | None,
    batch_size: int,
    gsa_bioproject_db_file: Path | None = None,
) -> list[tuple[str, Callable[..., None], tuple]]:
    """Build the GSA transfer tasks, or an empty list if no DB file exists."""
    cra = gsa_db_file if gsa_db_file and gsa_db_file.exists() else None
    hra = (
        gsa_human_db_file if gsa_human_db_file and gsa_human_db_file.exists() else None
    )
    bp = (
        gsa_bioproject_db_file
        if gsa_bioproject_db_file and gsa_bioproject_db_file.exists()
        else None
    )
    if not cra and not hra and not bp:
        return []

    common = (pg_url, cra, hra, batch_size)
    titled = (pg_url, cra, hra, batch_size, bp)
    tasks: list[tuple[str, Callable[..., None], tuple]] = []
    if cra or hra:
        tasks += [
            ("gsa_studies", _transfer_gsa_studies, titled),
            ("gsa_studies_search", _transfer_gsa_studies_search, titled),
            ("gsa_experiments", _transfer_gsa_experiments, common),
            ("gsa_samples", _transfer_gsa_samples, common),
            ("gsa_run_downloads", _transfer_gsa_run_downloads, common),
        ]
    if bp:
        tasks.append(
            ("gsa_projects", _transfer_gsa_projects, (pg_url, bp, batch_size)),
        )
    return tasks


def _transfer_gsa_projects(
    pg_url: str,
    bioproject_db: Path,
    batch_size: int,
) -> None:
    """Upsert scraped PRJCA project metadata; JSON text cols land in JSONB."""

    def rows() -> Iterator[tuple]:
        cols = ", ".join(_GSA_PROJECTS_COLS)
        with sqlite_connect(bioproject_db) as c:
            for r in c.execute(f"SELECT {cols} FROM gsa_projects"):  # noqa: S608
                yield tuple(r)

    _upsert(
        pg_url,
        "gsa_projects",
        _GSA_PROJECTS_COLS,
        rows(),
        "accession",
        batch_size,
        "gsa_projects",
    )


def _upsert(
    pg_url: str,
    table: str,
    cols: list[str],
    rows: Iterator[tuple],
    conflict_col: str,
    batch_size: int,
    label: str,
) -> None:
    """Batch upsert rows into a pre-existing Postgres table."""
    col_sql = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != conflict_col)
    query = (
        f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders}) "  # noqa: S608
        f"ON CONFLICT ({conflict_col}) DO UPDATE SET {updates}"
    )
    with psycopg.connect(pg_url) as conn:
        batch: list[tuple] = []
        with tqdm(desc=f"copying {label}", unit="rows", leave=False) as pbar:
            for row in rows:
                batch.append(row)
                if len(batch) >= batch_size:
                    _flush(conn, query, batch)
                    pbar.update(len(batch))
                    batch = []
            if batch:
                _flush(conn, query, batch)
                pbar.update(len(batch))


def _flush(conn: psycopg.Connection, query: str, batch: list[tuple]) -> None:
    with conn.cursor() as cur:
        cur.executemany(cast("LiteralString", query), batch)
    conn.commit()


def _iso_date(value: Any) -> str | None:
    """Coerce a date string to ISO YYYY-MM-DD, else None."""
    if not value:
        return None
    text = str(value).strip()[:10]
    try:
        return datetime.date.fromisoformat(text).isoformat()
    except ValueError:
        return None


def _project_titles(bp: Path | None) -> dict[str, str]:
    """PRJCA -> project title, used to replace GSA's placeholder study titles."""
    if not bp:
        return {}
    with sqlite_connect(bp) as c:
        return dict(
            c.execute(
                "SELECT accession, title FROM gsa_projects WHERE title IS NOT NULL",
            ),
        )


def _study_title(
    title: str | None,
    project: str | None,
    project_titles: dict[str, str],
) -> str | None:
    """
    Resolve a study's display title.

    GSA titles most CRA/HRA studies after their own accession ("CRA_10"), which
    carries no information. The submitter's real title lives on the parent BioProject,
    so prefer that whenever the study title is one of these placeholders.
    """
    if title and not _PLACEHOLDER_TITLE_RE.fullmatch(title):
        return title
    return project_titles.get(project or "") or title


def _transfer_gsa_studies(
    pg_url: str,
    cra: Path | None,
    hra: Path | None,
    batch_size: int,
    bp: Path | None = None,
) -> None:
    project_titles = _project_titles(bp)

    def rows() -> Iterator[tuple]:
        if cra:
            with sqlite_connect(cra) as c:
                for r in c.execute(
                    """SELECT accession, title, alias, organization, project_accession,
                        organism, release_time, pubmed_id_pysradb, pubmed_last_checked
                    FROM gsa_studies""",
                ):
                    (acc, title, alias, org, proj, organism, rel, pmid, pmck) = r
                    fp = _iso_date(rel)
                    yield (
                        acc,
                        _study_title(title, proj, project_titles),
                        None,
                        alias,
                        proj,
                        org,
                        organism,
                        fp,
                        fp,
                        pmid,
                        _iso_date(pmck),
                    )
        if hra:
            with sqlite_connect(hra) as c:
                for r in c.execute(
                    """SELECT accession, title, disease, organization,
                        project_accession, organism, release_time,
                        pubmed_id_pysradb, pubmed_last_checked
                    FROM gsa_human_studies""",
                ):
                    (acc, title, disease, org, proj, organism, rel, pmid, pmck) = r
                    fp = _iso_date(rel)
                    yield (
                        acc,
                        _study_title(title, proj, project_titles),
                        disease,
                        None,
                        proj,
                        org,
                        organism or GSA_HUMAN_ORGANISM,
                        fp,
                        fp,
                        pmid,
                        _iso_date(pmck),
                    )

    _upsert(
        pg_url,
        "gsa_studies",
        _GSA_STUDIES_COLS,
        rows(),
        "study_accession",
        batch_size,
        "gsa_studies",
    )


def _transfer_gsa_studies_search(
    pg_url: str,
    cra: Path | None,
    hra: Path | None,
    batch_size: int,
    bp: Path | None = None,
) -> None:
    project_titles = _project_titles(bp)

    def rows() -> Iterator[tuple]:
        if cra:
            with sqlite_connect(cra) as c:
                for acc, title, alias, org, proj, rel in c.execute(
                    """SELECT accession, title, alias, organization,
                        project_accession, release_time FROM gsa_studies""",
                ):
                    fp = _iso_date(rel)
                    t = _study_title(title, proj, project_titles)
                    yield (acc, t, None, alias, org, proj, fp, fp)
        if hra:
            with sqlite_connect(hra) as c:
                for acc, title, disease, org, proj, rel in c.execute(
                    # Controlled studies have no fetchable detail; exclude from search.
                    """SELECT accession, title, disease, organization,
                        project_accession, release_time FROM gsa_human_studies
                        WHERE access = 'Open'""",
                ):
                    fp = _iso_date(rel)
                    t = _study_title(title, proj, project_titles)
                    yield (acc, t, disease, None, org, proj, fp, fp)

    _upsert(
        pg_url,
        "gsa_studies_search",
        _GSA_STUDIES_SEARCH_COLS,
        rows(),
        "accession",
        batch_size,
        "gsa_studies_search",
    )


def _transfer_gsa_experiments(
    pg_url: str,
    cra: Path | None,
    hra: Path | None,
    batch_size: int,
) -> None:
    # one row per run, joined to its experiment and study.
    cra_sql = """
        SELECT r.accession, r.experiment_accession, e.sample_accession,
            r.study_accession, e.title, e.library_strategy, e.library_source,
            e.library_selection, e.library_layout, e.library_name, e.platform,
            st.organism, st.release_time
        FROM gsa_runs r
        LEFT JOIN gsa_experiments e
            ON e.accession = r.experiment_accession
            AND e.study_accession = r.study_accession
        LEFT JOIN gsa_studies st ON st.accession = r.study_accession"""
    hra_sql = """
        SELECT r.accession, r.experiment_accession,
            COALESCE(e.sample_accession, r.sample_accession), r.study_accession,
            e.title, e.library_strategy, e.library_source, e.library_selection,
            e.library_layout, e.library_name, COALESCE(e.platform, r.platform),
            st.organism, st.release_time
        FROM gsa_human_runs r
        LEFT JOIN gsa_human_experiments e
            ON e.accession = r.experiment_accession
            AND e.study_accession = r.study_accession
        LEFT JOIN gsa_human_studies st ON st.accession = r.study_accession"""

    def _emit(conn: Any, sql: str, default_org: str | None) -> Iterator[tuple]:
        for r in conn.execute(sql):
            (
                run,
                exp,
                samp,
                study,
                title,
                strat,
                src,
                sel,
                layout,
                libname,
                platform,
                organism,
                rel,
            ) = r
            yield (
                run,
                exp,
                samp,
                study,
                title,
                strat,
                src,
                sel,
                layout,
                libname,
                platform,
                None,
                organism or default_org,
                GSA_COUNTRY,
                _iso_date(rel),
                None,
                None,
                None,
            )

    def rows() -> Iterator[tuple]:
        if cra:
            with sqlite_connect(cra) as c:
                yield from _emit(c, cra_sql, None)
        if hra:
            with sqlite_connect(hra) as c:
                yield from _emit(c, hra_sql, GSA_HUMAN_ORGANISM)

    _upsert(
        pg_url,
        "gsa_experiments",
        _GSA_EXPERIMENTS_COLS,
        rows(),
        "run_accession",
        batch_size,
        "gsa_experiments",
    )


def _transfer_gsa_samples(
    pg_url: str,
    cra: Path | None,
    hra: Path | None,
    batch_size: int,
) -> None:
    def rows() -> Iterator[tuple]:
        if cra:
            with sqlite_connect(cra) as c:
                for r in c.execute(
                    """SELECT accession, study_accession, organism, sample_title,
                        public_description, sample_name, tissue, disease, cell_type,
                        sex, biomaterial_provider, collection_date FROM gsa_samples""",
                ):
                    yield tuple(r)
        if hra:
            with sqlite_connect(hra) as c:
                for r in c.execute(
                    """SELECT accession, study_accession, organism, sample_title,
                        public_description, sample_name, tissue, NULL, NULL, NULL,
                        biomaterial_provider, collection_date FROM gsa_human_samples""",
                ):
                    (acc, study, organism, *rest) = r
                    yield (acc, study, organism or GSA_HUMAN_ORGANISM, *rest)

    _upsert(
        pg_url,
        "gsa_samples",
        _GSA_SAMPLES_COLS,
        rows(),
        "sample_accession",
        batch_size,
        "gsa_samples",
    )


def _bytes_from_name(name: str | None) -> str:
    """CRA file names embed the size as 'CRD000008 (7374228529 bytes)'."""
    match = _BYTES_IN_NAME_RE.search(name or "")
    return match.group(1) if match else ""


def _join_or_none(parts: tuple[str | None, ...]) -> str | None:
    """Semicolon-join per-file values, or None if every value is empty."""
    joined = ";".join(p or "" for p in parts)
    return joined if joined.strip(";") else None


_LEGACY_HOST_RE = re.compile(r"^ftp://download\.big\.ac\.cn/")


def _https_url(url: str) -> str:
    """
    Rewrite the legacy ftp:// host CNCB still emits in CRA workbooks and HRA manifests.

    Browsers dropped ftp:// support, and big.ac.cn only 301s to download.cncb.ac.cn
    (its cert covers *.cncb.ac.cn alone), so point there directly.
    """
    return _LEGACY_HOST_RE.sub(f"{GSA_DOWNLOAD_URL}/", url)


def _download_row(
    run: str,
    study: str,
    exp: str | None,
    layout: str | None,
    files: list[tuple[str, str | None, str]],
) -> tuple | None:
    """Join per-file (url, md5, bytes) into semicolon-delimited fastq_* fields."""
    kept = [(url, md5, byts) for url, md5, byts in files if url]
    if not kept:
        return None
    urls, md5s, byts = zip(*kept, strict=True)
    fastq_ftp = ";".join(_https_url(u) for u in urls)
    fastq_bytes, fastq_md5 = _join_or_none(byts), _join_or_none(md5s)
    return (run, study, exp, layout, fastq_ftp, fastq_bytes, fastq_md5)


def _transfer_gsa_run_downloads(
    pg_url: str,
    cra: Path | None,
    hra: Path | None,
    batch_size: int,
) -> None:
    # CRA file*_url are clean (bytes embedded in file*_name); HRA workbook urls are
    # unreliable, so HRA downloads come from gsa_human_run_files (manifest-built).
    cra_sql = """
        SELECT r.accession, r.study_accession, r.experiment_accession,
            r.file1_name, r.file1_md5, r.file1_url,
            r.file2_name, r.file2_md5, r.file2_url, e.library_layout
        FROM gsa_runs r
        LEFT JOIN gsa_experiments e
            ON e.accession = r.experiment_accession
            AND e.study_accession = r.study_accession"""
    hra_sql = """
        SELECT f.run_accession, r.study_accession, r.experiment_accession,
            e.library_layout, f.file_url, f.file_md5
        FROM gsa_human_run_files f
        JOIN gsa_human_runs r
            ON r.accession = f.run_accession
            AND r.study_accession = f.study_accession
        LEFT JOIN gsa_human_experiments e
            ON e.accession = r.experiment_accession
            AND e.study_accession = r.study_accession
        ORDER BY f.run_accession, f.file_ord"""

    def cra_rows(conn: Any) -> Iterator[tuple]:
        for run, study, exp, n1, m1, u1, n2, m2, u2, layout in conn.execute(cra_sql):
            files = [(u1, m1, _bytes_from_name(n1)), (u2, m2, _bytes_from_name(n2))]
            row = _download_row(run, study, exp, layout, files)
            if row:
                yield row

    def hra_rows(conn: Any) -> Iterator[tuple]:
        for run, group in groupby(conn.execute(hra_sql), key=lambda row: row[0]):
            members = list(group)
            _, study, exp, layout, *_ = members[0]
            files = [(url, md5, "") for _, _, _, _, url, md5 in members]
            if row := _download_row(run, study, exp, layout, files):
                yield row

    def rows() -> Iterator[tuple]:
        if cra:
            with sqlite_connect(cra) as c:
                yield from cra_rows(c)
        if hra:
            with sqlite_connect(hra) as c:
                yield from hra_rows(c)

    _upsert(
        pg_url,
        "sra_run_download_links",
        _GSA_RUN_DOWNLOAD_COLS,
        rows(),
        "run_accession",
        batch_size,
        "gsa_run_downloads",
    )
