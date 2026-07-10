"""
Transfer GSA (CRA) and GSA-Human (HRA) SQLite into the shared gsa_* Postgres tables.

Both sources are merged under 'gsa'. Postgres tables are created by setup_gsa.sh.
"""

from __future__ import annotations

import datetime
import logging
from typing import TYPE_CHECKING, Any, LiteralString, cast

import psycopg
from tqdm import tqdm

from helpers import sqlite_connect

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

logger = logging.getLogger(__name__)

GSA_COUNTRY = "China"
GSA_HUMAN_ORGANISM = "Homo sapiens"

# Column lists must match the setup_gsa.sh DDL.
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


def gsa_transfer_tasks(
    pg_url: str,
    gsa_db_file: Path | None,
    gsa_human_db_file: Path | None,
    batch_size: int,
) -> list[tuple[str, Callable[..., None], tuple]]:
    """Build the GSA transfer tasks, or an empty list if neither DB file exists."""
    cra = gsa_db_file if gsa_db_file and gsa_db_file.exists() else None
    hra = (
        gsa_human_db_file if gsa_human_db_file and gsa_human_db_file.exists() else None
    )
    if not cra and not hra:
        return []

    common = (pg_url, cra, hra, batch_size)
    return [
        ("gsa_studies", _transfer_gsa_studies, common),
        ("gsa_studies_search", _transfer_gsa_studies_search, common),
        ("gsa_experiments", _transfer_gsa_experiments, common),
        ("gsa_samples", _transfer_gsa_samples, common),
    ]


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


def _transfer_gsa_studies(
    pg_url: str,
    cra: Path | None,
    hra: Path | None,
    batch_size: int,
) -> None:
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
                        title,
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
                        title,
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
) -> None:
    def rows() -> Iterator[tuple]:
        if cra:
            with sqlite_connect(cra) as c:
                for acc, title, alias, org, proj, rel in c.execute(
                    """SELECT accession, title, alias, organization,
                        project_accession, release_time FROM gsa_studies""",
                ):
                    fp = _iso_date(rel)
                    yield (acc, title, None, alias, org, proj, fp, fp)
        if hra:
            with sqlite_connect(hra) as c:
                for acc, title, disease, org, proj, rel in c.execute(
                    """SELECT accession, title, disease, organization,
                        project_accession, release_time FROM gsa_human_studies""",
                ):
                    fp = _iso_date(rel)
                    yield (acc, title, disease, None, org, proj, fp, fp)

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
