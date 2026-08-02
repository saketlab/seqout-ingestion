"""
Transfer DDBJ DRA SQLite into the dra_* Postgres tables.

DRA and SRA share the INSDC XML schema, so the dra_* tables mirror sra_* column for
column (see sql/tables/dra_pg.sql). The one addition is is_only_ddbj, which marks the
studies SRA/ENA hold no mirror of.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, LiteralString, cast

import psycopg
from tqdm import tqdm

from helpers import exec_query_from_file, sqlite_connect
from transfer.pg_fields import non_nulling_update

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

logger = logging.getLogger(__name__)

# Column lists must match sql/tables/dra_pg.sql.
_SUBMISSIONS_COLS = [
    "accession",
    "alias",
    "broker_name",
    "center_name",
    "lab_name",
    "published_at",
    "updated_at",
]
_STUDIES_COLS = [
    "accession",
    "alias",
    "center_name",
    "abstract",
    "title",
    "external_id",
    "study_type",
    "attributes_json",
    "links",
    "center_project_name",
    "bioproject_id",
    "submission",
    "published_at",
    "updated_at",
    "pubmed_id_pysradb",
    "pubmed_last_checked",
    "is_only_ddbj",
]
_EXPERIMENTS_COLS = [
    "accession",
    "design_description",
    "library_layout",
    "library_name",
    "library_selection",
    "library_source",
    "library_strategy",
    "samples",
    "platform",
    "instrument_model",
    "study",
    "title",
    "submission",
]
_SAMPLES_COLS = [
    "accession",
    "alias",
    "description",
    "title",
    "scientific_name",
    "taxon_id",
    "external_id",
    "attributes_json",
    "submission",
    "links",
]
_RUNS_COLS = ["accession", "experiment", "submission"]
_SEARCH_COLS = [
    "accession",
    "study_title",
    "study_description",
    "study_name",
    "center_name",
    "project_name",
    "last_updated",
    "first_public",
]

# Column lists must match sql/tables/gea_pg.sql.
_GEA_EXPERIMENTS_COLS = [
    "accession",
    "title",
    "organism",
    "description",
    "study_type",
    "bioproject",
    "release_date",
    "submission_date",
    "updated_at",
    "experimental_factors",
    "experimental_factor_types",
    "contact_name",
    "contact_email",
    "contact_affiliation",
    "pubmed_id",
    "pubmed_id_pysradb",
    "pubmed_last_checked",
    "protocol_names",
    "protocol_types",
    "protocols_description",
    "attributes_json",
    "data_files",
]
_GEA_SAMPLES_COLS = [
    "id",
    "experiment_accession",
    "source_name",
    "organism",
    "taxon_id",
    "organism_part",
    "tissue",
    "cell_type",
    "genotype",
    "strain",
    "sex",
    "age",
    "biosample",
    "sample_title",
    "sample_description",
    "library_strategy",
    "library_source",
    "library_selection",
    "library_layout",
    "instrument_model",
    "dra_experiment",
    "dra_run",
    "attributes_json",
]
_GEA_ARRAY_COLS = frozenset(
    {
        "experimental_factors",
        "experimental_factor_types",
        "protocol_names",
        "protocol_types",
    },
)
_GEA_DATE_COLS = frozenset(
    {"release_date", "submission_date", "updated_at", "pubmed_last_checked"},
)


def bioproject_of(external_id: str | None) -> str | None:
    if not external_id:
        return None
    try:
        value = json.loads(external_id).get("BioProject")
    except (json.JSONDecodeError, TypeError, AttributeError):
        return None
    return value or None


def string_list(value: str | None) -> list[str] | None:
    if not value:
        return None
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return None
    return [str(v) for v in parsed] if isinstance(parsed, list) else None


def _iso_date(value: Any) -> str | None:
    if not value:
        return None
    text = str(value).strip()[:10]
    return text if len(text) == 10 and text[4] == "-" and text[7] == "-" else None  # noqa: PLR2004


_DL_COLS = [
    "run_accession",
    "study_accession",
    "experiment_accession",
    "library_layout",
    "fastq_ftp",
    "fastq_bytes",
    "fastq_md5",
]


def dl_tasks(
    pg_url: str,
    download_links_db_file: Path | None,
    batch_size: int,
) -> list[tuple[str, Callable[..., None], tuple]]:
    if not download_links_db_file or not download_links_db_file.exists():
        return []
    return [
        (
            "sra_run_download_links (DRR)",
            _transfer_dra_download_links,
            (pg_url, download_links_db_file, batch_size),
        ),
    ]


def _transfer_dra_download_links(pg_url: str, db: Path, batch_size: int) -> None:
    def rows() -> Iterator[tuple]:
        cols = ", ".join(_DL_COLS)
        with sqlite_connect(db) as c:
            yield from c.execute(
                f"SELECT {cols} FROM run_download_links "  # noqa: S608
                "WHERE run_accession LIKE 'DRR%'",
            )

    _upsert(
        pg_url,
        "sra_run_download_links",
        _DL_COLS,
        rows(),
        batch_size,
        conflict_col="run_accession",
        fill_gaps_only=True,
    )


def dra_tasks(
    pg_url: str,
    dra_db_file: Path | None,
    batch_size: int,
) -> list[tuple[str, Callable[..., None], tuple]]:
    if not dra_db_file or not dra_db_file.exists():
        return []

    common = (pg_url, dra_db_file, batch_size)
    return [
        ("dra_submissions", _transfer_submissions, common),
        ("dra_studies", _transfer_studies, common),
        ("dra_studies_search", _transfer_studies_search, common),
        ("dra_experiments", _transfer_experiments, common),
        ("dra_samples", _transfer_samples, common),
        ("dra_runs", _transfer_runs, common),
    ]


def gea_tasks(
    pg_url: str,
    gea_db_file: Path | None,
    batch_size: int,
) -> list[tuple[str, Callable[..., None], tuple]]:
    if not gea_db_file or not gea_db_file.exists():
        return []

    common = (pg_url, gea_db_file, batch_size)
    return [
        ("gea_experiments", _transfer_gea_experiments, common),
        ("gea_samples", _transfer_gea_samples, common),
    ]


def ddbj_transfer(
    pg_url: str,
    dra_db_file: Path | None,
    gea_db_file: Path | None,
    batch_size: int,
    download_links_db_file: Path | None = None,
) -> None:
    tasks = dra_tasks(pg_url, dra_db_file, batch_size)
    if tasks:
        exec_query_from_file("tables/dra_pg.sql", pg_dsn=pg_url)

    gea = gea_tasks(pg_url, gea_db_file, batch_size)
    if gea:
        exec_query_from_file("tables/gea_pg.sql", pg_dsn=pg_url)
    tasks += gea
    tasks += dl_tasks(pg_url, download_links_db_file, batch_size)

    if not tasks:
        logger.warning("no dra or gea db given; nothing to transfer")
        return

    # sequential: the whole DDBJ set is small enough (~1.9M rows) that a thread per
    # table buys little, and the child tables reference the parents.
    for name, fn, args in tasks:
        logger.info("transferring %s", name)
        fn(*args)
    logger.info("ddbj transfer complete")


def _transfer_submissions(pg_url: str, db: Path, batch_size: int) -> None:
    def rows() -> Iterator[tuple]:
        with sqlite_connect(db) as c:
            for acc, alias, broker, center, lab, pub, upd in c.execute(
                """SELECT accession, alias, broker_name, center_name, lab_name,
                    published_at, updated_at FROM dra_submissions""",
            ):
                yield (
                    acc,
                    alias,
                    broker,
                    center,
                    lab,
                    _iso_date(pub),
                    _iso_date(upd),
                )

    _upsert(pg_url, "dra_submissions", _SUBMISSIONS_COLS, rows(), batch_size)


def _transfer_studies(pg_url: str, db: Path, batch_size: int) -> None:
    def rows() -> Iterator[tuple]:
        with sqlite_connect(db) as c:
            for r in c.execute(
                """SELECT accession, alias, center_name, abstract, title, external_id,
                    study_type, attributes_json, links, center_project_name, submission,
                    published_at, updated_at, pubmed_id_pysradb, pubmed_last_checked,
                    is_only_ddbj
                FROM dra_studies""",
            ):
                (
                    acc,
                    alias,
                    center,
                    abstract,
                    title,
                    external_id,
                    study_type,
                    attrs,
                    links,
                    center_project,
                    submission,
                    pub,
                    upd,
                    pmid,
                    pm_checked,
                    is_only_ddbj,
                ) = r
                yield (
                    acc,
                    alias,
                    center,
                    abstract,
                    title,
                    external_id,
                    study_type,
                    attrs,
                    links,
                    center_project,
                    bioproject_of(external_id),
                    submission,
                    _iso_date(pub),
                    _iso_date(upd),
                    pmid,
                    _iso_date(pm_checked),
                    is_only_ddbj,
                )

    _upsert(pg_url, "dra_studies", _STUDIES_COLS, rows(), batch_size)


def _transfer_studies_search(pg_url: str, db: Path, batch_size: int) -> None:
    """Only is_only_ddbj = 1 -- see sql/tables/dra_pg.sql."""

    def rows() -> Iterator[tuple]:
        with sqlite_connect(db) as c:
            for acc, title, abstract, alias, center, project, upd, pub in c.execute(
                """SELECT accession, title, abstract, alias, center_name,
                    center_project_name, updated_at, published_at
                FROM dra_studies WHERE is_only_ddbj = 1""",
            ):
                yield (
                    acc,
                    title,
                    abstract,
                    alias,
                    center,
                    project,
                    _iso_date(upd),
                    _iso_date(pub),
                )

    _upsert(pg_url, "dra_studies_search", _SEARCH_COLS, rows(), batch_size)


def _transfer_experiments(pg_url: str, db: Path, batch_size: int) -> None:
    def rows() -> Iterator[tuple]:
        with sqlite_connect(db) as c:
            for r in c.execute(
                """SELECT accession, design_description, library_layout, library_name,
                    library_selection, library_source, library_strategy, samples,
                    platform, instrument_model, study, title, submission
                FROM dra_experiments""",
            ):
                yield (*r[:7], string_list(r[7]), *r[8:])

    _upsert(pg_url, "dra_experiments", _EXPERIMENTS_COLS, rows(), batch_size)


def _transfer_samples(pg_url: str, db: Path, batch_size: int) -> None:
    def rows() -> Iterator[tuple]:
        with sqlite_connect(db) as c:
            yield from c.execute(
                """SELECT accession, alias, description, title, scientific_name,
                    taxon_id, external_id, attributes_json, submission, links
                FROM dra_samples""",
            )

    _upsert(pg_url, "dra_samples", _SAMPLES_COLS, rows(), batch_size)


def _transfer_runs(pg_url: str, db: Path, batch_size: int) -> None:
    def rows() -> Iterator[tuple]:
        with sqlite_connect(db) as c:
            yield from c.execute(
                "SELECT accession, experiment, submission FROM dra_runs",
            )

    _upsert(pg_url, "dra_runs", _RUNS_COLS, rows(), batch_size)


def _coerce_gea(cols: list[str], row: tuple) -> tuple:
    """SQLite json arrays -> text[], date-ish text -> ISO, per the gea_pg.sql types."""
    out = []
    for col, value in zip(cols, row, strict=True):
        if col in _GEA_ARRAY_COLS:
            out.append(string_list(value))
        elif col in _GEA_DATE_COLS:
            out.append(_iso_date(value))
        else:
            out.append(value)
    return tuple(out)


def _transfer_gea_experiments(pg_url: str, db: Path, batch_size: int) -> None:
    def rows() -> Iterator[tuple]:
        cols = ", ".join(_GEA_EXPERIMENTS_COLS)
        with sqlite_connect(db) as c:
            for row in c.execute(f"SELECT {cols} FROM gea_experiments"):  # noqa: S608
                yield _coerce_gea(_GEA_EXPERIMENTS_COLS, row)

    _upsert(pg_url, "gea_experiments", _GEA_EXPERIMENTS_COLS, rows(), batch_size)


def _transfer_gea_samples(pg_url: str, db: Path, batch_size: int) -> None:
    def rows() -> Iterator[tuple]:
        cols = ", ".join(_GEA_SAMPLES_COLS)
        with sqlite_connect(db) as c:
            yield from c.execute(f"SELECT {cols} FROM gea_samples")  # noqa: S608

    _upsert(
        pg_url,
        "gea_samples",
        _GEA_SAMPLES_COLS,
        rows(),
        batch_size,
        conflict_col="id",
    )


def _upsert(
    pg_url: str,
    table: str,
    cols: list[str],
    rows: Iterator[tuple],
    batch_size: int,
    conflict_col: str = "accession",
    *,
    fill_gaps_only: bool = False,
) -> None:
    """fill_gaps_only writes only the columns the existing row leaves empty."""
    col_sql = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    payload = [c for c in cols if c != conflict_col]
    if fill_gaps_only:
        updates = ", ".join(
            f"{c} = COALESCE(NULLIF({table}.{c}, ''), EXCLUDED.{c})" for c in payload
        )
        # Postgres cannot see COALESCE(x, ...) = x. Without the guard every
        # conflicting row is rewritten: dead tuples across the table.
        guard = " WHERE " + " OR ".join(
            f"{table}.{c} IS NULL OR {table}.{c} = ''" for c in payload
        )
        action = f"DO UPDATE SET {updates}{guard}"
    else:
        update_sql = non_nulling_update(table, payload)
        action = f"DO UPDATE {update_sql}" if update_sql else "DO NOTHING"
    query = (
        f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders}) "  # noqa: S608
        f"ON CONFLICT ({conflict_col}) {action}"
    )
    with psycopg.connect(pg_url) as conn:
        batch: list[tuple] = []
        with tqdm(desc=f"copying {table}", unit="rows", leave=False) as pbar:
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
