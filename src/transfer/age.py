"""
Transfer normalized ages (age_norm.sqlite3) into Postgres ontology_samples_v4.

The target PK is (study_accession, sample) with no standalone unique key on
sample, so this UPDATEs existing rows keyed by BOTH columns (an INSERT ...
ON CONFLICT(sample) would create duplicates). Rows are streamed via COPY into a
temp table, then a single UPDATE join annotates the target. Idempotent.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, LiteralString, cast

import psycopg

from helpers import exec_query_from_file, sqlite_connect

if TYPE_CHECKING:
    from collections.abc import Iterator

logger = logging.getLogger(__name__)

_SQL_DIR = Path(__file__).resolve().parents[2] / "sql"
_AGE_INDEX = "idx_ontology_v4_age_days"

_TEMP_DDL = (
    "CREATE TEMP TABLE _age_stage ("
    "study_accession text, sample text, age_days double precision, "
    "age_group text, prenatal boolean, age_raw text) ON COMMIT DROP"
)
_UPDATE_SQL = """
UPDATE ontology_samples_v4 t
SET age_days = v.age_days, age_group = v.age_group,
    prenatal = v.prenatal, age_raw = v.age_raw
FROM _age_stage v
WHERE t.study_accession = v.study_accession AND t.sample = v.sample
"""


def _stage_rows(age_norm_db: Path) -> Iterator[tuple]:
    with sqlite_connect(age_norm_db) as sq:
        for study, sample, age_days, age_group, prenatal, age_raw in sq.execute(
            "SELECT study_accession, sample, age_days, age_group, prenatal, age_raw "
            "FROM age_norm"
        ):
            yield (
                study,
                sample,
                age_days,
                age_group,
                None if prenatal is None else bool(prenatal),
                age_raw,
            )


def age_transfer(pg_url: str, age_norm_db: Path, batch_size: int) -> None:  # noqa: ARG001
    """
    Annotate ontology_samples_v4 with normalized age columns. Idempotent.

    Heavy: one COPY of the whole stage table + one UPDATE join over the target.
    statement_timeout is disabled for the duration (like sample_tsv).
    """
    exec_query_from_file("tables/age_pg.sql", pg_dsn=pg_url)

    with psycopg.connect(pg_url) as conn, conn.cursor() as cur:
        cur.execute("SET statement_timeout = 0")
        cur.execute(_TEMP_DDL)
        copied = 0
        with cur.copy(
            "COPY _age_stage (study_accession, sample, age_days, age_group, "
            "prenatal, age_raw) FROM STDIN"
        ) as copy:
            for row in _stage_rows(age_norm_db):
                copy.write_row(row)
                copied += 1
        cur.execute("CREATE INDEX ON _age_stage (study_accession, sample)")
        cur.execute("ANALYZE _age_stage")
        cur.execute(_UPDATE_SQL)
        updated = cur.rowcount
        conn.commit()
    logger.info(
        "age transfer: copied %d stage rows, updated %d target rows", copied, updated
    )

    _create_age_index(pg_url)
    logger.info("age transfer complete")


def _create_age_index(pg_url: str) -> None:
    """
    Build the age_days index on an autocommit connection.

    CONCURRENTLY cannot run in a txn; drop any INVALID index left by an
    interrupted prior build first, then (re)create.
    """
    with psycopg.connect(pg_url, autocommit=True) as conn:
        row = conn.execute(
            "SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass(%s)",
            (_AGE_INDEX,),
        ).fetchone()
        if row is not None and row[0] is False:
            logger.warning("dropping invalid %s from an interrupted build", _AGE_INDEX)
            conn.execute(f"DROP INDEX IF EXISTS {_AGE_INDEX}")
        index_sql = (_SQL_DIR / "indexes" / "age.sql").read_text()
        conn.execute(cast("LiteralString", index_sql))
