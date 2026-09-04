"""
Build the single-cell database and work queue.

Phases replace target tables transactionally. `--resume` skips populated phases
with provenance; Postgres access is SELECT-only.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import psycopg

from helpers import exec_query_from_file, sqlite_connect
from sc_db import phases, queue

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

logger = logging.getLogger(__name__)

PHASES = ("evidence", "pentimento", "unified", "downloads", "matrix", "queue")

SCHEMA_FILES = (
    "tables/single_cell_evidence.sql",
    "tables/pentimento.sql",
    "tables/unified_metadata.sql",
    "tables/sc_work_queue.sql",
)
INDEX_FILES = (
    "indexes/single_cell_evidence.sql",
    "indexes/pentimento.sql",
    "indexes/unified_metadata.sql",
)


def _apply_schema(conn: sqlite3.Connection) -> None:
    for sql_file in SCHEMA_FILES:
        exec_query_from_file(sql_file, conn=conn)
    for sql_file in INDEX_FILES:
        exec_query_from_file(sql_file, conn=conn)


def _table_rows(conn: sqlite3.Connection, table: str) -> int:
    row = conn.execute(f"SELECT count(*) FROM {table}").fetchone()  # noqa: S608
    return int(row[0]) if row else 0


def _already_done(conn: sqlite3.Connection, table: str) -> bool:
    provenance = conn.execute(
        "SELECT 1 FROM sc_borrowed_source WHERE table_name = ?",
        (table,),
    ).fetchone()
    return bool(provenance) and _table_rows(conn, table) > 0


def _dry_run(
    conn: sqlite3.Connection,
    pg_conn: psycopg.Connection | None,
    wanted: Sequence[str],
    sc_evidence_db_file: Path,
) -> None:
    logger.info("DRY RUN -- nothing is written")
    if "evidence" in wanted:
        with sqlite3.connect(f"file:{sc_evidence_db_file}?immutable=1", uri=True) as ev:
            for table in phases.EVIDENCE_TABLES:
                rows = ev.execute(f"SELECT count(*) FROM {table}").fetchone()[0]  # noqa: S608
                logger.info("  evidence  %-22s would copy %s rows", table, f"{rows:,}")
    if pg_conn is None:
        return
    plan = {
        "pentimento": ("pentimento_run", "pentimento_sample", "pentimento_study"),
        "unified": ("unified_metadata",),
    }
    for phase, tables in plan.items():
        if phase in wanted:
            for table in tables:
                logger.info(
                    "  %-10s %-22s would copy ~%s rows",
                    phase,
                    table,
                    f"{phases.pg_estimate(pg_conn, table):,}",
                )
    if "downloads" in wanted:
        has_unified = _table_rows(conn, "unified_metadata") > 0
        accessions = phases.candidate_accessions(conn) if has_unified else []
        logger.info(
            "  downloads  sra_run_download_links: ~%s rows in Postgres, "
            "aggregated over %s candidate accessions (0 if unified_metadata "
            "has not been borrowed yet)",
            f"{phases.pg_estimate(pg_conn, 'sra_run_download_links'):,}",
            f"{len(accessions):,}",
        )
    if "queue" in wanted:
        studies = conn.execute(
            "SELECT count(DISTINCT study_accession) FROM single_cell_evidence "
            "WHERE study_accession IS NOT NULL",
        ).fetchone()[0]
        logger.info("  queue      sc_work_queue: would rebuild %s rows", f"{studies:,}")


def sc_db_build(
    sc_db_file: Path,
    sc_evidence_db_file: Path,
    pg_url: str,
    *,
    geo_db_file: Path | None = None,
    ae_db_file: Path | None = None,
    ncount_extra_db_file: Path | None = None,
    queue_csv_file: Path | None = None,
    phases: Sequence[str] = PHASES,
    is_resume: bool = False,
    is_dry_run: bool = False,
) -> None:
    """Run the phases in order against one output file."""
    sc_db_file.parent.mkdir(parents=True, exist_ok=True)
    # uri=True so ATTACH can take a `file:...?immutable=1` source; a plain path
    # is still a plain path, SQLite only parses a name starting with "file:".
    conn = sqlite_connect(sc_db_file, uri=True)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    _apply_schema(conn)
    needs_pg = bool({"pentimento", "unified", "downloads"} & set(phases))
    pg_conn = psycopg.connect(pg_url, connect_timeout=30) if needs_pg else None
    if pg_conn is not None:
        pg_conn.read_only = True

    started = time.monotonic()
    try:
        if is_dry_run:
            _dry_run(conn, pg_conn, phases, sc_evidence_db_file)
            return
        for phase in PHASES:
            if phase not in phases:
                continue
            _run_phase(
                phase,
                conn,
                pg_conn,
                sc_evidence_db_file=sc_evidence_db_file,
                geo_db_file=geo_db_file,
                ae_db_file=ae_db_file,
                ncount_extra_db_file=ncount_extra_db_file,
                queue_csv_file=queue_csv_file,
                is_resume=is_resume,
            )
    finally:
        if pg_conn is not None:
            pg_conn.close()
        conn.execute("PRAGMA optimize")
        conn.close()
    logger.info("done in %.1f min -> %s", (time.monotonic() - started) / 60, sc_db_file)


_RESUME_SENTINEL = {
    "evidence": "single_cell_evidence",
    "pentimento": "run_preflightx",
    "unified": "unified_metadata",
    "downloads": "sc_study_downloads",
    "matrix": "sc_study_matrix",
}


def _run_phase(
    phase: str,
    conn: sqlite3.Connection,
    pg_conn: psycopg.Connection | None,
    *,
    sc_evidence_db_file: Path,
    geo_db_file: Path | None,
    ae_db_file: Path | None,
    ncount_extra_db_file: Path | None,
    queue_csv_file: Path | None,
    is_resume: bool,
) -> None:
    sentinel = _RESUME_SENTINEL.get(phase, "sc_work_queue")
    if is_resume and (
        _already_done(conn, sentinel)
        or (phase == "queue" and _table_rows(conn, "sc_work_queue") > 0)
    ):
        logger.info("phase %s: already populated, skipped (--resume)", phase)
        return
    logger.info("phase %s: starting at %s", phase, datetime.now(UTC).isoformat())
    started = time.monotonic()
    if phase == "evidence":
        phases.phase_evidence(conn, sc_evidence_db_file)
    elif phase == "pentimento" and pg_conn is not None:
        phases.phase_pentimento(conn, pg_conn, ncount_extra_db_file)
    elif phase == "unified" and pg_conn is not None:
        phases.phase_unified(conn, pg_conn)
    elif phase == "downloads" and pg_conn is not None:
        phases.phase_downloads(conn, pg_conn)
    elif phase == "matrix":
        phases.phase_matrix(conn, geo_db_file, ae_db_file)
    elif phase == "queue":
        queue.build_queue(conn, queue_csv_file)
    logger.info("phase %s: done in %.1fs", phase, time.monotonic() - started)


def summarise(sc_db_file: Path) -> str:
    """Bucket counts with denominators, printed at the end of a build."""
    conn = sqlite_connect(f"file:{sc_db_file}?immutable=1", uri=True)
    try:
        total = _table_rows(conn, "sc_work_queue")
        lines = [f"sc_work_queue: {total:,} studies"]
        for label, sql in (
            (
                "data_bucket",
                "SELECT data_bucket, count(*) FROM sc_work_queue "
                "GROUP BY 1 ORDER BY 2 DESC",
            ),
            (
                "next_stage",
                "SELECT next_stage, count(*), sum(n_runs), "
                "sum(coalesce(fastq_bytes,0)) FROM sc_work_queue "
                "GROUP BY 1 ORDER BY 2 DESC",
            ),
        ):
            lines.append(f"-- {label}")
            lines.extend("   " + json.dumps(row) for row in conn.execute(sql))
    finally:
        conn.close()
    return "\n".join(lines)
