"""
Copy phases for `sc_db build`, in execution order.

Each function replaces its target table inside a transaction; none writes to
Postgres.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from typing import TYPE_CHECKING

from sc_db import borrow, facts, matrix

if TYPE_CHECKING:
    from pathlib import Path

    import psycopg

logger = logging.getLogger(__name__)

EVIDENCE_TABLES = ("sc_rule", "single_cell_evidence", "sc_archive_coverage")

# SQLite run_preflightx maps to Postgres pentimento_run. assay_display lives
# on pentimento_sample in Postgres, so join it back or chemistry reads NULL
# while recorded one table over.
RUN_PREFLIGHTX_SQL = """
SELECT r.run_accession, r.experiment_accession, r.sample_accession,
       r.gsm_accession, r.study_accession, r.species_called,
       r.species_confidence, r.species_mislabel, r.sex_verdict,
       r.sex_confidence, r.assay, s.assay_display, r.assay_is_single_cell,
       r.modality, r.modality_confidence, r.bulk_vs_sc, r.read_end,
       r.preflightx_source, r.library_layout, r.fastq_ftp, r.fastq_bytes
FROM pentimento_run r
LEFT JOIN pentimento_sample s ON s.sample_accession = r.sample_accession
"""
RUN_PREFLIGHTX_COLUMNS = (
    "run_accession",
    "experiment_accession",
    "sample_accession",
    "gsm_accession",
    "study_accession",
    "species_called",
    "species_confidence",
    "species_mislabel",
    "sex_verdict",
    "sex_confidence",
    "assay",
    "assay_display",
    "assay_is_single_cell",
    "modality",
    "modality_confidence",
    "bulk_vs_sc",
    "read_end",
    "preflightx_source",
    "library_layout",
    "fastq_ftp",
    "fastq_bytes",
)

# Samples whose matrix was read. pentimento_sample also has NULL cells/genes
# rows; folding them into sample_ncount would make count(*) look measured.
SAMPLE_NCOUNT_SQL = """
SELECT sample_accession, study_accession, cells, genes, nnz, file_format,
       unfiltered
FROM pentimento_sample
WHERE cells IS NOT NULL OR genes IS NOT NULL
"""
SAMPLE_NCOUNT_COLUMNS = (
    "sample_accession",
    "study_accession",
    "cells",
    "genes",
    "nnz",
    "file_format",
    "unfiltered",
)

# The run rows aggregate in Postgres before crossing the tunnel. Byte
# totals are summed there too, avoiding shipment of ';'-joined file lists.
DOWNLOADS_SQL = """
SELECT l.study_accession,
       count(*),
       count(*) FILTER (WHERE coalesce(l.fastq_ftp, '') <> ''),
       count(*) FILTER (WHERE coalesce(l.fastq_ftp, '') = ''
                          AND (coalesce(l.sra_ftp, '') <> ''
                            OR coalesce(l.ncbi_sra_url, '') <> ''
                            OR coalesce(l.ncbi_sra_normalized_url, '') <> ''
                            OR coalesce(l.ncbi_sra_lite_url, '') <> '')),
       sum(b.fq),
       sum(nullif(l.sra_bytes, '')::bigint)
FROM sra_run_download_links l
LEFT JOIN LATERAL (
    SELECT sum(x::bigint) AS fq
    FROM unnest(string_to_array(l.fastq_bytes, ';')) x
    WHERE x ~ '^[0-9]+$'
) b ON true
WHERE l.study_accession = ANY(%s)
GROUP BY 1
"""


def _table_rows(conn: sqlite3.Connection, table: str) -> int:
    row = conn.execute(f"SELECT count(*) FROM {table}").fetchone()  # noqa: S608
    return int(row[0]) if row else 0


def pg_estimate(pg_conn: psycopg.Connection, table: str) -> int | None:
    """Planner estimate, for tables where count(*) is its own minutes-long job."""
    row = pg_conn.execute(
        "SELECT reltuples::bigint FROM pg_class WHERE relname = %s",
        (table,),
    ).fetchone()
    return int(row[0]) if row else None


def phase_evidence(conn: sqlite3.Connection, sc_evidence_db_file: Path) -> None:
    """Copy the evidence build's tables in whole."""
    conn.execute("ATTACH ? AS ev", (f"file:{sc_evidence_db_file}?immutable=1",))
    try:
        for table in EVIDENCE_TABLES:
            source_rows = int(
                conn.execute(f"SELECT count(*) FROM ev.{table}").fetchone()[0],  # noqa: S608
            )
            conn.execute(f"DELETE FROM {table}")  # noqa: S608
            started = time.monotonic()
            conn.execute(f"INSERT INTO {table} SELECT * FROM ev.{table}")  # noqa: S608
            conn.commit()
            copied = _table_rows(conn, table)
            logger.info(
                "  %s: %s rows in %.1fs",
                table,
                f"{copied:,}",
                time.monotonic() - started,
            )
            borrow.record_source(
                conn,
                table,
                source_kind="sqlite",
                source_ref=str(sc_evidence_db_file),
                source_rows=source_rows,
                rows_copied=copied,
                notes="sc_evidence build output, copied verbatim",
            )
    finally:
        conn.execute("DETACH ev")


def phase_pentimento(
    conn: sqlite3.Connection,
    pg_conn: psycopg.Connection,
    ncount_extra_db_file: Path | None,
) -> None:
    """Borrow the read-derived and matrix-derived facts."""
    borrow.copy_pg_table(
        conn,
        pg_conn,
        target="run_preflightx",
        pg_table="pentimento_run",
        query=RUN_PREFLIGHTX_SQL,
        columns=RUN_PREFLIGHTX_COLUMNS,
        notes="assay_display joined from pentimento_sample; the numeric "
        "evidence columns (y_hits, intron_frac, ...) are NOT in the "
        "Postgres projection and stay NULL here",
    )
    borrow.copy_pg_table(
        conn,
        pg_conn,
        target="sample_ncount",
        pg_table="pentimento_sample",
        query=SAMPLE_NCOUNT_SQL,
        columns=SAMPLE_NCOUNT_COLUMNS,
        copy_filter="cells IS NOT NULL OR genes IS NOT NULL",
        notes="scope is not in the Postgres projection",
    )
    for table in ("pentimento_sample", "pentimento_study"):
        borrow.copy_pg_table(conn, pg_conn, target=table, pg_table=table)
    if ncount_extra_db_file is not None:
        _load_ncount_extra(conn, ncount_extra_db_file)


def _load_ncount_extra(conn: sqlite3.Connection, db_file: Path) -> None:
    """
    Cell counts measured after the Postgres snapshot, kept separately named.

    Merging these into sample_ncount would make the next re-borrow of Postgres
    look like a loss of samples. They are unioned at query time instead.
    """
    with sqlite3.connect(f"file:{db_file}?immutable=1", uri=True) as src:
        rows = src.execute(
            "SELECT sample, accession, cells, genes, file_format, unfiltered "
            "FROM ncount_sample",
        ).fetchall()
    conn.execute("DELETE FROM ncount_extra WHERE source_file = ?", (str(db_file),))
    conn.executemany(
        "INSERT OR REPLACE INTO ncount_extra (sample_accession, study_accession, "
        "cells, genes, file_format, unfiltered, source_file) VALUES (?,?,?,?,?,?,?)",
        [(*row, str(db_file)) for row in rows],
    )
    conn.commit()
    borrow.record_source(
        conn,
        "ncount_extra",
        source_kind="sqlite",
        source_ref=str(db_file),
        source_rows=len(rows),
        rows_copied=_table_rows(conn, "ncount_extra"),
        notes="ncount gap run; not yet in Postgres pentimento_sample",
    )


def phase_unified(conn: sqlite3.Connection, pg_conn: psycopg.Connection) -> None:
    """Borrow unified_metadata for the cohort view."""
    borrow.copy_pg_table(
        conn,
        pg_conn,
        target="unified_metadata",
        pg_table="unified_metadata",
        notes="columns the SQLite schema declares and Postgres does not have "
        "are listed in columns_missing and are NULL here",
    )


def candidate_accessions(conn: sqlite3.Connection) -> list[str]:
    """
    Every accession the queue's studies might hold runs under.

    Alias groups connect archive evidence keys to run-bearing download keys.
    The aggregate is restricted to candidate accessions before grouping.
    """
    studies = [
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT study_accession FROM single_cell_evidence "
            "WHERE study_accession IS NOT NULL",
        )
    ]
    groups = facts.alias_groups(conn, studies)
    return sorted({acc for group in groups.values() for acc in group})


def phase_downloads(conn: sqlite3.Connection, pg_conn: psycopg.Connection) -> None:
    """Roll download-link rows up to per-study counts and bytes."""
    accessions = candidate_accessions(conn)
    logger.info("  probing %s candidate accessions", f"{len(accessions):,}")
    started = time.monotonic()
    rows = pg_conn.execute(DOWNLOADS_SQL, (accessions,)).fetchall()
    logger.info(
        "  postgres returned %s study rollups in %.1fs",
        f"{len(rows):,}",
        time.monotonic() - started,
    )
    copied = borrow.load_rows(
        conn,
        "sc_study_downloads",
        (
            "study_accession",
            "n_runs",
            "n_runs_fastq",
            "n_runs_sra_only",
            "fastq_bytes",
            "sra_bytes",
        ),
        rows,
        label="sc_study_downloads",
    )
    borrow.record_source(
        conn,
        "sc_study_downloads",
        source_kind="postgres",
        source_ref="sra_run_download_links",
        source_rows=pg_estimate(pg_conn, "sra_run_download_links"),
        rows_copied=copied,
        copy_filter=f"study_accession IN (…{len(accessions)} alias-group members)",
        notes="source_rows is the planner's reltuples estimate: count(*) on "
        "that table is its own job. Aggregated server-side.",
    )


def phase_matrix(
    conn: sqlite3.Connection,
    geo_db_file: Path | None,
    ae_db_file: Path | None,
) -> None:
    """Find countable matrices with the tenx_output_files term list."""
    rows = matrix.geo_sample_channel(conn)
    logger.info("  geo_sample_tenx: %s studies", f"{len(rows):,}")
    if geo_db_file is not None:
        series = matrix.geo_series_channel(geo_db_file)
        logger.info("  geo_series_tenx: %s studies", f"{len(series):,}")
        rows += series
    else:
        logger.warning("no --geo-db-file: series-level matrices are NOT counted")
    if ae_db_file is not None:
        ae_rows = matrix.ae_channel(ae_db_file)
        logger.info(
            "  ae_processed: %s studies (%s more hold only .mtx/.h5ad/.loom, "
            "which the 10x term list excludes by design)",
            f"{len(ae_rows):,}",
            f"{matrix.ae_uncounted_extensions(ae_db_file):,}",
        )
        rows += ae_rows
    else:
        logger.warning("no --ae-db-file: ArrayExpress matrices are NOT counted")
    copied = borrow.load_rows(
        conn,
        "sc_study_matrix",
        (
            "study_accession",
            "channel",
            "n_objects",
            "has_countable",
            "terms",
            "example_value",
        ),
        rows,
        label="sc_study_matrix",
    )
    borrow.record_source(
        conn,
        "sc_study_matrix",
        source_kind="derived",
        source_ref=f"tenx_output_files terms; geo={geo_db_file}; ae={ae_db_file}",
        source_rows=None,
        rows_copied=copied,
        notes="has_countable=0 means a 10x pipeline artefact matched but no "
        "countable matrix did",
    )
