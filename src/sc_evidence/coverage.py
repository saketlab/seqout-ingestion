"""
Record sc_archive_coverage per archive and build.

Append-only history makes rule losses visible. The key check is
studies_declared_sc <= studies_with_evidence, using archive-owned denominators.
"""

import logging
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from helpers import exec_query_from_file, sqlite_connect
from sc_evidence.build import TABLES_SQL, open_archive_readonly
from sc_evidence.rules import RULES

logger = logging.getLogger(__name__)

_SC_GATE = "upper(library_source) LIKE '%SINGLE CELL%'"


@dataclass(frozen=True)
class _ArchiveQueries:
    """
    Denominators from archive-owned tables.

    `declared` is None for archives with prose-only evidence channels. NULL is
    not zero: GEO lacks library_source, and writing 0 there would read as
    "GEO declares no single-cell data".
    """

    total: str
    declared: str | None
    note: str | None = None
    # Extra archive slugs ATTACHed before queries run, plus empty stand-ins for
    # absent files. GSA needs this because CRA and GSA-Human together define the
    # archive-level audit number.
    attach: tuple[str, ...] = ()
    stub: tuple[str, ...] = ()


ARCHIVE_QUERIES: dict[str, _ArchiveQueries] = {
    "sra": _ArchiveQueries(
        total="SELECT count(*) FROM sra_studies",
        declared=(
            "SELECT count(DISTINCT study) FROM sra_experiments "  # noqa: S608
            f"WHERE {_SC_GATE}"
        ),
    ),
    "ena": _ArchiveQueries(
        # ENA is a base checkpoint plus dated increments; both count. The base alone
        # is just a floor. ena_studies keys study_accession, while SRA/GSA use
        # accession.
        total=(
            "SELECT count(*) FROM ("
            "  SELECT study_accession FROM ena_studies"
            "  UNION"
            "  SELECT study_accession FROM ena_delta.ena_studies)"
        ),
        declared=(
            "SELECT count(*) FROM ("  # noqa: S608
            "  SELECT DISTINCT study_accession FROM ena_experiments "
            f"  WHERE {_SC_GATE}"
            "  UNION"
            "  SELECT DISTINCT study_accession FROM ena_delta.ena_experiments "
            f"  WHERE {_SC_GATE})"
        ),
        attach=("ena_delta",),
        stub=(
            "CREATE TABLE IF NOT EXISTS ena_delta.ena_studies (study_accession TEXT)",
            "CREATE TABLE IF NOT EXISTS ena_delta.ena_experiments "
            "(experiment_accession TEXT, study_accession TEXT, "
            "library_source TEXT, library_strategy TEXT)",
        ),
    ),
    "geo": _ArchiveQueries(
        total="SELECT count(*) FROM geo_series",
        declared=None,
        note="GEO has no library_source; declared count is not derivable",
    ),
    "gsa": _ArchiveQueries(
        total=(
            "SELECT count(*) FROM ("
            "  SELECT accession FROM gsa_studies"
            "  UNION"
            "  SELECT accession FROM gsa_human.gsa_human_studies)"
        ),
        declared=(
            "SELECT count(*) FROM ("  # noqa: S608
            "  SELECT DISTINCT study_accession FROM gsa_experiments "
            f"  WHERE {_SC_GATE}"
            "  UNION"
            "  SELECT DISTINCT study_accession FROM gsa_human.gsa_human_experiments "
            f"  WHERE {_SC_GATE})"
        ),
        attach=("gsa_human",),
        stub=(
            "CREATE TABLE IF NOT EXISTS gsa_human.gsa_human_studies (accession TEXT)",
            "CREATE TABLE IF NOT EXISTS gsa_human.gsa_human_experiments "
            "(accession TEXT, study_accession TEXT, library_source TEXT)",
        ),
    ),
    "dra": _ArchiveQueries(
        total="SELECT count(*) FROM dra_studies",
        declared=(
            "SELECT count(DISTINCT study) FROM dra_experiments "  # noqa: S608
            f"WHERE {_SC_GATE}"
        ),
    ),
    "ae": _ArchiveQueries(
        total="SELECT count(*) FROM arrayexpress_experiments",
        # AE denominator is experiments: arrayexpress_samples has no sample accession,
        # and library_source is often NULL, so this is a declared floor.
        declared=(
            "SELECT count(DISTINCT experiment_accession) FROM arrayexpress_samples "  # noqa: S608
            f"WHERE {_SC_GATE}"
        ),
    ),
    "gea": _ArchiveQueries(
        total="SELECT count(*) FROM gea_experiments",
        declared=(
            "SELECT count(*) FROM gea_experiments "
            "WHERE upper(study_type) LIKE '%SINGLE CELL%'"
        ),
    ),
    "enrichment": _ArchiveQueries(
        total="SELECT count(DISTINCT study_accession) FROM ontology_samples",
        # Use the enrichment assay_category denominator. Free-text assay arms also
        # contribute, so this check catches recall loss there.
        declared=(
            "SELECT count(DISTINCT study_accession) FROM ontology_samples "
            "WHERE upper(assay_category) IN ("
            "'SINGLE-CELL TRANSCRIPTOMICS','SINGLE-CELL EPIGENOMICS',"
            "'SINGLE-CELL MULTI-OMICS','SINGLE-CELL GENOMICS',"
            "'SPATIAL TRANSCRIPTOMICS')"
        ),
        note=(
            "denominator is assay_category only, independent of the rule's "
            "free-text arm"
        ),
    ),
}


def _scalar(conn: sqlite3.Connection, sql: str) -> int | None:
    try:
        row = conn.execute(sql).fetchone()
    except sqlite3.Error:
        logger.exception("coverage query failed: %s", sql)
        return None
    return int(row[0]) if row and row[0] is not None else None


def _found(conn: sqlite3.Connection, archive: str) -> tuple[int, int, int]:
    """Count what this build found for one archive, from the evidence DB."""
    row = conn.execute(
        "SELECT count(DISTINCT e.study_accession), "
        "       count(DISTINCT CASE WHEN r.tier='high' "
        "                           THEN e.study_accession END), "
        "       count(DISTINCT CASE WHEN e.grain='experiment' "
        "                           THEN e.accession END) "
        "FROM single_cell_evidence e JOIN sc_rule r ON r.id = e.rule_id "
        "WHERE r.archive = ?",
        (archive,),
    ).fetchone()
    return (row[0] or 0, row[1] or 0, row[2] or 0)


def _previous(conn: sqlite3.Connection, archive: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT studies_with_evidence, studies_high_tier, built_at "
        "FROM sc_archive_coverage WHERE archive = ? "
        "ORDER BY built_at DESC LIMIT 1",
        (archive,),
    ).fetchone()


def sc_evidence_coverage(
    sc_evidence_db_file: Path,
    archive_db_files: dict[str, Path | None],
    fail_on_regression: bool = False,  # noqa: FBT002
) -> None:
    """
    Append one sc_archive_coverage row per archive for this build.

    Missing files yield rows with NULL denominators and notes. fail_on_regression
    exits when coverage drops or declared single-cell exceeds found evidence.
    """
    built_at = datetime.now(UTC).isoformat(timespec="seconds")
    archives = sorted({r.archive for r in RULES} | set(ARCHIVE_QUERIES))

    conn = sqlite_connect(sc_evidence_db_file)
    problems: list[str] = []
    try:
        # Idempotent, and it means `coverage` can run against a database whose
        # build predates a schema addition.
        exec_query_from_file(TABLES_SQL, conn=conn)

        for archive in archives:
            found, high, experiments = _found(conn, archive)
            previous = _previous(conn, archive)

            queries = ARCHIVE_QUERIES.get(archive)
            db_file = archive_db_files.get(archive)
            total: int | None = None
            declared: int | None = None
            notes: str | None = None

            if queries is None:
                notes = "no denominator query defined for this archive"
            elif db_file is None or not Path(db_file).exists():
                notes = "archive database not provided; denominators unknown"
            else:
                archive_conn = open_archive_readonly(
                    Path(db_file),
                    {name: archive_db_files.get(name) for name in queries.attach},
                )
                try:
                    for statement in queries.stub:
                        archive_conn.execute(statement)
                    total = _scalar(archive_conn, queries.total)
                    if queries.declared is not None:
                        declared = _scalar(archive_conn, queries.declared)
                finally:
                    archive_conn.close()
                notes = queries.note

            conn.execute(
                "INSERT INTO sc_archive_coverage (archive, built_at, "
                "studies_total, studies_with_evidence, studies_high_tier, "
                "experiments_with_evidence, studies_declared_sc, notes) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (archive, built_at, total, found, high, experiments, declared, notes),
            )
            logger.info(
                "%-4s total %s  evidence %d  high %d  experiments %d  declared %s",
                archive,
                "?" if total is None else f"{total:,}",
                found,
                high,
                experiments,
                "?" if declared is None else f"{declared:,}",
            )

            if declared is not None and declared > found:
                problems.append(
                    f"{archive}: declares {declared:,} single-cell studies but the "
                    f"rules found {found:,} -- declared data is on the floor",
                )
            if previous is not None and found < (previous[0] or 0):
                problems.append(
                    f"{archive}: coverage fell from {previous[0]:,} to {found:,} "
                    f"since {previous[2]}",
                )
        conn.commit()
    finally:
        conn.close()

    for problem in problems:
        logger.error("REGRESSION %s", problem)
    if problems and fail_on_regression:
        raise SystemExit(1)
