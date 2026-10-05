#!/usr/bin/env python3
"""
Transfer the recall fix from SQLite into Postgres. Dry-run by default.

Two write patterns, because the columns mean different things:

    ontology_samples_v4.organism  Fill, then correct. Archive organism values
        override LLM-derived values that contradict them. Prior values are saved
        in sample_organism_overwrite before the write; do not rebuild that table
        after applying, or rollback loses its source.

    unified_metadata.is_single_cell / single_cell_status  Rebuilt from current
        pentimento/preflightx state. NULL means "nothing has screened this", not
        a gap. Same exception as transfer/pentimento.py::_upsert; read that
        docstring before adding another pattern.

The `not_screened` reset is the one destructive-looking step: it sets
is_single_cell to NULL on rows currently reading 0. Those 0s were manufactured by
a column DEFAULT, never by a measurement, so the reset removes a false claim
rather than real data. Nothing that was ever measured is touched, and --dry-run
prints the exact counts first.

Refresh loop (after new preflightx/pentimento data lands):
    python3 recipes/recall_fix/build.py --db ~/recall_fix.db --only single-cell
    python3 recipes/recall_fix/transfer.py --db ~/recall_fix.db --only single-cell
    python3 recipes/recall_fix/transfer.py --db ~/recall_fix.db --only single-cell \
        --apply
"""

from __future__ import annotations

import argparse
import logging
import pathlib
import sqlite3
import sys
from typing import TYPE_CHECKING

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from pg_connect import connect

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from psycopg import Connection, Cursor

logger = logging.getLogger("recall_fix.transfer")

# Schema lives in sql/; see CLAUDE.md. With the ALTERs inline, this recipe was
# the only record of single_cell_status, so a rebuild from scratch produced a
# table without it.
SCHEMA_FILES = (
    "tables/unified_metadata_pg.sql",
    "indexes/unified_metadata_pg.sql",
)
SQL_DIR = pathlib.Path(__file__).resolve().parent.parent.parent / "sql"


def apply_schema(pg: Connection) -> None:
    """
    Bring unified_metadata up to the schema the repo declares.

    Idempotent: CREATE TABLE IF NOT EXISTS, ADD COLUMN IF NOT EXISTS, CREATE
    INDEX IF NOT EXISTS throughout, so this adds only what is missing and is
    safe to re-run.
    """
    cur = pg.cursor()
    for rel in SCHEMA_FILES:
        path = SQL_DIR / rel
        logger.info("applying %s", path)
        cur.execute(path.read_text())
    pg.commit()


BATCH = 20_000

# One definition, used by both the dry run and the apply, so a statement that
# passes the dry run is literally the statement that runs.
FILL_SQL = (
    "UPDATE ontology_samples_v4 s SET organism = st.organism,"
    "       taxid = COALESCE(s.taxid, st.taxid)"
    "  FROM _stage st WHERE st.sample = s.sample AND s.organism IS NULL"
)

OVERWRITE_SQL = (
    "UPDATE ontology_samples_v4 s"
    "   SET organism = ow.organism, taxid = COALESCE(ow.taxid, s.taxid)"
    "  FROM _ow ow WHERE ow.sample = s.sample"
    "   AND s.organism = ow.prior_organism"
)

ROLLBACK_SQL = (
    "UPDATE ontology_samples_v4 s"
    "   SET organism = rb.prior_organism, taxid = rb.prior_taxid"
    "  FROM _rb rb WHERE rb.sample = s.sample AND s.organism = rb.organism"
)


def _chunks(rows: Iterable, n: int = BATCH) -> Iterator[list]:
    buf = []
    for r in rows:
        buf.append(r)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf


def snapshot(sq: sqlite3.Connection, pg: Connection, phase: str) -> None:
    """
    Record postgres's current values for every row this recipe will touch.

    Run with phase='before' just ahead of a write and phase='after' just behind
    it, so the effect of an apply is a diff of two tables in the staging file
    rather than a claim in a log. Only rows named by the staging tables are
    captured; nothing else is read or written.
    """
    cur = pg.cursor()
    samples = [
        r[0]
        for r in sq.execute(
            "SELECT sample FROM sample_organism"
            " UNION SELECT sample FROM sample_organism_overwrite"
        )
    ]
    sq.execute("DELETE FROM pg_snapshot_sample WHERE phase = ?", (phase,))
    logger.info("snapshot %-6s: %d samples", phase, len(samples))
    for chunk in _chunks(samples, 50_000):
        cur.execute(
            "SELECT DISTINCT ON (sample) sample, organism, taxid"
            "  FROM ontology_samples_v4 WHERE sample = ANY(%s) ORDER BY sample",
            (chunk,),
        )
        sq.executemany(
            "INSERT OR REPLACE INTO pg_snapshot_sample"
            " (phase, sample, organism, taxid) VALUES (?, ?, ?, ?)",
            [(phase, s, o, tx) for s, o, tx in cur.fetchall()],
        )
    sq.commit()

    sq.execute("DELETE FROM pg_snapshot_study WHERE phase = ?", (phase,))
    # Every study, not just the staged ones: the single-cell write also resets
    # studies that have NO evidence, and a snapshot that skipped them could not
    # show what that reset did.
    cur.execute("SELECT COUNT(*) FROM unified_metadata")
    logger.info("snapshot %-6s: %d studies", phase, cur.fetchone()[0])
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.columns"
        " WHERE table_name = 'unified_metadata' AND column_name = 'single_cell_status'"
    )
    has_status = cur.fetchone()[0] > 0
    status_col = "single_cell_status" if has_status else "NULL::text"
    named = pg.cursor(name=f"snap_{phase}")
    named.itersize = 50_000
    # status_col is one of two literals chosen above, never caller input.
    named.execute(
        "SELECT canonical_accession, is_single_cell, "  # noqa: S608
        + status_col
        + ", single_cell_modality FROM unified_metadata"
    )
    while True:
        rows = named.fetchmany(50_000)
        if not rows:
            break
        sq.executemany(
            "INSERT OR REPLACE INTO pg_snapshot_study"
            " (phase, study_accession, is_single_cell, single_cell_status,"
            "  single_cell_modality) VALUES (?, ?, ?, ?, ?)",
            [
                (phase, a, None if sc is None else int(sc), st, mod)
                for a, sc, st, mod in rows
            ],
        )
    named.close()
    sq.commit()


def report_diff(sq: sqlite3.Connection) -> None:
    """Report what actually changed, from the before/after snapshots."""
    row = sq.execute(
        "SELECT COUNT(*) FROM pg_snapshot_sample b JOIN pg_snapshot_sample a"
        "   ON a.sample = b.sample AND a.phase = 'after' AND b.phase = 'before'"
        " WHERE a.organism IS NOT b.organism"
    ).fetchone()
    logger.info("samples whose organism changed: %s", row[0])
    joined = (
        "SELECT COUNT(*) FROM pg_snapshot_sample b JOIN pg_snapshot_sample a"
        "   ON a.sample = b.sample AND a.phase = 'after' AND b.phase = 'before'"
        " WHERE "
    )
    for kind, sql in (
        ("filled (NULL -> value)", "b.organism IS NULL AND a.organism IS NOT NULL"),
        (
            "corrected (value -> value)",
            "b.organism IS NOT NULL AND a.organism IS NOT NULL"
            " AND a.organism <> b.organism",
        ),
        # A value becoming NULL would mean this recipe destroyed information.
        # It must always print 0; if it ever does not, roll back and find out why.
        ("LOST (value -> NULL)", "b.organism IS NOT NULL AND a.organism IS NULL"),
    ):
        n = sq.execute(joined + sql).fetchone()[0]
        logger.info("  %-28s %d", kind, n)
    for before, after, n in sq.execute(
        "SELECT b.is_single_cell, a.is_single_cell, COUNT(*)"
        "  FROM pg_snapshot_study b JOIN pg_snapshot_study a"
        "    ON a.study_accession = b.study_accession"
        "   AND a.phase = 'after' AND b.phase = 'before'"
        " WHERE a.is_single_cell IS NOT b.is_single_cell"
        " GROUP BY 1, 2 ORDER BY 3 DESC"
    ):
        logger.info("  is_single_cell %s -> %s: %d", before, after, n)


def transfer_organism(sq: sqlite3.Connection, pg: Connection, apply: bool) -> None:
    total = sq.execute("SELECT COUNT(*) FROM sample_organism").fetchone()[0]
    logger.info("staged sample_organism rows: %d", total)
    for route, n in sq.execute(
        "SELECT route, COUNT(*) FROM sample_organism GROUP BY 1 ORDER BY 2 DESC"
    ):
        logger.info("  route %-14s %d", route, n)

    cur = pg.cursor()
    cur.execute("SELECT COUNT(*) FROM ontology_samples_v4 WHERE organism IS NULL")
    before = cur.fetchone()[0]
    logger.info("postgres NULL organism before: %d", before)

    if not apply:
        # Cost the write without doing it: how many staged rows land on a row
        # that is still NULL. Anything else is a no-op by construction.
        cur.execute(
            "CREATE TEMP TABLE _stage"
            " (sample TEXT PRIMARY KEY, organism TEXT, taxid TEXT)"
        )
        _load_stage(sq, cur, "SELECT sample, organism, taxid FROM sample_organism")
        cur.execute(
            "SELECT COUNT(*) FROM _stage st"
            " JOIN ontology_samples_v4 s ON s.sample = st.sample"
            " WHERE s.organism IS NULL"
        )
        logger.info(
            "DRY RUN: would fill %d rows; 0 overwritten, 0 nulled", cur.fetchone()[0]
        )
        cur.execute(
            "SELECT COUNT(*) FROM _stage st"
            " JOIN ontology_samples_v4 s ON s.sample = st.sample"
            " WHERE s.organism IS NOT NULL AND s.organism <> st.organism"
        )
        cur.execute(
            "SELECT COUNT(*) FROM _stage st"
            " JOIN ontology_samples_v4 s ON s.sample = st.sample"
            " WHERE s.organism IS NOT NULL AND s.organism <> st.organism"
        )
        logger.info(
            "DRY RUN: %d rows disagree with an existing value and are LEFT ALONE",
            cur.fetchone()[0],
        )
        # Run the REAL statement and roll it back. Counting rows proves the
        # staging table joins; it does not prove the UPDATE compiles. The first
        # apply died on "COALESCE types text and integer cannot be matched"
        # after an 11-minute snapshot, because a dry run that never executes the
        # write cannot catch a type error in it. Postgres DDL and DML are both
        # transactional, so this costs one rollback and catches the whole class.
        cur.execute(FILL_SQL)
        logger.info("DRY RUN: UPDATE executed and rolled back, %d rows", cur.rowcount)
        pg.rollback()
        return

    cur.execute(
        # taxid is TEXT in ontology_samples_v4. Declaring it INT here makes the
        # UPDATE fail with "COALESCE types text and integer cannot be matched",
        # and the dry run never ran the UPDATE, so it surfaced only at apply
        # time. Hence _dry_run_update below.
        "CREATE TEMP TABLE _stage (sample TEXT PRIMARY KEY, organism TEXT, taxid TEXT)"
    )
    _load_stage(sq, cur, "SELECT sample, organism, taxid FROM sample_organism")
    cur.execute("ANALYZE _stage")
    cur.execute(FILL_SQL)
    logger.info("filled %d rows", cur.rowcount)
    pg.commit()
    cur.execute("SELECT COUNT(*) FROM ontology_samples_v4 WHERE organism IS NULL")
    logger.info("postgres NULL organism after: %d", cur.fetchone()[0])


def transfer_organism_overwrite(
    sq: sqlite3.Connection, pg: Connection, apply: bool
) -> None:
    """
    Correct existing organisms that contradict the archive record.

    Guarded by a re-check of the prior value: a row is overwritten only if it
    still holds the staged prior. Rows changed by someone else are skipped and
    reported, keeping the undo log aligned with the table.
    """
    n = sq.execute("SELECT COUNT(*) FROM sample_organism_overwrite").fetchone()[0]
    logger.info("staged organism corrections: %d", n)
    if not n:
        logger.info("nothing staged; run build.py --only organism-overwrite first")
        return
    for prior, new_org, cnt in sq.execute(
        "SELECT prior_organism, organism, COUNT(*) FROM sample_organism_overwrite"
        " GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 12"
    ):
        logger.info("  %-30s -> %-30s %d", str(prior)[:29], str(new_org)[:29], cnt)

    cur = pg.cursor()
    cur.execute(
        "CREATE TEMP TABLE _ow (sample TEXT PRIMARY KEY, prior_organism TEXT,"
        " organism TEXT, taxid TEXT)"
    )
    rows = sq.execute(
        "SELECT sample, prior_organism, organism, taxid FROM sample_organism_overwrite"
    )
    for chunk in _chunks(rows):
        with cur.copy(
            "COPY _ow (sample, prior_organism, organism, taxid) FROM STDIN"
        ) as cp:
            for r in chunk:
                cp.write_row(r)
    cur.execute("ANALYZE _ow")

    # `sample` is not unique in ontology_samples_v4, so one staged correction can
    # match several table rows. NULL copies come from the fill pass; excluding
    # them keeps this warning focused on real drift.
    cur.execute(
        "SELECT COUNT(*) FROM _ow ow JOIN ontology_samples_v4 s ON s.sample = ow.sample"
        " WHERE s.organism IS NOT NULL AND s.organism <> ow.prior_organism"
    )
    drifted = cur.fetchone()[0]
    if drifted:
        logger.warning(
            "%d rows hold a non-NULL organism that is neither the recorded prior"
            " value nor NULL; they are SKIPPED so the undo log stays truthful",
            drifted,
        )

    if not apply:
        cur.execute(OVERWRITE_SQL)
        logger.info(
            "DRY RUN: UPDATE executed and rolled back, would overwrite %d rows",
            cur.rowcount,
        )
        pg.rollback()
        return

    cur.execute(OVERWRITE_SQL)
    logger.info("overwrote %d rows", cur.rowcount)
    pg.commit()


def rollback_organism(sq: sqlite3.Connection, pg: Connection, apply: bool) -> None:
    """
    Put every overwritten organism back to what it was.

    Only touches rows that still hold the value this recipe wrote, so later
    legitimate corrections are not reverted.
    """
    cur = pg.cursor()
    cur.execute(
        "CREATE TEMP TABLE _rb (sample TEXT PRIMARY KEY, prior_organism TEXT,"
        " prior_taxid TEXT, organism TEXT)"
    )
    rows = sq.execute(
        "SELECT sample, prior_organism, prior_taxid, organism"
        "  FROM sample_organism_overwrite"
    )
    for chunk in _chunks(rows):
        with cur.copy(
            "COPY _rb (sample, prior_organism, prior_taxid, organism) FROM STDIN"
        ) as cp:
            for r in chunk:
                cp.write_row(r)
    cur.execute("ANALYZE _rb")
    cur.execute(
        "SELECT COUNT(*) FROM _rb rb JOIN ontology_samples_v4 s ON s.sample = rb.sample"
        " WHERE s.organism = rb.organism"
    )
    logger.info("revertible rows: %d", cur.fetchone()[0])
    if not apply:
        cur.execute(ROLLBACK_SQL)
        logger.info(
            "DRY RUN: UPDATE executed and rolled back, would revert %d rows",
            cur.rowcount,
        )
        pg.rollback()
        return
    cur.execute(ROLLBACK_SQL)
    logger.info("reverted %d rows", cur.rowcount)
    pg.commit()


def _load_stage(sq: sqlite3.Connection, cur: Cursor, select: str) -> None:
    rows = sq.execute(select)
    n = 0
    for chunk in _chunks(rows):
        with cur.copy("COPY _stage (sample, organism, taxid) FROM STDIN") as cp:
            for r in chunk:
                cp.write_row(r)
        n += len(chunk)
        if n % 500_000 == 0:
            logger.info("  staged %d into postgres temp", n)


def transfer_single_cell(
    sq: sqlite3.Connection, pg: Connection, apply: bool, keep_unstatused: bool = False
) -> None:
    cur = pg.cursor()
    cur.execute(
        "SELECT is_single_cell, COUNT(*) FROM unified_metadata"
        " GROUP BY 1 ORDER BY 2 DESC"
    )
    logger.info("postgres is_single_cell before: %s", cur.fetchall())

    rows = sq.execute(
        "SELECT study_accession, is_single_cell, single_cell_status,"
        "       single_cell_modality, single_cell_modality_evidence"
        "  FROM study_single_cell"
    ).fetchall()
    logger.info("staged studies: %d", len(rows))

    if apply:
        apply_schema(pg)
    else:
        # The status columns may not exist yet; a dry run must not depend on them.
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.columns"
            " WHERE table_name='unified_metadata' AND column_name='single_cell_status'"
        )
        if not cur.fetchone()[0]:
            logger.info("NOTE: single_cell_status does not exist yet; --apply adds it")

    cur.execute(
        "CREATE TEMP TABLE _sc (study_accession TEXT PRIMARY KEY,"
        " is_single_cell BOOLEAN,"
        " single_cell_status TEXT, single_cell_modality TEXT,"
        " single_cell_modality_evidence TEXT)"
    )
    with cur.copy(
        "COPY _sc (study_accession, is_single_cell, single_cell_status,"
        " single_cell_modality, single_cell_modality_evidence) FROM STDIN"
    ) as cp:
        for acc, sc, status, mod, mod_ev in rows:
            cp.write_row((acc, None if sc is None else bool(sc), status, mod, mod_ev))
    cur.execute("ANALYZE _sc")

    if keep_unstatused:
        # Flags set by the pre-status text rule (is_single_cell TRUE, status NULL)
        # are not demoted by a rebuild; they go to a separate reviewed pass.
        cur.execute(
            "CREATE TEMP TABLE _keep AS SELECT u.canonical_accession"
            "  FROM unified_metadata u"
            " WHERE u.is_single_cell AND u.single_cell_status IS NULL"
            "   AND NOT EXISTS (SELECT 1 FROM _sc sc"
            "                    WHERE sc.study_accession = u.canonical_accession"
            "                      AND sc.is_single_cell)"
        )
        logger.info(
            "--keep-unstatused: %d flagged studies left untouched", cur.rowcount
        )
        cur.execute(
            "DELETE FROM _sc WHERE study_accession IN (SELECT canonical_accession FROM _keep)"
        )
    else:
        cur.execute("CREATE TEMP TABLE _keep (canonical_accession TEXT)")

    cur.execute(
        "SELECT sc.single_cell_status, u.is_single_cell, COUNT(*)"
        "  FROM _sc sc"
        "  JOIN unified_metadata u ON u.canonical_accession = sc.study_accession"
        " GROUP BY 1, 2 ORDER BY 3 DESC"
    )
    logger.info("transition matrix (new status x current flag):")
    for status, old, n in cur.fetchall():
        logger.info("  %-42s %-6s %d", status, old, n)

    cur.execute(
        "SELECT COUNT(*) FROM unified_metadata u"
        " WHERE u.is_single_cell IS NOT NULL"
        "   AND NOT EXISTS (SELECT 1 FROM _sc sc"
        "                    WHERE sc.study_accession = u.canonical_accession)"
    )
    unscreened = cur.fetchone()[0]
    logger.info(
        "studies with NO evidence of any kind -> is_single_cell NULL,"
        " single_cell_status 'not_screened': %d",
        unscreened,
    )

    if not apply:
        logger.info("DRY RUN: nothing written")
        pg.rollback()
        return

    cur.execute(
        "UPDATE unified_metadata u"
        "   SET is_single_cell = sc.is_single_cell,"
        "       single_cell_status = sc.single_cell_status,"
        "       single_cell_modality = sc.single_cell_modality,"
        "       single_cell_modality_evidence = sc.single_cell_modality_evidence"
        "  FROM _sc sc WHERE sc.study_accession = u.canonical_accession"
        "   AND (u.is_single_cell IS DISTINCT FROM sc.is_single_cell"
        "     OR u.single_cell_status IS DISTINCT FROM sc.single_cell_status"
        "     OR u.single_cell_modality IS DISTINCT FROM sc.single_cell_modality)"
    )
    logger.info("studies updated from evidence: %d", cur.rowcount)

    # Everything with no evidence at all. These read FALSE only because the
    # sqlite column carries DEFAULT 0; nothing ever measured them.
    cur.execute(
        "UPDATE unified_metadata u"
        "   SET is_single_cell = NULL, single_cell_status = 'not_screened',"
        "       single_cell_modality = NULL, single_cell_modality_evidence = NULL"
        " WHERE NOT EXISTS (SELECT 1 FROM _sc sc"
        "                    WHERE sc.study_accession = u.canonical_accession)"
        "   AND NOT EXISTS (SELECT 1 FROM _keep k"
        "                    WHERE k.canonical_accession = u.canonical_accession)"
        "   AND (u.is_single_cell IS NOT NULL"
        "     OR u.single_cell_status IS DISTINCT FROM 'not_screened')"
    )
    logger.info("studies reset to not_screened: %d", cur.rowcount)
    pg.commit()

    cur.execute(
        "SELECT single_cell_status, COUNT(*) FROM unified_metadata"
        " GROUP BY 1 ORDER BY 2 DESC"
    )
    logger.info("postgres single_cell_status after: %s", cur.fetchall())


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", type=pathlib.Path, required=True)
    ap.add_argument(
        "--only",
        choices=("both", "organism", "organism-overwrite", "single-cell"),
        default="both",
    )
    ap.add_argument(
        "--snapshot-only",
        choices=("before", "after"),
        help="record postgres state into the staging db and exit; writes nothing",
    )
    ap.add_argument(
        "--rollback-organism",
        action="store_true",
        help="undo the organism corrections from the staged prior values",
    )
    ap.add_argument(
        "--apply", action="store_true", help="without this, nothing is written"
    )
    ap.add_argument(
        "--keep-unstatused",
        action="store_true",
        help="do not demote single-cell flags that carry no single_cell_status",
    )
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    sq = sqlite3.connect(args.db)
    pg = connect()
    if args.snapshot_only:
        snapshot(sq, pg, args.snapshot_only)
        if args.snapshot_only == "after":
            report_diff(sq)
        return 0
    if args.rollback_organism:
        rollback_organism(sq, pg, args.apply)
        return 0
    # A write always has a before-picture. Taking it here rather than leaving it
    # to the operator means there is no way to apply without one.
    if args.apply:
        snapshot(sq, pg, "before")
    if args.only in ("both", "organism"):
        transfer_organism(sq, pg, args.apply)
    if args.only in ("both", "organism", "organism-overwrite"):
        transfer_organism_overwrite(sq, pg, args.apply)
    if args.only in ("both", "single-cell"):
        transfer_single_cell(sq, pg, args.apply, args.keep_unstatused)
    if args.apply:
        snapshot(sq, pg, "after")
        report_diff(sq)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
