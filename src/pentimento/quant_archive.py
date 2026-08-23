"""
Durable SQLite archive for quantifier output files.

`pentimento build` reads this archive before visible CSVs, so rollups survive file
pruning. `is_*` flags are recomputed from whole-corpus prevalence.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from helpers import batch_query_in, exec_query_from_file, sqlite_connect
from pentimento import sources

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path

logger = logging.getLogger(__name__)

#: Tables built from quantifier output files.
TABLES = ("microbe_quant", "quant_provenance")


def _count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]  # noqa: S608


def _columns(conn: sqlite3.Connection, table: str, schema: str = "main") -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA {schema}.table_info({table})")]


def _shared_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    """Columns the archive and the build in progress both have, in build order."""
    have = set(_columns(conn, table, "quant_arch"))
    return [c for c in _columns(conn, table) if c in have]


def runs_in_dir(quant_dir: Path) -> list[str]:
    """List supplied runs by filename."""
    if not quant_dir.is_dir():
        return []
    return [p.name.removesuffix(".quant.csv") for p in quant_dir.rglob("*.quant.csv")]


def clear_runs(conn: sqlite3.Connection, runs: list[str]) -> int:
    """
    Drop microbe_quant rows for runs about to be reloaded.

    Deleting the run first makes a rescan authoritative; quant_provenance is
    run-keyed and uses REPLACE.
    """
    if not runs:
        return 0
    before = _count(conn, "microbe_quant")
    batch_query_in(conn, "DELETE FROM microbe_quant WHERE run_accession", runs, 999)
    conn.commit()
    removed = before - _count(conn, "microbe_quant")
    if removed:
        logger.info(
            "cleared %d stale microbe_quant rows for %d runs", removed, len(runs)
        )
    return removed


def archive_quant(quant_dir: Path, archive_db_file: Path) -> dict[str, int]:
    """
    Fold a directory of quantifier output into the archive. Idempotent.

    Returns fold deltas for archive tables.
    """
    from pentimento.build import _executemany

    archive_db_file.parent.mkdir(parents=True, exist_ok=True)
    # Use the full pentimento schema so archive tables track build tables; extra
    # empty tables cost nothing.
    exec_query_from_file("tables/pentimento.sql", db_file=archive_db_file)

    with sqlite_connect(archive_db_file) as conn:
        before = {t: _count(conn, t) for t in TABLES}

        logger.info("archiving microbial quantification from %s", quant_dir)
        # authoritative per run: see clear_runs
        clear_runs(conn, runs_in_dir(quant_dir))
        _executemany(conn, "microbe_quant", sources.load_microbe_quant(quant_dir))
        _executemany(conn, "quant_provenance", sources.load_quant_provenance(quant_dir))
        # Attempted unmeasured runs stay distinct from negative results.
        known = {
            r[0] for r in conn.execute("SELECT run_accession FROM quant_provenance")
        }
        _executemany(
            conn, "quant_provenance", sources.load_status_only(quant_dir, known)
        )
        conn.commit()

        after = {t: _count(conn, t) for t in TABLES}

    for table in TABLES:
        logger.info(
            "archive %s: %d rows (+%d)",
            table,
            after[table],
            after[table] - before[table],
        )
    return after


def load_archive(conn: sqlite3.Connection, archive_db_file: Path) -> dict[str, int]:
    """
    Copy the archive's quant tables into a build in progress.

    Applied BEFORE any --quant-dir load so that a directory holding a newer
    scan of the same run wins on conflict; the archive is history, the directory
    is what just came off the fleet.
    """
    if not archive_db_file.exists():
        logger.warning("quant archive missing, skipping: %s", archive_db_file)
        return {}

    conn.execute("ATTACH DATABASE ? AS quant_arch", (str(archive_db_file),))
    counts: dict[str, int] = {}
    try:
        for table in TABLES:
            # Columns are named and intersected. The archive can outlive schema changes,
            # column-order changes, and newly added columns; positional copying silently
            # shifts values.
            cols = _shared_columns(conn, table)
            if not cols:
                logger.warning("quant archive has no usable %s columns", table)
                continue
            names = ", ".join(cols)
            conn.execute(
                f"INSERT OR REPLACE INTO {table} ({names}) "  # noqa: S608
                f"SELECT {names} FROM quant_arch.{table}"
            )
            counts[table] = _count(conn, table)
            logger.info("from quant archive, %s: %d rows", table, counts[table])
        conn.commit()
    finally:
        conn.execute("DETACH DATABASE quant_arch")
    return counts


def verify_archived(quant_dir: Path, archive_db_file: Path) -> tuple[int, list[str]]:
    """
    Check every run in `quant_dir` is present in the archive.

    Precondition for deleting files. Returns (n_checked, missing runs); missing
    runs mean keep the files. `quant_provenance` records every attempted run,
    including failures with a .status and no .quant.csv.
    """
    runs: set[str] = set()
    for pattern, suffix in ((".quant.csv", ".quant.csv"), (".status", ".status")):
        runs |= {p.name.removesuffix(suffix) for p in quant_dir.rglob(f"*{pattern}")}

    with sqlite_connect(archive_db_file) as conn:
        archived = {
            r[0] for r in conn.execute("SELECT run_accession FROM quant_provenance")
        }
    missing = sorted(runs - archived)
    logger.info(
        "verify: %d runs on disk, %d in archive, %d missing",
        len(runs),
        len(runs & archived),
        len(missing),
    )
    return len(runs), missing


def prune_archived(
    quant_dir: Path, archive_db_file: Path, newer_than: float | None = None
) -> tuple[int, int]:
    """
    Delete files whose run is still provably archived.

    Re-check membership per file; fold timestamp protects same-name rescans.
    """
    with sqlite_connect(archive_db_file) as conn:
        archived = {
            r[0] for r in conn.execute("SELECT run_accession FROM quant_provenance")
        }

    deleted = skipped = 0
    for suffix in (".quant.csv", ".status"):
        for path in quant_dir.rglob(f"*{suffix}"):
            fresh = newer_than is not None and path.stat().st_mtime > newer_than
            if path.name.removesuffix(suffix) in archived and not fresh:
                path.unlink()
                deleted += 1
            else:
                skipped += 1
    logger.info(
        "pruned %d files from %s; left %d unarchived", deleted, quant_dir, skipped
    )
    return deleted, skipped
