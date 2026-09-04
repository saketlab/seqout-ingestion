"""
Load the sweep's per-accession JSON into one SQLite file.

Staging keeps "measured, nothing found" apart from "never measured": a study
with no row was never attempted, while `sc_stats_study.state` records what
happened to one that was.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import TYPE_CHECKING, Any

from helpers import exec_query_from_file, sqlite_connect

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path

logger = logging.getLogger(__name__)

MATRIX_COLS = (
    "accession",
    "file",
    "sample_accession",
    "n_cells",
    "n_barcodes",
    "is_raw_barcode_space",
    "species",
    "species_method",
    "n_features",
    "n_mito_genes",
    "ncount_min",
    "ncount_mean",
    "ncount_median",
    "ncount_max",
    "nfeature_min",
    "nfeature_mean",
    "nfeature_median",
    "nfeature_max",
    "pct_mito_min",
    "pct_mito_mean",
    "pct_mito_median",
    "pct_mito_max",
    "min_counts",
    "n_cells_filtered",
    "ncount_f_min",
    "ncount_f_mean",
    "ncount_f_median",
    "ncount_f_max",
    "nfeature_f_min",
    "nfeature_f_mean",
    "nfeature_f_median",
    "nfeature_f_max",
    "pct_mito_f_min",
    "pct_mito_f_mean",
    "pct_mito_f_median",
    "pct_mito_f_max",
)

# Columns come from MATRIX_COLS above, never from input.
_UPSERT_MATRIX = (
    f"INSERT INTO sc_stats_matrix ({','.join(MATRIX_COLS)})"  # noqa: S608
    f" VALUES ({','.join('?' * len(MATRIX_COLS))})"
    " ON CONFLICT(accession, file) DO UPDATE SET "
    + ",".join(f"{c}=excluded.{c}" for c in MATRIX_COLS[2:])
)

_COMMIT_EVERY = 2000

# The transfer sends these; the rollup columns are filled after load, so they
# are not in _UPSERT_STUDY's insert list.
STUDY_COLS = (
    "accession",
    "state",
    "detail",
    "n_matrices",
    "n_failures",
    "failures_json",
    "species",
    "any_raw_barcode_space",
)

_UPSERT_STUDY = """
INSERT INTO sc_stats_study
    (accession, state, detail, n_matrices, n_failures, failures_json)
VALUES (?,?,?,?,?,?)
ON CONFLICT(accession) DO UPDATE SET
    state         = COALESCE(excluded.state, sc_stats_study.state),
    detail        = COALESCE(excluded.detail, sc_stats_study.detail),
    n_matrices    = excluded.n_matrices,
    n_failures    = excluded.n_failures,
    failures_json = excluded.failures_json
"""


_GSM = re.compile(r"^(GSM\d+)")


def _sample_accession(file: str | None) -> str | None:
    """
    GSM off the front of the filename, the join key to pentimento_sample.

    Series-scope files (a bare `matrix.mtx`) carry no sample identity, so they
    get NULL rather than a guess.

    The other side of that join is derived the same way, by `_sample_key` in
    sc_enrich/deep/yuj_worker.py, and the two do NOT match: it searches anywhere
    in the basename and falls back to the study accession, this one anchors and
    falls back to NULL. Both differences are deliberate, because a wrong key
    joins to the wrong sample while a NULL merely fails to join.

    Parsing a filename is not the authority on which sample owns a file; GEO's
    own `geo_series.samples_ref` is. Every pair this produces is currently
    confirmed by it, and that is worth re-checking after a sweep rather than
    assumed:

        SELECT count(*) FROM sc_stats_matrix m
          JOIN geo_series g ON g.accession = m.accession
         WHERE m.sample_accession IS NOT NULL
           AND NOT (m.sample_accession = ANY(g.samples_ref));

    A non-zero answer means the parse invented an edge GEO does not declare.
    """
    if not file:
        return None
    m = _GSM.match(file)
    return m.group(1) if m else None


def _quad(matrix: dict[str, Any], key: str) -> tuple[Any, Any, Any, Any]:
    """min/mean/median/max for one metric, or four NULLs when it is absent."""
    block = matrix.get(key) or {}
    return (block.get("min"), block.get("mean"), block.get("median"), block.get("max"))


def _matrix_row(accession: str, matrix: dict[str, Any]) -> tuple[Any, ...]:
    return (
        accession,
        matrix.get("file"),
        _sample_accession(matrix.get("file")),
        matrix.get("n_cells"),
        matrix.get("n_barcodes"),
        int(bool(matrix.get("is_raw_barcode_space"))),
        matrix.get("species"),
        matrix.get("species_method"),
        # absent from every result written before 2026-08-30; NULL there means
        # "not measured", not "no features"
        matrix.get("n_features"),
        matrix.get("n_mito_genes"),
        *_quad(matrix, "nCount_RNA"),
        *_quad(matrix, "nFeature_RNA"),
        *_quad(matrix, "pct_mito"),
        matrix.get("min_counts"),
        matrix.get("n_cells_filtered"),
        *_quad(matrix, "nCount_RNA_filtered"),
        *_quad(matrix, "nFeature_RNA_filtered"),
        *_quad(matrix, "pct_mito_filtered"),
    )


def _status(results_dir: Path, accession: str) -> tuple[str | None, str | None]:
    """
    Read the `.status` sidecar: first line is the state, the rest is detail.

    No existence probe: read_text raises for a missing file and the caller wants
    the same answer either way. The results dirs live on NFS often enough that a
    stat per accession is the difference between milliseconds and a minute.
    """
    try:
        lines = (results_dir / f"{accession}.status").read_text().splitlines()
    except OSError:
        return (None, None)
    state = lines[0].strip() if lines else None
    detail = "\n".join(lines[1:]).strip() or None
    return (state or None, detail)


def _entries(results_dir: Path) -> tuple[list[str], set[str]]:
    """
    Split a results dir into measured accessions and status-only ones.

    One scandir instead of two globs and a stat per file. On the NFS-backed
    results dirs the stat-per-file form dominated the whole ingest.
    """
    stats, statuses = [], set()
    with os.scandir(results_dir) as it:
        for entry in it:
            name = entry.name
            if name.endswith(".stats.json"):
                stats.append(name.removesuffix(".stats.json"))
            elif name.endswith(".status"):
                statuses.add(name.removesuffix(".status"))
    stats.sort()
    return stats, statuses - set(stats)


def ingest_dirs(db_file: Path, results_dirs: list[Path]) -> dict[str, int]:
    """
    Load every `<accession>.stats.json` under the given dirs into `db_file`.

    Later directories win on conflict, so pass the most recent pull last.
    """
    db_file.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite_connect(db_file)
    exec_query_from_file("tables/sc_stats.sql", conn=conn)

    counts = {"studies": 0, "matrices": 0, "unreadable": 0, "status_only": 0}
    for results_dir in results_dirs:
        if not results_dir.is_dir():
            logger.warning("results dir missing, skipped: %s", results_dir)
            continue
        measured, status_only = _entries(results_dir)
        for accession in measured:
            path = results_dir / f"{accession}.stats.json"
            try:
                payload = json.loads(path.read_text())
            except (OSError, ValueError):
                # A truncated write is not a study with no matrices; leave it
                # absent so the sweep can be asked for it again.
                counts["unreadable"] += 1
                logger.warning("unreadable, left unstaged: %s", path)
                continue

            matrices = payload.get("matrices") or []
            failures = payload.get("failures") or []
            state, detail = _status(results_dir, accession)
            conn.execute(
                _UPSERT_STUDY,
                (
                    accession,
                    payload.get("state") or state,
                    detail,
                    len(matrices),
                    len(failures),
                    json.dumps(failures) if failures else None,
                ),
            )
            if matrices:
                conn.executemany(
                    _UPSERT_MATRIX, [_matrix_row(accession, m) for m in matrices]
                )
            counts["studies"] += 1
            counts["matrices"] += len(matrices)
            if counts["studies"] % _COMMIT_EVERY == 0:
                conn.commit()
                logger.info("staged %d studies", counts["studies"])

        counts["status_only"] += _stage_status_only(conn, results_dir, status_only)

    counts["studies_rolled_up"] = rollup_studies(conn)
    conn.commit()
    return counts


def rollup_studies(conn: sqlite3.Connection) -> int:
    """
    Derive the per-study fields that have no counterpart in pentimento.

    Cell and gene totals are deliberately absent: pentimento_study.study_cells
    and pentimento_sample.cells/genes already hold them, and a second copy here
    would be a second source to reconcile. Join through
    sc_stats_matrix.sample_accession instead.
    """
    conn.execute(
        "UPDATE sc_stats_study SET"
        "  species = (SELECT m.species FROM sc_stats_matrix m"
        "              WHERE m.accession = sc_stats_study.accession"
        "                AND m.species IS NOT NULL"
        "              GROUP BY m.species ORDER BY COUNT(*) DESC LIMIT 1),"
        "  any_raw_barcode_space = (SELECT MAX(m.is_raw_barcode_space)"
        "                             FROM sc_stats_matrix m"
        "                            WHERE m.accession = sc_stats_study.accession)"
    )
    return conn.execute(
        "SELECT COUNT(*) FROM sc_stats_study WHERE any_raw_barcode_space IS NOT NULL"
    ).fetchone()[0]


def _stage_status_only(
    conn: sqlite3.Connection, results_dir: Path, accessions: set[str]
) -> int:
    """
    Stage studies that produced only a `.status`.

    `no_matrix` and `fetch_failed` write no JSON, and they are the answer for a
    large minority of the corpus. Dropping them would leave those studies
    indistinguishable from ones the sweep never reached.
    """
    for accession in sorted(accessions):
        state, detail = _status(results_dir, accession)
        conn.execute(_UPSERT_STUDY, (accession, state, detail, 0, 0, None))
    return len(accessions)
