"""Transfer the Pentimento SQLite into the pentimento_* Postgres tables."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, LiteralString, cast

import psycopg
from tqdm import tqdm

from helpers import exec_query_from_file, sqlite_connect
from pentimento.flags import FLAG_PANEL
from pentimento.rollup import PICKED_COLUMNS

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

logger = logging.getLogger(__name__)

BATCH_SIZE = 5_000

_FLAGS = [f for f, _, _, _ in FLAG_PANEL]

_STUDY_COLS = [
    "study_accession",
    "source",
    "title",
    "aliases",
    "study_cells",
    "cells_unfiltered",
    "single_cell_file_format",
    "n_samples_reported",
    "n_samples_detailed",
    "sample_cells_total",
    "unassigned_cells",
    "sample_breakdown_complete",
    "any_unfiltered",
    "n_runs_linked",
    "n_runs_preflightx",
    "n_runs_measurable",
    "has_metadata",
    "has_celltype",
    "has_donor",
    "has_demographics",
    "flags",
]

_SAMPLE_COLS = [
    "study_accession",
    "sample_accession",
    "cells",
    "genes",
    "nnz",
    "file_format",
    "unfiltered",
    "n_runs",
    "n_runs_measurable",
    # taken from the rollup's own list: this is the projection of
    # pentimento_sample, so a column added there reaches Postgres without a
    # second list here going stale
    *(c for c, _, _ in PICKED_COLUMNS),
    "calls_ambiguous",
    "flags",
    "n_flags",
    "hpv_top_type",
    "hpv_ambiguous",
]

_RUN_COLS = [
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
    "assay_is_single_cell",
    "preflightx_source",
    "library_layout",
    "fastq_ftp",
    "fastq_bytes",
    "quant_status",
    "total_reads",
    "measurable",
    "flags",
    "n_flags",
]

_BENCHMARK_COLS = [
    "accession",
    "experiment",
    "field",
    "gt_raw",
    "pred_raw",
    "gt_norm",
    "pred_norm",
    "correct",
]

_MICROBE_COLS = [
    "run_accession",
    "organism",
    "class",
    "reads",
    "ref_bp",
    "covered_bp",
    "breadth_frac",
    "is_background",
]


def _flag_json(row: dict[str, Any]) -> str | None:
    """
    Tripped flag names, or None when nothing was measurable.

    None and '[]' are different answers and must stay different: None is "we
    could not tell", '[]' is "we looked and found nothing".
    """
    if row.get(_FLAGS[0]) is None:
        return None
    return json.dumps([f for f in _FLAGS if row.get(f)])


def _dict_factory(cursor: Any, row: tuple) -> dict[str, Any]:
    return {c[0]: v for c, v in zip(cursor.description, row, strict=True)}


def _upsert(
    conn: psycopg.Connection,
    table: str,
    cols: list[str],
    rows: Iterator[tuple],
    conflict: str,
    batch_size: int,
) -> int:
    """
    Idempotent load. A rerun over unchanged data is a no-op.

    Deliberately NOT `pg_fields.non_nulling_update`, which every other transfer
    uses. That helper COALESCEs so an incoming NULL never overwrites -- correct
    for the incremental archive sources, where ingest writes a fresh per-window
    sqlite and a re-transfer must not erase what a later backfill filled in.
    Pentimento is the opposite: it is rebuilt whole and IS the truth, so a NULL is
    a fact (the tri-state "not measurable"), and COALESCE would both resurrect
    corruption the sanitiser just removed and pin a stale 0 in place of it.

    The `IS DISTINCT FROM` guard IS kept: Postgres cannot tell that a row is
    unchanged, so without it every conflicting row is rewritten on every
    re-transfer, leaving dead tuples across the table.
    """
    keys = {c.strip() for c in conflict.split(",")}
    payload = [c for c in cols if c not in keys]
    if payload:
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in payload)
        guard = " OR ".join(
            f"{table}.{c} IS DISTINCT FROM EXCLUDED.{c}" for c in payload
        )
        action = f"DO UPDATE SET {updates} WHERE {guard}"
    else:
        action = "DO NOTHING"
    query = (
        f"INSERT INTO {table} ({', '.join(cols)}) "  # noqa: S608
        f"VALUES ({', '.join(['%s'] * len(cols))}) "
        f"ON CONFLICT ({conflict}) {action}"
    )
    total = 0
    batch: list[tuple] = []
    with tqdm(desc=table, unit="row", leave=False) as pbar:
        for row in rows:
            batch.append(row)
            if len(batch) >= batch_size:
                total += _flush(conn, query, batch)
                pbar.update(len(batch))
                batch = []
        if batch:
            total += _flush(conn, query, batch)
            pbar.update(len(batch))
    logger.info("%s: %d rows", table, total)
    return total


def _flush(conn: psycopg.Connection, query: str, batch: list[tuple]) -> int:
    with conn.cursor() as cur:
        cur.executemany(cast("LiteralString", query), batch)
    conn.commit()
    return len(batch)


# columns whose SQLite integer must become a Postgres boolean. Everything else
# passes through _txt, a no-op on non-strings, so a new text column cannot be
# forgotten; hand-written tuples once let NUL bytes through on nine of the
# twenty-one run columns.
_BOOLEAN_COLS = frozenset(
    {
        "cells_unfiltered",
        "sample_breakdown_complete",
        "any_unfiltered",
        "has_metadata",
        "has_celltype",
        "has_donor",
        "has_demographics",
        "unfiltered",
        "calls_ambiguous",
        "is_background",
        "assay_is_single_cell",
        "hpv_ambiguous",
        "species_mislabel",
        "species_ambiguous",
        "sex_mixed_suspected",
        "measurable",
        "correct",
    },
)


def _row(record: dict[str, Any], cols: list[str]) -> tuple:
    """
    Build one parameter tuple, addressed BY NAME.

    The previous form hand-wrote a positional tuple literal per table -- ~95
    lines in which nothing tied the i-th value to the i-th entry of the _COLS
    list. Inserting a column in the middle of a _COLS list shifted every value
    after it, and psycopg would happily have written `title` into `aliases`
    wherever the types allowed.
    """
    out: list[Any] = []
    for col in cols:
        if col == "flags":
            out.append(_flag_json(record))
        elif col in _BOOLEAN_COLS:
            out.append(_tri(record.get(col)))
        else:
            out.append(_txt(record.get(col)))
    return tuple(out)


def _load(
    conn: psycopg.Connection,
    sconn: Any,
    table: str,
    cols: list[str],
    sql: str,
    conflict: str,
    batch_size: int,
) -> int:
    return _upsert(
        conn,
        table,
        cols,
        (_row(r, cols) for r in sconn.execute(sql)),
        conflict,
        batch_size,
    )


def pentimento_transfer(
    pg_url: str,
    pentimento_db_file: Path,
    batch_size: int = BATCH_SIZE,
) -> None:
    """Load pentimento_study / _sample / _run / _microbe / _flag_panel into pg."""
    # outer join so unquantified runs still ship with their fastq link
    run_sql = (
        "SELECT p.*, f.status AS quant_status, f.total_reads, "  # noqa: S608
        "       f.measurable, f.n_flags, "
        + ", ".join(f"f.{f}" for f in _FLAGS)
        + " FROM run_preflightx p "
        "LEFT JOIN run_microbe_flags f USING (run_accession)"
    )
    loads = (
        (
            "pentimento_study",
            _STUDY_COLS,
            "SELECT * FROM pentimento_study",
            "study_accession",
        ),
        (
            "pentimento_sample",
            _SAMPLE_COLS,
            "SELECT * FROM pentimento_sample",
            "study_accession, sample_accession",
        ),
        ("pentimento_run", _RUN_COLS, run_sql, "run_accession"),
        # background taxa are excluded: E. coli in 69% of runs is reagent
        # contamination and would dominate any "why is this positive" panel
        (
            "pentimento_microbe",
            _MICROBE_COLS,
            "SELECT * FROM microbe_quant WHERE is_background = 0",
            "run_accession, organism",
        ),
        (
            "pentimento_flag_panel",
            ["flag", "organism", "gate"],
            "SELECT * FROM microbe_flag_panel",
            "flag, organism",
        ),
        (
            "pentimento_benchmark",
            _BENCHMARK_COLS,
            "SELECT * FROM preflightx_benchmark",
            "accession, experiment, field",
        ),
    )
    with psycopg.connect(pg_url) as conn:
        exec_query_from_file("tables/pentimento_pg.sql", conn=conn)
        conn.commit()
        with sqlite_connect(pentimento_db_file) as sconn:
            sconn.row_factory = _dict_factory
            for table, cols, sql, conflict in loads:
                _load(conn, sconn, table, cols, sql, conflict, batch_size)


def _tri(value: Any) -> bool | None:
    """SQLite int -> nullable Postgres boolean, preserving NULL."""
    return None if value is None else bool(value)


def _txt(value: Any) -> Any:
    """
    Strip embedded NULs, which Postgres rejects and SQLite tolerates.

    The Pentimento builder sanitises these at source, but a single NUL anywhere
    aborts the whole batch and would take a production load down, so the
    transfer refuses to depend on that having been run.
    """
    return value.replace("\x00", "") if isinstance(value, str) else value
