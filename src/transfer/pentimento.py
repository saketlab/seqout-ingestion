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
from transfer.pg_fields import non_nulling_update, overwrite_update

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
    # from the rollup's list, so added columns travel to Postgres
    *(c for c, _, _ in PICKED_COLUMNS),
    "calls_ambiguous",
    "flags",
    "n_flags",
    "hpv_top_type",
    "hpv_ambiguous",
    "has_viral_reads",
    "has_viral_evidence",
    "has_bacterial_reads",
    "viral_kmer_mass",
    "bacterial_kmer_mass",
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
    "assay_confidence",
    "assay_onlist_pct",
    "modality",
    "modality_confidence",
    "bulk_vs_sc",
    "read_end",
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

_MICROBE_COLS = [
    "run_accession",
    "organism",
    "class",
    "reads",
    "kmer_mass",
    "n_unitigs",
    "ref_bp",
    "covered_bp",
    "breadth_frac",
    "is_background",
    "is_validated_viral",
    "is_viral_evidence",
    "is_validated_bacterial",
]


def _flag_json(row: dict[str, Any]) -> str | None:
    """
    Tripped flag names, or None when measurements are unavailable.

    None means indeterminate; '[]' means measured with zero tripped flags.
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
    *,
    non_nulling: bool = False,
) -> int:
    """
    Idempotent load with overwrite_update and dead-tuple guard.

    Pentimento is rebuilt whole, so NULL is a fact in tri-state fields.
    non_nulling=True is for incomplete builds and must be followed by a full
    transfer so real NULLs can clear stale values.
    """
    keys = {c.strip() for c in conflict.split(",")}
    payload = [c for c in cols if c not in keys]
    if payload and non_nulling:
        # Reuse the archive-side builder for COALESCE and the dead-tuple guard.
        set_clause = non_nulling_update(table, payload)
        action = f"DO UPDATE {set_clause}" if set_clause else "DO NOTHING"
    elif payload:
        # same builder the GARD transfer uses; see pg_fields.overwrite_update
        action = f"DO UPDATE {overwrite_update(table, payload)}"
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


# SQLite integer columns converted to Postgres booleans. Other values pass
# through _txt, so new text columns get NUL stripping; handwritten tuples once
# leaked NUL bytes on nine of the twenty-one run columns.
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
        "is_validated_viral",
        "is_viral_evidence",
        "is_validated_bacterial",
        "has_viral_reads",
        "has_viral_evidence",
        "has_bacterial_reads",
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
    Build one parameter tuple addressed by column name.

    Name-addressing keeps _COLS edits from shifting positional values into the
    wrong table columns.
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
    *,
    non_nulling: bool = False,
) -> int:
    return _upsert(
        conn,
        table,
        cols,
        (_row(r, cols) for r in sconn.execute(sql)),
        conflict,
        batch_size,
        non_nulling=non_nulling,
    )


def _dry_run_table(
    conn: psycopg.Connection,
    sconn: Any,
    table: str,
    cols: list[str],
    sql: str,
    conflict: str,
) -> dict[str, Any]:
    """
    Report what this load would change in dry-run mode.

    Uses the same `_row` tuples as `_load`. The wipe metric tracks existing
    Postgres values this build would replace with NULL.
    """
    keys = [c.strip() for c in conflict.split(",")]
    payload = [c for c in cols if c not in keys]
    key_idx = [cols.index(k) for k in keys]
    pay_idx = [cols.index(c) for c in payload]

    # pg side, streamed through a named (server-side) cursor: pentimento_run is
    # large, and a client-side fetch would be held entirely in memory
    # twice over
    pg_null: dict[tuple, int] = {}
    quoted = ", ".join(keys + payload)
    with conn.cursor(name=f"dry_{table}") as cur:
        cur.itersize = 20_000
        cur.execute(f"SELECT {quoted} FROM {table}")  # noqa: S608
        nk = len(keys)
        for row in cur:
            mask = 0
            for i, value in enumerate(row[nk:]):
                if value is None:
                    mask |= 1 << i
            pg_null[tuple(str(v) for v in row[:nk])] = mask

    would_null = dict.fromkeys(payload, 0)
    would_fill = dict.fromkeys(payload, 0)
    inserts = matched = 0

    for record in sconn.execute(sql):
        row = _row(record, cols)
        key = tuple(str(row[i]) for i in key_idx)
        before = pg_null.get(key)
        if before is None:
            inserts += 1
            continue
        matched += 1
        for bit, (name, idx) in enumerate(zip(payload, pay_idx, strict=True)):
            pg_is_null = bool(before & (1 << bit))
            new_is_null = row[idx] is None
            if new_is_null and not pg_is_null:
                would_null[name] += 1
            elif pg_is_null and not new_is_null:
                would_fill[name] += 1

    wipes = {c: n for c, n in would_null.items() if n}
    fills = {c: n for c, n in would_fill.items() if n}
    logger.info(
        "%s: %d rows in pg, %d matched, %d new; columns filled %s; columns BLANKED %s",
        table,
        len(pg_null),
        matched,
        inserts,
        json.dumps(fills, sort_keys=True) if fills else "{}",
        json.dumps(wipes, sort_keys=True) if wipes else "{} (none)",
    )
    return {
        "table": table,
        "pg_rows": len(pg_null),
        "matched": matched,
        "inserts": inserts,
        "filled": fills,
        "blanked": wipes,
    }


def pentimento_transfer(
    pg_url: str,
    pentimento_db_file: Path,
    batch_size: int = BATCH_SIZE,
    *,
    non_nulling: bool = False,
    dry_run: bool = False,
    tables: list[str] | None = None,
) -> list[dict[str, Any]] | None:
    """
    Load Pentimento tables into Postgres.

    `non_nulling` forwards the _upsert escape hatch: add values while preserving
    existing Postgres data for incomplete builds.
    """
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
        # background taxa are excluded: E. coli across unrelated runs is reagent
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
    )
    if tables:
        # Tables are independent upserts. A build can be authoritative for one
        # table while behind Postgres on another, so load only what it owns.
        known = {t for t, _, _, _ in loads}
        unknown = sorted(set(tables) - known)
        if unknown:
            msg = f"unknown table(s) {unknown}; known: {sorted(known)}"
            raise ValueError(msg)
        loads = tuple(ld for ld in loads if ld[0] in set(tables))
        logger.info("restricted to %s", sorted(t for t, _, _, _ in loads))

    with psycopg.connect(pg_url) as conn:
        if dry_run:
            # Dry-run mode leaves schema and DDL untouched.
            with sqlite_connect(pentimento_db_file) as sconn:
                sconn.row_factory = _dict_factory
                report = [
                    _dry_run_table(conn, sconn, table, cols, sql, conflict)
                    for table, cols, sql, conflict in loads
                ]
            conn.rollback()
            blanked = sum(sum(r["blanked"].values()) for r in report)
            logger.info(
                "DRY RUN: nothing written. %d cell(s) would be blanked, %d filled",
                blanked,
                sum(sum(r["filled"].values()) for r in report),
            )
            return report
        exec_query_from_file("tables/pentimento_pg.sql", conn=conn)
        conn.commit()
        with sqlite_connect(pentimento_db_file) as sconn:
            sconn.row_factory = _dict_factory
            for table, cols, sql, conflict in loads:
                _load(
                    conn,
                    sconn,
                    table,
                    cols,
                    sql,
                    conflict,
                    batch_size,
                    non_nulling=non_nulling,
                )
    return None


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
