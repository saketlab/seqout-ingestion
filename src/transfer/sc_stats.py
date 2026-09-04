"""
Transfer the sc_stats sweep into Postgres.

Non-nulling by default. The sweep is delivered incrementally by the fleet, so a
staging file always describes part of the corpus, and a bare overwrite would
blank whatever an earlier, larger pass had already loaded.

Cells and genes are not carried. pentimento_sample already holds both from the
counting job, and sc_stats_matrix.sample_accession is the join key to it.
"""

from __future__ import annotations

import logging
from itertools import batched
from typing import TYPE_CHECKING, Any, LiteralString, cast

import psycopg

from helpers import exec_query_from_file, sqlite_connect
from sc_stats.ingest import MATRIX_COLS, STUDY_COLS
from transfer.pg_fields import non_nulling_update, overwrite_update

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

logger = logging.getLogger(__name__)

BATCH = 5000

_TABLES = {
    "sc_stats_matrix": (MATRIX_COLS, ("accession", "file")),
    "sc_stats_study": (STUDY_COLS, ("accession",)),
}

# INTEGER 0/1 in the staging SQLite, BOOLEAN in Postgres.
_BOOL_COLS = frozenset({"is_raw_barcode_space", "any_raw_barcode_space"})


def _upsert(
    table: str, cols: tuple[str, ...], key: tuple[str, ...], *, overwrite: bool
) -> LiteralString:
    payload = [c for c in cols if c not in key]
    clause = (
        overwrite_update(table, payload)
        if overwrite
        else non_nulling_update(table, payload)
    )
    return cast(
        "LiteralString",
        f"INSERT INTO {table} ({', '.join(cols)}) "  # noqa: S608 - constants above
        f"VALUES ({', '.join(['%s'] * len(cols))}) "
        f"ON CONFLICT ({', '.join(key)}) DO UPDATE {clause}",
    )


def _clean(value: Any, *, is_bool: bool) -> Any:
    """
    Widen SQLite's 0/1 into a real boolean, and strip embedded NULs.

    Postgres rejects a NUL byte and SQLite tolerates it, and one anywhere aborts
    the whole batch. `detail` comes from `.status` files written by fleet hosts,
    so the transfer refuses to assume they are clean.
    """
    if is_bool:
        return None if value is None else bool(value)
    return value.replace("\x00", "") if isinstance(value, str) else value


def _rows(conn: Any, table: str, cols: tuple[str, ...]) -> Iterator[tuple[Any, ...]]:
    flags = [c in _BOOL_COLS for c in cols]
    query = f"SELECT {', '.join(cols)} FROM {table}"  # noqa: S608 - constants above
    for row in conn.execute(query):
        yield tuple(_clean(v, is_bool=f) for v, f in zip(row, flags, strict=True))


def _count(cur: Any, table: str) -> int:
    """Row count for one of this module's literal table names."""
    return cur.execute(
        cast("LiteralString", f"SELECT count(*) FROM {table}")  # noqa: S608
    ).fetchone()[0]


def sc_stats_transfer(
    pg_url: str,
    db_file: Path,
    *,
    overwrite: bool = False,
    is_dry_run: bool = False,
) -> dict[str, int]:
    """
    Load sc_stats_matrix and sc_stats_study into Postgres.

    A dry run still applies the DDL: `exec_query_from_file` commits, so the two
    tables exist afterwards even though no rows were written. That is harmless
    on a rerun but means a schema change needs the empty tables dropped first.
    """
    counts: dict[str, int] = {}
    sqlite = sqlite_connect(db_file)
    try:
        staged = {t: _count(sqlite, t) for t in _TABLES}
        with psycopg.connect(pg_url) as conn, conn.cursor() as cur:
            exec_query_from_file("tables/sc_stats_pg.sql", conn=conn)
            before = {t: _count(cur, t) for t in _TABLES}

            if not is_dry_run:
                for table, (cols, key) in _TABLES.items():
                    sql = _upsert(table, cols, key, overwrite=overwrite)
                    rows = _rows(sqlite, table, cols)
                    for chunk in batched(rows, BATCH, strict=False):
                        cur.executemany(sql, chunk)
                conn.commit()

            for table in _TABLES:
                held = before[table] if is_dry_run else _count(cur, table)
                logger.info(
                    "%s: %d rows in pg, %d staged, now %d%s",
                    table,
                    before[table],
                    staged[table],
                    held,
                    " (dry run wrote nothing)" if is_dry_run else "",
                )
                counts[f"{table}_staged"] = staged[table]
                counts[f"{table}_held"] = held
    finally:
        sqlite.close()
    return counts
