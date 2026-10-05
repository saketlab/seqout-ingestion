"""
Build a SQLite delta containing rows absent from Postgres.

Compare keys before copying payloads to avoid transferring existing rows.
Use the transfer task table's conflict keys and preserve source table names
and DDL. Copy tables without conflict keys in full.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import TYPE_CHECKING

import psycopg

if TYPE_CHECKING:
    from pathlib import Path

from transfer.postgres import build_table_info

logger = logging.getLogger(__name__)

_COPY_CHUNK = 20_000


def _new_keys(
    pg: psycopg.Connection,
    src: sqlite3.Connection,
    sqlite_table: str,
    pg_table: str,
    key: str,
) -> tuple[set[str], int]:
    """Keys present in the staged table and absent from Postgres."""
    with pg.cursor() as cur:
        cur.execute("SET statement_timeout='0'")
        cur.execute("DROP TABLE IF EXISTS _delta_keys")
        cur.execute("CREATE TEMP TABLE _delta_keys(k text primary key)")
        seen: set[str] = set()
        with cur.copy("COPY _delta_keys(k) FROM STDIN") as cp:
            for (value,) in src.execute(f"SELECT {key} FROM {sqlite_table}"):  # noqa: S608
                if value is not None and value not in seen:
                    seen.add(value)
                    cp.write_row((value,))
        cur.execute("ANALYZE _delta_keys")
        cur.execute(
            f"SELECT k FROM _delta_keys WHERE NOT EXISTS "  # noqa: S608
            f"(SELECT 1 FROM {pg_table} t WHERE t.{key} = _delta_keys.k)",
        )
        new = {r[0] for r in cur.fetchall()}
        cur.execute("DROP TABLE _delta_keys")
    return new, len(seen)


def _copy_rows(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    table: str,
    keep: set[str] | None,
    key_index: int,
) -> int:
    cols = [r[1] for r in src.execute(f"PRAGMA table_info({table})")]
    placeholders = ", ".join("?" * len(cols))
    # Ignore duplicate rows when destination tasks share a SQLite source table
    # (e.g. ena_studies and ena_studies_search).
    insert = (
        f"INSERT OR IGNORE INTO {table} ({', '.join(cols)}) "  # noqa: S608
        f"VALUES ({placeholders})"
    )
    batch: list[tuple] = []
    written = 0
    for row in src.execute(f"SELECT {', '.join(cols)} FROM {table}"):  # noqa: S608
        if keep is not None and row[key_index] not in keep:
            continue
        batch.append(row)
        if len(batch) >= _COPY_CHUNK:
            dst.executemany(insert, batch)
            written += len(batch)
            batch.clear()
    if batch:
        dst.executemany(insert, batch)
        written += len(batch)
    dst.commit()
    return written


def build_delta(
    pg_url: str,
    source_db_file: Path,
    out_db_file: Path,
    tables: list[str],
    read_batch_size: int = 500,
) -> dict[str, int]:
    """Write out_db_file holding only rows Postgres lacks. Returns rows per table."""
    info = build_table_info(
        source_db_file,
        source_db_file,
        source_db_file,
        source_db_file,
        source_db_file,
        source_db_file,
        source_db_file,
        read_batch_size,
    )
    out_db_file.unlink(missing_ok=True)
    src = sqlite3.connect(f"file:{source_db_file}?mode=ro", uri=True)
    dst = sqlite3.connect(out_db_file)
    # Disable durability for this disposable, rebuilt delta.
    dst.execute("PRAGMA journal_mode=OFF")
    dst.execute("PRAGMA synchronous=OFF")

    counts: dict[str, int] = {}
    with psycopg.connect(pg_url) as pg:
        for table in tables:
            meta = info.get(table)
            if meta is None:
                logger.warning("no transfer task named %s; skipped", table)
                continue
            sqlite_table = meta.select_table_name or table
            ddl_row = src.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (sqlite_table,),
            ).fetchone()
            if ddl_row is None:
                logger.warning(
                    "%s absent from %s; skipped", sqlite_table, source_db_file
                )
                continue
            # Tasks sharing a SQLite source table must reuse its delta table.
            already_created = dst.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (sqlite_table,),
            ).fetchone()
            if not already_created:
                dst.execute(ddl_row[0])

            if not meta.conflict_cols:
                written = _copy_rows(src, dst, sqlite_table, None, 0)
                logger.info("%s: no key, copied whole (%d rows)", table, written)
                counts[table] = written
                continue

            key = meta.conflict_cols[0]
            new, staged = _new_keys(pg, src, sqlite_table, table, key)
            cols = [r[1] for r in src.execute(f"PRAGMA table_info({sqlite_table})")]
            written = (
                _copy_rows(src, dst, sqlite_table, new, cols.index(key)) if new else 0
            )
            logger.info("%s: staged %d, new %d", table, staged, written)
            counts[table] = written

    dst.close()
    src.close()
    return counts
