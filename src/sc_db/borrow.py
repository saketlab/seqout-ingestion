"""
Copy work-queue facts from production Postgres into SQLite.

Postgres access is SELECT/COPY-only. Local copies let SQLite views resolve in
one schema. Each target table is transactionally emptied and refilled.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterable, Iterator, Sequence

    import psycopg

logger = logging.getLogger(__name__)

# Server-side cursor page. Large enough to amortize tunnel trips, small
# enough for memory.
FETCH_SIZE = 50_000
PROGRESS_EVERY = 250_000


def _adapt(value: Any) -> Any:
    """Postgres types mapped into SQLite while preserving the tri-state."""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, Decimal):
        return int(value)
    if isinstance(value, dict | list):
        return json.dumps(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def pg_row_count(pg_conn: psycopg.Connection, table: str) -> int | None:
    """Exact count of the source table, recorded beside the copy."""
    try:
        row = pg_conn.execute(f"SELECT count(*) FROM {table}").fetchone()  # noqa: S608
    except Exception:
        logger.exception("could not count %s", table)
        return None
    return int(row[0]) if row else None


def pg_columns(pg_conn: psycopg.Connection, table: str) -> list[str]:
    return [
        r[0]
        for r in pg_conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s "
            "ORDER BY ordinal_position",
            (table,),
        )
    ]


def sqlite_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]


def record_source(
    conn: sqlite3.Connection,
    table_name: str,
    *,
    source_kind: str,
    source_ref: str,
    source_rows: int | None,
    rows_copied: int,
    copy_filter: str | None = None,
    columns_missing: Sequence[str] | None = None,
    notes: str | None = None,
) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO sc_borrowed_source (table_name, source_kind, "
        "source_ref, source_rows, rows_copied, copy_filter, columns_missing, "
        "copied_at, notes) VALUES (?,?,?,?,?,?,?,?,?)",
        (
            table_name,
            source_kind,
            source_ref,
            source_rows,
            rows_copied,
            copy_filter,
            json.dumps(list(columns_missing)) if columns_missing else None,
            datetime.now(UTC).isoformat(),
            notes,
        ),
    )
    conn.commit()


def _insert_sql(table: str, columns: Sequence[str]) -> str:
    placeholders = ",".join("?" * len(columns))
    cols = ",".join(columns)
    return f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({placeholders})"  # noqa: S608


def load_rows(
    conn: sqlite3.Connection,
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
    *,
    label: str,
    truncate: bool = True,
) -> int:
    """
    Refill `table` from `rows` inside one transaction.

    Reruns reproduce the table, and crashes leave previous contents intact.
    """
    if truncate:
        conn.execute(f"DELETE FROM {table}")  # noqa: S608
    stmt = _insert_sql(table, columns)
    started = time.monotonic()
    total = 0
    batch: list[tuple] = []
    for row in rows:
        batch.append(tuple(_adapt(v) for v in row))
        if len(batch) >= FETCH_SIZE:
            conn.executemany(stmt, batch)
            total += len(batch)
            batch.clear()
            if total % PROGRESS_EVERY < FETCH_SIZE:
                logger.info(
                    "  %s: %s rows (%.0f rows/s)",
                    label,
                    f"{total:,}",
                    total / max(time.monotonic() - started, 1e-6),
                )
    if batch:
        conn.executemany(stmt, batch)
        total += len(batch)
    conn.commit()
    logger.info(
        "  %s: %s rows in %.1fs",
        label,
        f"{total:,}",
        time.monotonic() - started,
    )
    return total


def pg_stream(
    pg_conn: psycopg.Connection,
    query: str,
    params: Sequence[Any] | None = None,
) -> Iterator[tuple]:
    """Server-side cursor, so a 2.5M-row table is never materialised locally."""
    with pg_conn.cursor(name="sc_db_stream") as cur:
        cur.itersize = FETCH_SIZE
        cur.execute(query, params)
        yield from cur


def copy_pg_table(
    conn: sqlite3.Connection,
    pg_conn: psycopg.Connection,
    *,
    target: str,
    pg_table: str,
    query: str | None = None,
    params: Sequence[Any] | None = None,
    columns: Sequence[str] | None = None,
    copy_filter: str | None = None,
    notes: str | None = None,
) -> int:
    """
    Copy one Postgres table or projection into `target`.

    Columns default to the intersection of Postgres and SQLite declarations;
    sc_borrowed_source records omissions so missing data is visible.
    """
    target_cols = sqlite_columns(conn, target)
    if columns is None:
        source_cols = pg_columns(pg_conn, pg_table)
        columns = [c for c in target_cols if c in source_cols]
        missing = [c for c in target_cols if c not in source_cols]
    else:
        missing = [c for c in target_cols if c not in columns]
    if not columns:
        msg = f"no shared columns between {pg_table} and {target}"
        raise ValueError(msg)

    source_rows = pg_row_count(pg_conn, pg_table)
    sql = query or f"SELECT {','.join(columns)} FROM {pg_table}"  # noqa: S608
    logger.info(
        "borrow %s <- postgres.%s (%s source rows)",
        target,
        pg_table,
        f"{source_rows:,}" if source_rows else "?",
    )
    copied = load_rows(
        conn,
        target,
        columns,
        pg_stream(pg_conn, sql, params),
        label=target,
    )
    record_source(
        conn,
        target,
        source_kind="postgres",
        source_ref=pg_table,
        source_rows=source_rows,
        rows_copied=copied,
        copy_filter=copy_filter,
        columns_missing=missing,
        notes=notes,
    )
    return copied
