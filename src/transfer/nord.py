"""
Transfer the NORD dimension table into Postgres.

The table is rebuilt whole from one static catalogue and uses overwrite_update.
Retired entries keep existing rows.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, LiteralString, cast

import psycopg

from helpers import exec_query_from_file, sqlite_connect
from transfer.pg_fields import overwrite_update

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

logger = logging.getLogger(__name__)

_KEY = ("nord_type", "nord_slug")
_COLS = (
    *_KEY,
    "nord_name",
    "nord_url",
    "mondo_id",
    "mondo_label",
    "match_kind",
    "mondo_rare",
    "synonyms",
    "fetched_at",
)
_PAYLOAD = [c for c in _COLS if c not in _KEY]
_UPSERT = (
    f"INSERT INTO nord_disease ({', '.join(_COLS)}) "  # noqa: S608
    f"VALUES ({', '.join(['%s'] * len(_COLS))}) "
    f"ON CONFLICT ({', '.join(_KEY)}) DO UPDATE "
    f"{overwrite_update('nord_disease', _PAYLOAD)}"
)


def _split(value: str | None) -> list[str]:
    """Split ';'-joined staging text into the text[] Postgres holds."""
    if not value:
        return []
    return [part for part in (p.strip() for p in value.split(";")) if part]


def _rows(nord_db_file: Path) -> Iterator[tuple]:
    with sqlite_connect(nord_db_file) as conn:
        query = f"SELECT {', '.join(_COLS)} FROM nord_disease"  # noqa: S608 -- constants
        for row in conn.execute(query):
            # positional, in _COLS order
            ntype, slug, name, url, mondo, label, kind, rare, syns, at = row
            yield (
                ntype,
                slug,
                name,
                url,
                mondo,
                label,
                kind,
                None if rare is None else bool(rare),
                _split(syns),
                at,
            )


def nord_transfer(pg_url: str, nord_db_file: Path) -> None:
    """
    Load nord_disease into Postgres.

    Idempotent; retired entries keep existing rows.
    """
    rows = list(_rows(nord_db_file))
    with psycopg.connect(pg_url) as conn, conn.cursor() as cur:
        exec_query_from_file("tables/nord_pg.sql", conn=conn)
        cur.executemany(cast("LiteralString", _UPSERT), rows)
        conn.commit()
        counts = cur.execute(
            "SELECT count(*), count(mondo_id), count(DISTINCT mondo_id) "
            "FROM nord_disease"
        ).fetchone()
        held, mapped, distinct = counts or (0, 0, 0)
    logger.info(
        "nord transfer: upserted %d rows; table holds %d "
        "(%d with a MONDO id, %d distinct MONDO terms)",
        len(rows),
        held,
        mapped,
        distinct,
    )
