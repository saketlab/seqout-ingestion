"""
Transfer the GARD dimension table into Postgres.

The table is rebuilt whole from one static catalogue and uses overwrite_update
for dead-tuple guards. Retired diseases keep existing rows.
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

_COLS = (
    "gard_id",
    "gard_name",
    "mondo_id",
    "mondo_label",
    "mondo_rare",
    "categories",
    "specialties",
    "groups",
    "synonyms",
    "inheritance",
    "inheritance_fetched_at",
    "fetched_at",
)
_PAYLOAD = [c for c in _COLS if c != "gard_id"]
_UPSERT = (
    f"INSERT INTO gard_disease ({', '.join(_COLS)}) "  # noqa: S608
    f"VALUES ({', '.join(['%s'] * len(_COLS))}) "
    f"ON CONFLICT (gard_id) DO UPDATE {overwrite_update('gard_disease', _PAYLOAD)}"
)


def _split(value: str | None) -> list[str]:
    """Split ';'-joined staging text into the text[] Postgres facets on."""
    if not value:
        return []
    return [part for part in (p.strip() for p in value.split(";")) if part]


def _rows(gard_db_file: Path) -> Iterator[tuple]:
    with sqlite_connect(gard_db_file) as conn:
        query = f"SELECT {', '.join(_COLS)} FROM gard_disease"  # noqa: S608 -- constants
        for row in conn.execute(query):
            # positional, in _COLS order
            (
                gid,
                name,
                mondo,
                label,
                rare,
                cats,
                specs,
                groups,
                syns,
                inheritance,
                inheritance_at,
                at,
            ) = row
            yield (
                gid,
                name,
                mondo,
                label,
                None if rare is None else bool(rare),
                _split(cats),
                _split(specs),
                _split(groups),
                _split(syns),
                _split(inheritance),
                inheritance_at,
                at,
            )


def gard_transfer(pg_url: str, gard_db_file: Path) -> None:
    """
    Load gard_disease into Postgres.

    Idempotent; retired diseases keep existing rows.
    """
    rows = list(_rows(gard_db_file))
    with psycopg.connect(pg_url) as conn, conn.cursor() as cur:
        exec_query_from_file("tables/gard_pg.sql", conn=conn)
        cur.executemany(cast("LiteralString", _UPSERT), rows)
        conn.commit()
        # count(mondo_id) is the non-null count
        counts = cur.execute(
            "SELECT count(*), count(mondo_id) FROM gard_disease"
        ).fetchone()
        held, mapped = counts or (0, 0)
    total = len(rows)
    logger.info(
        "gard transfer: upserted %d rows; table holds %d (%d with a MONDO id)",
        total,
        held,
        mapped,
    )
