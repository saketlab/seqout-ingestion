"""
Transfer disease_inheritance into Postgres.

The table is rebuilt whole from one upstream release and uses overwrite_update.
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
    "mondo_id",
    "modes",
    "hpo_ids",
    "omim_ids",
    "orpha_ids",
    "sources",
    "hpoa_version",
    "hpo_version",
    "orphanet_version",
    "fetched_at",
)
#: ';'-joined in staging, text[] in Postgres. The rest travel as scalars.
_ARRAYS = {"modes", "hpo_ids", "omim_ids", "orpha_ids", "sources"}
_PAYLOAD = [c for c in _COLS if c != "mondo_id"]
_UPSERT = (
    f"INSERT INTO disease_inheritance ({', '.join(_COLS)}) "  # noqa: S608
    f"VALUES ({', '.join(['%s'] * len(_COLS))}) "
    f"ON CONFLICT (mondo_id) DO UPDATE "
    f"{overwrite_update('disease_inheritance', _PAYLOAD)}"
)


def _split(value: str | None) -> list[str]:
    """Split ';'-joined staging text into the text[] Postgres holds."""
    if not value:
        return []
    return [part for part in (p.strip() for p in value.split(";")) if part]


def _rows(db_file: Path) -> Iterator[tuple]:
    with sqlite_connect(db_file) as conn:
        query = f"SELECT {', '.join(_COLS)} FROM disease_inheritance"  # noqa: S608
        for row in conn.execute(query):
            yield tuple(
                _split(value) if col in _ARRAYS else value
                for col, value in zip(_COLS, row, strict=True)
            )


def inheritance_transfer(pg_url: str, inheritance_db_file: Path) -> None:
    """Load disease_inheritance into Postgres with an idempotent upsert."""
    rows = list(_rows(inheritance_db_file))
    with psycopg.connect(pg_url) as conn, conn.cursor() as cur:
        exec_query_from_file("tables/inheritance_pg.sql", conn=conn)
        cur.executemany(cast("LiteralString", _UPSERT), rows)
        conn.commit()
        held = cur.execute("SELECT count(*) FROM disease_inheritance").fetchone()
    logger.info(
        "inheritance transfer: upserted %d rows; table holds %d",
        len(rows),
        (held or (0,))[0],
    )
