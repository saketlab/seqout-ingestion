"""
Transfer longread_chemistry.db into Postgres.

Non-nulling and tier-aware: an exact-tier chemistry call survives a
bucket-tier rebuild.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, LiteralString, cast

import psycopg

from helpers import exec_query_from_file, sqlite_connect
from transfer.pg_fields import tier_guarded_update

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

logger = logging.getLogger(__name__)

BATCH_SIZE = 5_000

_COLS = (
    "run_accession",
    "study_accession",
    "instrument_platform",
    "instrument_model",
    "chemistry",
    "chemistry_confidence",
    "chemistry_source",
    "raw_evidence",
    "bam_checked_at",
    "declared_checked_at",
    "basecaller_software",
    "basecaller_software_version",
    "flow_cell_id",
    "ont_pore",
    "ont_kit",
    "ont_speed_bps",
    "ont_model_tier",
    "ont_basecall_model_version",
    "pacbio_platform_model",
    "pacbio_chemistry_code",
    "pacbio_binding_kit",
    "pacbio_sequencing_kit",
    "pacbio_smrtcell_kit",
    "pacbio_basecaller_version",
)
_PAYLOAD = [c for c in _COLS if c != "run_accession"]

# populate.py rebuilds the whole db at bucket tier every run, so plain
# COALESCE would let a rebuild overwrite an already-resolved exact or
# declared value. declared (author-reported protocol text) beats bucket
# (instrument_model guess) but never a BAM-header-measured exact call.
_TIER_RANK = ["unknown", "bucket", "declared", "exact"]
_TIER_COLS = ["chemistry", "chemistry_confidence", "chemistry_source", "raw_evidence"]
_REST_COLS = [c for c in _PAYLOAD if c not in _TIER_COLS]
_SET_CLAUSE = tier_guarded_update(
    "longread_chemistry", _TIER_COLS, "chemistry_confidence", _TIER_RANK, _REST_COLS
)
_UPSERT = cast(
    "LiteralString",
    f"INSERT INTO longread_chemistry ({', '.join(_COLS)}) "  # noqa: S608
    f"VALUES ({', '.join(['%s'] * len(_COLS))}) "
    f"ON CONFLICT (run_accession) DO UPDATE {_SET_CLAUSE}",
)


def _rows(db_file: Path) -> Iterator[tuple]:
    with sqlite_connect(db_file) as conn:
        # a populate.py-only build predates ingest_results.py's ALTER and has
        # just the first 8 columns.
        present = {r[1] for r in conn.execute("PRAGMA table_info(longread_chemistry)")}
        select = ", ".join(c if c in present else "NULL" for c in _COLS)
        yield from conn.execute(f"SELECT {select} FROM longread_chemistry")  # noqa: S608


def longread_chemistry_transfer(pg_url: str, longread_chemistry_db_file: Path) -> None:
    """Load longread_chemistry into Postgres with a non-nulling upsert."""
    with (
        psycopg.connect(pg_url) as conn,
        conn.cursor() as cur,
    ):
        exec_query_from_file("tables/longread_chemistry_pg.sql", conn=conn)
        n = 0
        batch: list[tuple] = []
        for row in _rows(longread_chemistry_db_file):
            batch.append(row)
            n += 1
            if len(batch) >= BATCH_SIZE:
                cur.executemany(_UPSERT, batch)
                batch.clear()
        if batch:
            cur.executemany(_UPSERT, batch)
        conn.commit()
        held = cur.execute("SELECT count(*) FROM longread_chemistry").fetchone()
    logger.info(
        "longread_chemistry transfer: upserted %d rows; table holds %d",
        n,
        (held or (0,))[0],
    )
