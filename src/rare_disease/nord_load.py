"""
Stage NORD disease CSV rows into SQLite.

`nord fetch` owns fetching and MONDO name matching. NORD publishes no stable id,
so it stays separate from exact-xref GARD rows.
"""

from __future__ import annotations

import csv
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from helpers import exec_query_from_file, sqlite_connect

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

logger = logging.getLogger(__name__)

_INSERT = """
INSERT INTO nord_disease (
    nord_type, nord_slug, nord_name, nord_url, mondo_id, mondo_label,
    match_kind, mondo_rare, synonyms, fetched_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(nord_type, nord_slug) DO UPDATE SET
    nord_name   = excluded.nord_name,
    nord_url    = excluded.nord_url,
    mondo_id    = excluded.mondo_id,
    mondo_label = excluded.mondo_label,
    match_kind  = excluded.match_kind,
    mondo_rare  = excluded.mondo_rare,
    synonyms    = excluded.synonyms,
    fetched_at  = excluded.fetched_at
"""


def _rows(csv_file: Path, fetched_at: str) -> Iterator[tuple]:
    with csv_file.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            rare = row.get("mondo_rare") or ""
            yield (
                row["nord_type"],
                row["nord_slug"],
                row["nord_name"],
                row.get("nord_url") or None,
                row.get("mondo_id") or None,
                row.get("mondo_label") or None,
                row.get("match") or None,
                # blank means unmapped; mapped rarity remains separate
                None if rare == "" else int(rare),
                row.get("search_terms") or None,
                fetched_at,
            )


def load_nord(csv_file: Path, output_db_file: Path) -> None:
    """Load the NORD catalogue CSV into nord_disease. Idempotent."""
    fetched_at = datetime.now(UTC).isoformat()
    rows = list(_rows(csv_file, fetched_at))
    output_db_file.parent.mkdir(parents=True, exist_ok=True)
    with sqlite_connect(output_db_file) as conn:
        exec_query_from_file("tables/nord.sql", conn=conn)
        exec_query_from_file("indexes/nord.sql", conn=conn)
        conn.executemany(_INSERT, rows)
        conn.commit()
        mapped = conn.execute(
            "SELECT count(*) FROM nord_disease WHERE mondo_id IS NOT NULL"
        ).fetchone()[0]
    logger.info(
        "nord load: staged %d entries, %d with a MONDO id (%.1f%%)",
        len(rows),
        mapped,
        100 * mapped / len(rows) if rows else 0,
    )
