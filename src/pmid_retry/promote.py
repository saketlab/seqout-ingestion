"""
Write verified PMIDs into the per-source staging databases.

Fills `pubmed_id_pysradb` only where empty; `transfer` carries it to Postgres
under the non-nulling upsert.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from helpers import sqlite_connect
from pmid_retry.verify import PROMOTABLE

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

# source -> (table, accession column) in that source's staging SQLite
TARGETS: dict[str, tuple[str, str]] = {
    "sra": ("sra_studies", "accession"),
    "ena": ("ena_studies", "study_accession"),
    "geo": ("geo_series", "accession"),
    "ae": ("arrayexpress_experiments", "accession"),
    "gea": ("gea_experiments", "accession"),
    "gsa": ("gsa_studies", "accession"),
    "dra": ("dra_studies", "accession"),
}


def promotable_claims(
    db_file: Path, source: str, verdicts: tuple[str, ...]
) -> list[tuple[str, str]]:
    conn = sqlite_connect(db_file)
    placeholders = ",".join("?" for _ in verdicts)
    rows = conn.execute(
        "SELECT v.accession, v.claimed_pmid FROM pmid_retry_verification v"
        " JOIN pmid_retry_results r ON r.accession = v.accession"
        f" WHERE r.source = ? AND v.verdict IN ({placeholders})",
        (source, *verdicts),
    ).fetchall()
    conn.close()
    return rows


def pmid_retry_promote(
    db_file: Path,
    source_db_files: dict[str, Path],
    verdicts: tuple[str, ...] = PROMOTABLE,
    *,
    is_dry_run: bool = True,
) -> dict[str, dict[str, int]]:
    """Fill empty `pubmed_id_pysradb` in each source DB from verified claims."""
    report: dict[str, dict[str, int]] = {}

    for source, source_db in source_db_files.items():
        if source not in TARGETS:
            logger.warning("no target table for source %s", source)
            continue
        table, column = TARGETS[source]
        claims = promotable_claims(db_file, source, verdicts)
        if not claims:
            report[source] = {"claims": 0, "written": 0, "already_set": 0, "absent": 0}
            continue

        conn = sqlite_connect(source_db)
        written = already = absent = 0
        for accession, pmid in claims:
            row = conn.execute(
                f"SELECT pubmed_id_pysradb FROM {table} WHERE {column} = ?",
                (accession,),
            ).fetchone()
            if row is None:
                absent += 1
                continue
            if row[0]:
                already += 1
                continue
            if not is_dry_run:
                conn.execute(
                    f"UPDATE {table} SET pubmed_id_pysradb = ?"
                    f" WHERE {column} = ? AND (pubmed_id_pysradb IS NULL"
                    " OR pubmed_id_pysradb = '')",
                    (pmid, accession),
                )
            written += 1
        if not is_dry_run:
            conn.commit()
        conn.close()

        report[source] = {
            "claims": len(claims),
            "written": written,
            "already_set": already,
            "absent": absent,
        }
        logger.info(
            "%s: %s %d of %d claims (already set %d, absent %d)",
            source,
            "would write" if is_dry_run else "wrote",
            written,
            len(claims),
            already,
            absent,
        )
    return report
