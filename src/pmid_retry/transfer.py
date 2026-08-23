"""
Write staged PMIDs into Postgres.

Updates only the sweep's accessions, and only where `pubmed_id_pysradb` is
empty, so a re-run is idempotent.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import psycopg

from helpers import sqlite_connect

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

# source -> (postgres table, accession column)
PG_TARGETS: dict[str, tuple[str, str]] = {
    "sra": ("sra_studies", "accession"),
    "ena": ("ena_studies", "study_accession"),
    "geo": ("geo_series", "accession"),
    "ae": ("arrayexpress_experiments", "accession"),
    "gea": ("gea_experiments", "accession"),
    "gsa": ("gsa_studies", "study_accession"),
    "dra": ("dra_studies", "accession"),
}

_UPDATE = """
UPDATE {table} AS t
   SET pubmed_id_pysradb = v.pmid
  FROM (SELECT unnest(%s::text[]) AS accession,
               unnest(%s::text[]) AS pmid) AS v
 WHERE t.{column} = v.accession
   AND (t.pubmed_id_pysradb IS NULL OR t.pubmed_id_pysradb = '')
"""


def staged_claims(
    db_file: Path, source: str, verdicts: tuple[str, ...] | None
) -> list[tuple[str, str]]:
    """Claims for one source, optionally narrowed to given verification verdicts."""
    conn = sqlite_connect(db_file)
    if verdicts:
        placeholders = ",".join("?" for _ in verdicts)
        rows = conn.execute(
            "SELECT r.accession, r.pmid FROM pmid_retry_results r"
            " JOIN pmid_retry_verification v ON v.accession = r.accession"
            f" WHERE r.source = ? AND r.n_pmids > 0 AND v.verdict IN ({placeholders})",
            (source, *verdicts),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT accession, pmid FROM pmid_retry_results"
            " WHERE source = ? AND n_pmids > 0",
            (source,),
        ).fetchall()
    conn.close()
    return rows


def pmid_retry_transfer(
    db_file: Path,
    pg_url: str,
    verdicts: tuple[str, ...] | None = None,
    batch_size: int = 1000,
    *,
    is_dry_run: bool = True,
) -> dict[str, dict[str, int]]:
    """Fill empty `pubmed_id_pysradb` in Postgres from the staged claims."""
    report: dict[str, dict[str, int]] = {}

    with psycopg.connect(pg_url, autocommit=False) as conn:
        for source, (table, column) in PG_TARGETS.items():
            claims = staged_claims(db_file, source, verdicts)
            if not claims:
                continue

            accessions = [a for a, _ in claims]
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) FROM {table}"
                    f" WHERE {column} = ANY(%s)"
                    " AND pubmed_id_pysradb IS NOT NULL AND pubmed_id_pysradb <> ''",
                    (accessions,),
                )
                already_set = cur.fetchone()[0]
                cur.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {column} = ANY(%s)",
                    (accessions,),
                )
                present = cur.fetchone()[0]

            updated = 0
            if not is_dry_run:
                with conn.cursor() as cur:
                    statement = _UPDATE.format(table=table, column=column)
                    for start in range(0, len(claims), batch_size):
                        chunk = claims[start : start + batch_size]
                        cur.execute(
                            statement,
                            ([a for a, _ in chunk], [p for _, p in chunk]),
                        )
                        updated += cur.rowcount

            report[source] = {
                "claims": len(claims),
                "in_pg": present,
                "absent": len(claims) - present,
                "already_set": already_set,
                "fillable": present - already_set,
                "updated": updated,
            }
            logger.info(
                "%s: claims=%d fillable=%d %s=%d",
                source,
                len(claims),
                present - already_set,
                "would update" if is_dry_run else "updated",
                present - already_set if is_dry_run else updated,
            )

        if is_dry_run:
            conn.rollback()
        else:
            conn.commit()
    return report
