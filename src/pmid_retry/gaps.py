"""
Find accessions with no PubMed link
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import psycopg

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

_NO_PYSRADB = "(pubmed_id_pysradb IS NULL OR pubmed_id_pysradb = '')"

GAP_SQL: dict[str, str] = {
    "sra": (
        "SELECT accession FROM sra_studies"
        " WHERE (links IS NULL OR links->'XREF_LINK'->>'DB' IS DISTINCT FROM 'pubmed')"
        f" AND {_NO_PYSRADB}"
    ),
    "geo": (
        "SELECT accession FROM geo_series"
        " WHERE (pubmed_id IS NULL OR cardinality(pubmed_id) = 0)"
        f" AND {_NO_PYSRADB}"
    ),
    "ena": f"SELECT study_accession FROM ena_studies WHERE {_NO_PYSRADB}",
    "ae": (
        "SELECT accession FROM arrayexpress_experiments"
        f" WHERE (pubmed_id IS NULL OR pubmed_id = '') AND {_NO_PYSRADB}"
    ),
    "dra": (
        "SELECT accession FROM dra_studies"
        " WHERE is_only_ddbj = 1"
        " AND (links IS NULL OR links->'XREF_LINK'->>'DB' IS DISTINCT FROM 'pubmed')"
        f" AND {_NO_PYSRADB}"
    ),
    "gea": (
        "SELECT accession FROM gea_experiments"
        f" WHERE (pubmed_id IS NULL OR pubmed_id = '') AND {_NO_PYSRADB}"
    ),
    "gsa": f"SELECT study_accession FROM gsa_studies WHERE {_NO_PYSRADB}",
}


def pmid_gaps(
    pg_url: str,
    sources: tuple[str, ...] | None = None,
    limit: int | None = None,
    exclude_db_file: Path | None = None,
) -> dict[str, list[str]]:
    """
    Collect accessions with no PubMed link, per source.
    """
    wanted = sources or tuple(GAP_SQL)
    attempted: set[str] = set()
    if exclude_db_file and exclude_db_file.is_file():
        from helpers import sqlite_connect

        conn = sqlite_connect(exclude_db_file)
        attempted = {
            row[0] for row in conn.execute("SELECT accession FROM pmid_retry_results")
        }
        conn.close()
        logger.info("excluding %d already attempted accessions", len(attempted))

    gaps: dict[str, list[str]] = {}
    with psycopg.connect(pg_url) as conn, conn.cursor() as cur:
        for source in wanted:
            query = GAP_SQL[source]
            if limit:
                query += f" LIMIT {int(limit)}"
            cur.execute(query)
            found = [row[0] for row in cur.fetchall() if row[0]]
            gaps[source] = [a for a in found if a not in attempted]
            logger.info(
                "%s: %d without a pubmed link, %d new",
                source,
                len(found),
                len(gaps[source]),
            )
    return gaps


def write_manifest(gaps: dict[str, list[str]], manifest_file: Path) -> int:
    """Write `<source>,<accession>` per line, the shape the fetch stage reads."""
    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"{source},{acc}" for source, accs in gaps.items() for acc in accs]
    manifest_file.write_text("\n".join(lines) + "\n" if lines else "")
    return len(lines)
