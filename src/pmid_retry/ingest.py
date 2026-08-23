"""
Load the sweep's per-accession JSON into one SQLite file.

Staging keeps "attempted, nothing found" apart from "never attempted".
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from helpers import exec_query_from_file, sqlite_connect

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path

logger = logging.getLogger(__name__)

_UPSERT = """
INSERT INTO pmid_retry_results
    (accession, source, resolver, pmids_json, pmid, n_pmids, error,
     pysradb_version, elapsed_s, checked_at, host)
VALUES (?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(accession) DO UPDATE SET
    pmid       = COALESCE(excluded.pmid, pmid_retry_results.pmid),
    pmids_json = CASE WHEN excluded.n_pmids > 0
                      THEN excluded.pmids_json ELSE pmid_retry_results.pmids_json END,
    n_pmids    = MAX(excluded.n_pmids, pmid_retry_results.n_pmids),
    error      = COALESCE(pmid_retry_results.error, excluded.error),
    checked_at = excluded.checked_at,
    host       = excluded.host
"""


def load_manifest(conn: sqlite3.Connection, manifest_file: Path) -> int:
    """Record what the sweep was asked for, as `<source>,<accession>` per line."""
    rows = []
    for line in manifest_file.read_text().splitlines():
        source, _, accession = line.strip().partition(",")
        if accession:
            rows.append((accession, source))
    conn.executemany(
        "INSERT OR IGNORE INTO pmid_retry_requested (accession, source) VALUES (?,?)",
        rows,
    )
    return len(rows)


def pmid_retry_ingest(
    results_dirs: list[Path],
    db_file: Path,
    manifest_file: Path | None = None,
) -> dict[str, int]:
    """Stage every `<accession>.json` under results_dirs into db_file."""
    db_file.parent.mkdir(parents=True, exist_ok=True)
    exec_query_from_file("tables/pmid_retry.sql", db_file=db_file)
    conn = sqlite_connect(db_file)

    if manifest_file:
        logger.info(
            "manifest %s: %d rows", manifest_file, load_manifest(conn, manifest_file)
        )

    loaded = unparseable = 0
    for results_dir in results_dirs:
        # per-host subdirs (yuj pull --per-host) and a flat dir both work
        for path in sorted(results_dir.rglob("*.json")):
            try:
                rec = json.loads(path.read_text())
            except (OSError, json.JSONDecodeError):
                unparseable += 1
                continue
            pmids = rec.get("pmids") or []
            host = path.parent.name if path.parent != results_dir else None
            conn.execute(
                _UPSERT,
                (
                    rec.get("accession"),
                    rec.get("source"),
                    rec.get("resolver"),
                    json.dumps(pmids),
                    # matches the resolver's own pick, so verification judges
                    # the value that would land in pubmed_id_pysradb
                    min(pmids, key=int) if pmids else None,
                    len(pmids),
                    rec.get("error"),
                    rec.get("pysradb_version"),
                    rec.get("elapsed_s"),
                    rec.get("checked_at"),
                    host,
                ),
            )
            loaded += 1
        conn.commit()

    stats = {
        "requested": conn.execute(
            "SELECT COUNT(*) FROM pmid_retry_requested"
        ).fetchone()[0],
        "attempted": conn.execute("SELECT COUNT(*) FROM pmid_retry_results").fetchone()[
            0
        ],
        "resolved": conn.execute(
            "SELECT COUNT(*) FROM pmid_retry_results WHERE n_pmids > 0"
        ).fetchone()[0],
        "errored": conn.execute(
            "SELECT COUNT(*) FROM pmid_retry_results WHERE error IS NOT NULL"
        ).fetchone()[0],
        "loaded": loaded,
        "unparseable": unparseable,
    }
    conn.close()
    return stats
