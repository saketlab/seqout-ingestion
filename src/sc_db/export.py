"""
Emit study accession lists from sc_work_queue, one per line.

`needs_*` selects every study wanting that stage; `next_stage` selects only
those routed there first.
"""

from __future__ import annotations

import csv
import logging
from typing import TYPE_CHECKING

from helpers import sqlite_connect
from sc_db.aliases import canonicalise

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

# bucket -> needs column, or None where the queue carries no flag for it and
# next_stage is the only selector
BUCKETS: dict[str, str | None] = {
    "preflightx": "needs_preflightx",
    "pentimento": "needs_pentimento",
    "ncount": "needs_ncount",
    "plate_route": None,
}


def has_fastq_manifest(manifest_dir: Path, accession: str) -> bool:
    """
    Report whether the study's manifest carries at least one FASTQ url.

    preflight.run_preflightx reads `fastq_ftp` and nothing else, so a manifest
    without it yields an empty result file. yuj marks a study done by output
    existence, so dispatching one costs a slot and never retries.
    """
    path = manifest_dir / f"{accession}.csv"
    if not path.is_file():
        return False
    with path.open(encoding="utf-8") as handle:
        return any(
            (row.get("fastq_ftp") or "").strip() for row in csv.DictReader(handle)
        )


def queue_accessions(
    sc_db_file: Path,
    bucket: str,
    limit: int | None = None,
    max_runs: int | None = None,
    evidence_tier: str | None = None,
    manifest_dir: Path | None = None,
    *,
    by_next_stage: bool = False,
    countable_matrix_only: bool = False,
    canonicalise_accessions: bool = False,
) -> list[str]:
    """Study accessions for one bucket, smallest studies first."""
    if bucket not in BUCKETS:
        msg = f"unknown bucket {bucket!r}; expected one of {sorted(BUCKETS)}"
        raise ValueError(msg)
    needs_column = BUCKETS[bucket]

    if needs_column is None or by_next_stage:
        where = "next_stage = ?"
        stage_param: list[str] = [bucket]
    else:
        where = f"{needs_column} = 1"
        stage_param = []
    query = f"SELECT study_accession FROM sc_work_queue WHERE {where}"  # noqa: S608
    params: list[object] = [*stage_param]
    if max_runs:
        # a mega-study can resolve to six figures of runs and starve the fleet
        query += " AND (n_runs IS NULL OR n_runs <= ?)"
        params.append(max_runs)
    if countable_matrix_only:
        # ncount opens a deposited matrix; without one the study has nothing to
        # count and cells stays NULL whatever the fleet does
        query += " AND has_countable_matrix = 1"
    if evidence_tier:
        # insdc_library_source is the high tier's primary rule, and reaches
        # GSA, DDBJ and ArrayExpress, absent from the SRA tables
        query += " AND evidence_tier = ?"
        params.append(evidence_tier)
    query += " ORDER BY COALESCE(n_runs, 0), study_accession"
    if limit and manifest_dir is None:
        query += " LIMIT ?"
        params.append(limit)

    conn = sqlite_connect(f"file:{sc_db_file}?immutable=1", uri=True)
    try:
        found = [row[0] for row in conn.execute(query, params)]
    finally:
        conn.close()

    if canonicalise_accessions:
        found = canonicalise(sc_db_file, found)
    if manifest_dir is not None:
        found = [a for a in found if has_fastq_manifest(manifest_dir, a)]
        if limit:
            found = found[:limit]
    return found


def export_accessions(
    sc_db_file: Path,
    bucket: str,
    out_file: Path,
    limit: int | None = None,
    max_runs: int | None = None,
    evidence_tier: str | None = None,
    manifest_dir: Path | None = None,
    *,
    by_next_stage: bool = False,
    countable_matrix_only: bool = False,
    canonicalise_accessions: bool = False,
) -> int:
    """Write one accession per line and return how many."""
    accessions = queue_accessions(
        sc_db_file,
        bucket,
        limit,
        max_runs,
        evidence_tier,
        manifest_dir,
        by_next_stage=by_next_stage,
        countable_matrix_only=countable_matrix_only,
        canonicalise_accessions=canonicalise_accessions,
    )
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text("\n".join(accessions) + "\n" if accessions else "")
    logger.info("%s: %d studies -> %s", bucket, len(accessions), out_file)
    return len(accessions)
