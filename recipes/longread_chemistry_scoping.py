#!/usr/bin/env python
"""
Scoping query for the long-read chemistry plan: how big is the long-read
corpus, and does sra_run_download_links tell us anything about BAM vs FASTQ
submission format.

Pulls ONT/PacBio runs from Postgres (sra_run_download_links joined to
ena_experiments, which mirrors SRA-origin runs too) into a local SQLite file,
then reports counts.

Usage: PG_URL=... python recipes/longread_chemistry_scoping.py [--out FILE]
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import time
from pathlib import Path

import psycopg

PLATFORMS = ("OXFORD_NANOPORE", "PACBIO_SMRT")

PULL_SQL = """
SELECT l.run_accession, l.study_accession, l.experiment_accession,
       e.instrument_platform, e.instrument_model,
       l.fastq_ftp, l.sra_ftp
FROM sra_run_download_links l
JOIN ena_experiments e USING (run_accession)
WHERE e.instrument_platform IN ('OXFORD_NANOPORE', 'PACBIO_SMRT')
"""

UNLINKED_SQL = """
SELECT count(*)
FROM ena_experiments e
LEFT JOIN sra_run_download_links l USING (run_accession)
WHERE e.instrument_platform IN ('OXFORD_NANOPORE', 'PACBIO_SMRT')
  AND l.run_accession IS NULL
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="/tmp/longread_download_links.db")
    args = ap.parse_args()

    out = Path(args.out)
    out.unlink(missing_ok=True)
    conn = sqlite3.connect(out)
    conn.execute(
        "CREATE TABLE longread_download_links ("
        "run_accession TEXT PRIMARY KEY, study_accession TEXT, "
        "experiment_accession TEXT, instrument_platform TEXT, "
        "instrument_model TEXT, fastq_ftp TEXT, sra_ftp TEXT)"
    )

    pg_url = os.environ["PG_URL"]
    t0, n = time.time(), 0
    batch = []
    with psycopg.connect(pg_url) as pg, pg.cursor() as cur:
        with cur.copy(f"COPY ({PULL_SQL}) TO STDOUT") as cp:
            for row in cp.rows():
                batch.append(row)
                if len(batch) >= 50_000:
                    conn.executemany(
                        "INSERT INTO longread_download_links VALUES (?,?,?,?,?,?,?)",
                        batch,
                    )
                    n += len(batch)
                    batch.clear()
        if batch:
            conn.executemany(
                "INSERT INTO longread_download_links VALUES (?,?,?,?,?,?,?)", batch
            )
            n += len(batch)
        conn.commit()
        print(f"pulled {n} long-read runs in {time.time() - t0:.0f}s")

        row = pg.execute(UNLINKED_SQL).fetchone()
        unlinked = row[0] if row else 0
        print(f"long-read runs with NO sra_run_download_links row: {unlinked}")

    conn.execute(
        "CREATE INDEX idx_lr_platform ON longread_download_links(instrument_platform)"
    )
    conn.commit()

    print("\n=== by platform ===")
    for row in conn.execute(
        "SELECT instrument_platform, count(*), "
        "count(DISTINCT study_accession), "
        "sum(CASE WHEN COALESCE(fastq_ftp,'')='' THEN 1 ELSE 0 END), "
        "sum(CASE WHEN COALESCE(sra_ftp,'')='' THEN 1 ELSE 0 END) "
        "FROM longread_download_links GROUP BY 1"
    ):
        platform, runs, studies, no_fastq, no_sra = row
        print(
            f"{platform:>16}: {runs:>9} runs, {studies:>7} studies, "
            f"{no_fastq:>7} with no fastq_ftp, {no_sra:>7} with no sra_ftp"
        )

    print("\n=== by instrument_model (top 20) ===")
    for model, n_runs in conn.execute(
        "SELECT instrument_model, count(*) FROM longread_download_links "
        "GROUP BY 1 ORDER BY 2 DESC LIMIT 20"
    ):
        print(f"  {model or '(null)':<24} {n_runs}")

    conn.close()
    print(f"\nwrote {out}")
    print(
        "\nNOTE: fastq_ftp presence is NOT a BAM-vs-FASTQ submission-format signal.\n"
        "ERR16773408 was submitted as BAM-only (no fastq at all in the ENA XML data\n"
        "block) but fastq_ftp is populated here anyway -- ENA converts BAM to fastq\n"
        "on demand for this field regardless of what was submitted. This table can\n"
        "size the corpus, not answer whether a native BAM (and its chemistry-bearing\n"
        "header) is available. That needs a live sample against ENA's filereport\n"
        "submitted_ftp/submitted_format fields, which this pipeline does not ingest."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
