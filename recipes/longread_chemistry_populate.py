#!/usr/bin/env python
"""
Populate a chemistry column for every long-read run, at whatever confidence
the evidence actually supports.

Two tiers, evidence stays visible so "populated" never overstates what is
known:

  bucket:  instrument_model alone bounds the chemistry to a generation.
              Zero network calls, near-total coverage, because every run
              already carries this field. Never sourced from first_public:
              that is a deposit date, not a sequencing date, so a study run
              on old chemistry but deposited late would bucket wrong with a
              confident-looking label. Model unresolved (ONT always; some
              PacBio models) yields confidence 'unknown' rather than a guess.
  exact:   the BAM header @RG DS: tag (see longread_chemistry_pilot.py),
              only for the runs that were submitted as native BAM. Written
              by longread_chemistry_bam_upgrade.py, which overwrites a
              bucket row's chemistry/confidence/source when it finds one,
              never the reverse.

PacBio-model buckets are deliberately coarse where the public
chemistry-version history is genuinely ambiguous (e.g. Revio's SPRQ vs
SPRQ-Nx cutover date is not publicly pinned); see _pacbio_bucket's
docstring. A wrong bucket that looks confident is worse than an honest
"unresolved".

Usage: PG_URL=... python recipes/longread_chemistry_populate.py --out FILE
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import time
from pathlib import Path

import psycopg

PULL_SQL = """
SELECT l.run_accession, l.study_accession, e.instrument_platform,
       e.instrument_model
FROM sra_run_download_links l
JOIN ena_experiments e USING (run_accession)
WHERE e.instrument_platform IN ('OXFORD_NANOPORE', 'PACBIO_SMRT')
"""


def _ont_bucket() -> tuple[str, str]:
    """
    ONT carries no per-run signal for chemistry generation short of the BAM
    header. Bucketing Kit 14 vs R9.4.1 off first_public is avoided since that
    is a deposit date, not a sequencing date, and a study run on old
    chemistry but deposited late would bucket wrong with a confident-looking
    label. Every ONT run stays unresolved at this tier; only the exact-tier
    BAM-header scan can call it.
    """
    return "Oxford Nanopore (chemistry unresolved)", "unknown"


def _pacbio_bucket(model: str | None) -> tuple[str, str]:
    """
    (chemistry, confidence) from instrument_model alone.

    Model is a hard boundary (a Sequel II cannot run RS-era chemistry), so it
    is the only key. Sub-model chemistry versions (C2 vs P4-P6 on RS II; 2.0
    vs 3.2 on Sequel II) are not resolved here: PacBio's own release
    history gives dates for the chemistry name but not a clean per-run
    boundary, and guessing one would be false precision. A first_public date
    split is deliberately avoided too, since it is a deposit date, not a
    sequencing date, so a study could deposit late while still running old
    chemistry and bucket wrong with a confident-looking label.
    """
    if not model:
        return "PacBio (chemistry unresolved, no instrument_model)", "unknown"
    m = model.strip().lower()
    if m in {"pacbio rs", "pacbio rs ii"}:
        return f"{model} (CLR, C2/P4-P6 era, exact kit unresolved)", "bucket"
    if m == "sequel":
        return "Sequel (CLR-or-early-HiFi, unresolved)", "unknown"
    if m == "sequel ii":
        return (
            "Sequel II (HiFi chemistry 2.0-3.2 era, exact version unresolved)",
            "bucket",
        )
    if m == "sequel iie":
        return "Sequel IIe (on-instrument HiFi, exact chemistry unresolved)", "bucket"
    if m == "revio":
        return "Revio (SPRQ family, SPRQ-vs-SPRQ-Nx unresolved)", "bucket"
    return f"{model} (unrecognized PacBio instrument, unresolved)", "unknown"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    out = Path(args.out)
    out.unlink(missing_ok=True)
    conn = sqlite3.connect(out)
    conn.execute(
        "CREATE TABLE longread_chemistry ("
        "run_accession TEXT PRIMARY KEY, study_accession TEXT, "
        "instrument_platform TEXT, instrument_model TEXT, "
        "chemistry TEXT, chemistry_confidence TEXT, chemistry_source TEXT, "
        "raw_evidence TEXT)"
    )

    t0, n = time.time(), 0
    batch = []
    with psycopg.connect(os.environ["PG_URL"]) as pg, pg.cursor() as cur:
        with cur.copy(f"COPY ({PULL_SQL}) TO STDOUT") as cp:
            for run_acc, study_acc, platform, model in cp.rows():
                if platform == "OXFORD_NANOPORE":
                    chem, conf = _ont_bucket()
                else:
                    chem, conf = _pacbio_bucket(model)
                batch.append(
                    (
                        run_acc,
                        study_acc,
                        platform,
                        model,
                        chem,
                        conf,
                        "instrument_model",
                        None,
                    ),
                )
                if len(batch) >= 50_000:
                    conn.executemany(
                        "INSERT INTO longread_chemistry VALUES (?,?,?,?,?,?,?,?)", batch
                    )
                    n += len(batch)
                    batch.clear()
    if batch:
        conn.executemany(
            "INSERT INTO longread_chemistry VALUES (?,?,?,?,?,?,?,?)", batch
        )
        n += len(batch)
    conn.commit()
    print(f"populated {n} rows in {time.time() - t0:.0f}s")

    print("\n=== chemistry_confidence breakdown ===")
    for conf, cnt in conn.execute(
        "SELECT chemistry_confidence, count(*) FROM longread_chemistry GROUP BY 1 ORDER BY 2 DESC"
    ):
        print(f"  {conf:<12} {cnt}")

    print("\n=== chemistry label breakdown ===")
    for chem, cnt in conn.execute(
        "SELECT chemistry, count(*) FROM longread_chemistry GROUP BY 1 ORDER BY 2 DESC"
    ):
        print(f"  {cnt:>9}  {chem}")

    conn.close()
    print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
