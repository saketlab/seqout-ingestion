#!/usr/bin/env python
"""
Load yuj/longread_chem's pulled result JSONs back into longread_chemistry.db.

status "exact" upgrades the row (chemistry/confidence/source + every
structured field the header yielded) and sets bam_checked_at. "no_bam" and
"no_tag" set bam_checked_at only -- the bucket-tier value already there
stands. "error" sets NOTHING: a run whose header we never successfully read
must stay unchecked and get retried by the next manifest build, not be
recorded as evidence-free. Same rule as longread_chemistry_bam_upgrade.py.

Usage: python recipes/longread_chemistry/ingest_results.py --db FILE --results-dir DIR
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from pathlib import Path

EVIDENCE_COLS = (
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


def _ensure_schema(conn: sqlite3.Connection) -> None:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(longread_chemistry)")}
    if "bam_checked_at" not in cols:
        conn.execute("ALTER TABLE longread_chemistry ADD COLUMN bam_checked_at TEXT")
    for col in EVIDENCE_COLS:
        if col not in cols:
            conn.execute(f"ALTER TABLE longread_chemistry ADD COLUMN {col} TEXT")  # noqa: S608
    conn.commit()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--results-dir", required=True)
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    _ensure_schema(conn)

    counts = {"exact": 0, "no_bam": 0, "no_tag": 0, "error": 0, "missing_row": 0}
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    n = 0
    for path in Path(args.results_dir).glob("*.json"):
        rec = json.loads(path.read_text())
        run_acc, status = rec["run_accession"], rec["status"]
        if status == "error":
            counts["error"] += 1
            continue
        if status == "exact":
            cols = [
                "chemistry",
                "chemistry_confidence",
                "chemistry_source",
                *EVIDENCE_COLS,
            ]
            values = [
                rec["chemistry"],
                "exact",
                "bam_header",
                *(rec.get(c) for c in EVIDENCE_COLS),
            ]
            cols += ["raw_evidence", "bam_checked_at"]
            values += [rec["raw_evidence"], now]
            sets = ", ".join(f"{c} = ?" for c in cols)
            cur = conn.execute(
                f"UPDATE longread_chemistry SET {sets} WHERE run_accession = ?",  # noqa: S608
                (*values, run_acc),
            )
        else:  # no_bam / no_tag
            cur = conn.execute(
                "UPDATE longread_chemistry SET bam_checked_at = ? WHERE run_accession = ?",
                (now, run_acc),
            )
        if cur.rowcount == 0:
            counts["missing_row"] += 1
        else:
            counts[status] += 1
        n += 1
        if n % 5000 == 0:
            conn.commit()
    conn.commit()
    conn.close()
    print(f"ingested {n} result files: {counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
