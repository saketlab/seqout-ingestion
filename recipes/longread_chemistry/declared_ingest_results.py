#!/usr/bin/env python
"""
Load declared_worker's pulled result JSONs back into longread_chemistry.db.

status "declared" upgrades the row, but ONLY when it isn't already at
'exact' -- unlike ingest_results.py's exact merge (always the top tier, safe
to overwrite unconditionally), declared must never downgrade a BAM-measured
call. "no_match"/"empty" set declared_checked_at only. "error" sets NOTHING,
so the run gets retried by the next manifest build rather than recorded as
evidence-free.

Usage: python recipes/longread_chemistry/declared_ingest_results.py --db FILE --results-dir DIR
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from pathlib import Path

DECLARED_COLS = (
    "basecaller_software",
    "basecaller_software_version",
    "ont_pore",
    "ont_kit",
)


def _ensure_schema(conn: sqlite3.Connection) -> None:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(longread_chemistry)")}
    if "declared_checked_at" not in cols:
        conn.execute(
            "ALTER TABLE longread_chemistry ADD COLUMN declared_checked_at TEXT"
        )
    for col in DECLARED_COLS:
        if col not in cols:
            conn.execute(f"ALTER TABLE longread_chemistry ADD COLUMN {col} TEXT")
    conn.commit()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--results-dir", required=True)
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    _ensure_schema(conn)

    counts = {
        "declared": 0,
        "no_match": 0,
        "empty": 0,
        "error": 0,
        "skipped_exact": 0,
        "missing_row": 0,
    }
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    n = 0
    for path in Path(args.results_dir).glob("*.json"):
        rec = json.loads(path.read_text())
        run_acc, status = rec["run_accession"], rec["status"]
        if status == "error":
            counts["error"] += 1
            continue
        if status == "declared":
            cols = [
                "chemistry",
                "chemistry_confidence",
                "chemistry_source",
                *DECLARED_COLS,
            ]
            values = [
                rec["chemistry"],
                "declared",
                "protocol_text",
                *(rec.get(c) for c in DECLARED_COLS),
            ]
            cols += ["raw_evidence", "declared_checked_at"]
            values += [rec["raw_evidence"], now]
            sets = ", ".join(f"{c} = ?" for c in cols)
            cur = conn.execute(
                f"UPDATE longread_chemistry SET {sets} WHERE run_accession = ? "  # noqa: S608
                "AND chemistry_confidence IS DISTINCT FROM 'exact'",
                (*values, run_acc),
            )
            if cur.rowcount == 0:
                # either no such row, or it's already exact -- tell those apart
                exists = conn.execute(
                    "SELECT 1 FROM longread_chemistry WHERE run_accession = ?",
                    (run_acc,),
                ).fetchone()
                counts["skipped_exact" if exists else "missing_row"] += 1
                n += 1
                continue
        else:  # no_match / empty
            cur = conn.execute(
                "UPDATE longread_chemistry SET declared_checked_at = ? "
                "WHERE run_accession = ?",
                (now, run_acc),
            )
            if cur.rowcount == 0:
                counts["missing_row"] += 1
                n += 1
                continue
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
