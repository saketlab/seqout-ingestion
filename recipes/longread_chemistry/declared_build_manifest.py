#!/usr/bin/env python
"""
Write yuj/longread_chem_declared/runs.txt: ONT run accessions still needing
a declared-chemistry check (declared_checked_at IS NULL), excluding runs
already at chemistry_confidence 'exact' or 'declared' -- no point re-mining
protocol text for a run that already has better or equal evidence.

Usage: python recipes/longread_chemistry/declared_build_manifest.py --db FILE [--limit N]
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--out", default="yuj/longread_chem_declared/runs.txt")
    ap.add_argument("--limit", type=int, default=1_000_000)
    args = ap.parse_args()

    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(longread_chemistry)")}
    checked_clause = (
        "AND declared_checked_at IS NULL" if "declared_checked_at" in cols else ""
    )
    rows = conn.execute(
        "SELECT run_accession FROM longread_chemistry "
        "WHERE instrument_platform = 'OXFORD_NANOPORE' "
        "AND chemistry_confidence NOT IN ('exact', 'declared') "
        f"{checked_clause} "
        "ORDER BY run_accession LIMIT ?",
        (args.limit,),
    ).fetchall()
    conn.close()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(r[0] for r in rows) + "\n")
    print(f"wrote {len(rows)} run accessions to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
