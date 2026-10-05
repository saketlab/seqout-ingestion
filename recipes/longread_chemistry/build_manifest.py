#!/usr/bin/env python
"""
Write yuj/longread_chem/runs.txt: run accessions still needing a BAM-header
check (bam_checked_at IS NULL), PacBio first since its validated yield is
~12% vs ONT's ~2% (see longread_chemistry_pilot.py).

Usage: python recipes/longread_chemistry/build_manifest.py --db FILE [--limit N]
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--out", default="yuj/longread_chem/runs.txt")
    ap.add_argument("--limit", type=int, default=100_000)
    args = ap.parse_args()

    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(longread_chemistry)")}
    # every row needs a check when bam_checked_at doesn't exist yet
    where = "WHERE bam_checked_at IS NULL" if "bam_checked_at" in cols else ""
    rows = conn.execute(
        f"SELECT run_accession FROM longread_chemistry {where} "  # noqa: S608
        "ORDER BY instrument_platform DESC LIMIT ?",
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
