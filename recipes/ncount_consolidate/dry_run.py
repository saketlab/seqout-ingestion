#!/usr/bin/env python3
"""
Report what ncount_consolidated.db would change in `pentimento_sample`.

Read-only: every statement is a SELECT. Classifies staged samples as new, fill,
agree, conflict, or would_null; conflict needs review, and would_null must not
be written.
"""

from __future__ import annotations

import csv
import io
import json
import sqlite3
import subprocess
import sys
from collections import Counter
from pathlib import Path

SSH = [
    "ssh",
    "-i",
    "/home/saket/.ssh/id_ed25519",
    "-o",
    "BatchMode=yes",
    "ubuntu@167.114.208.177",
]


def psql(sql: str) -> list[list[str]]:
    out = subprocess.run(
        [*SSH, f"sudo -u postgres psql -d pysradb --csv -t -c {json.dumps(sql)}"],
        capture_output=True,
        text=True,
        timeout=1800,
        check=True,
    ).stdout
    return [r for r in csv.reader(io.StringIO(out)) if r]


def main() -> int:
    db = Path(sys.argv[1])
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    staged = {
        (s, a): (c, g)
        for s, a, c, g in con.execute(
            "SELECT study_accession, sample_accession, cells, genes "
            "FROM ncount_sample WHERE scope='sample'"
        )
    }
    print(f"staged scope=sample rows: {len(staged)}")

    live = {
        (r[0], r[1]): (int(r[2]) if r[2] else None, int(r[3]) if r[3] else None)
        for r in psql(
            "SELECT study_accession, sample_accession, cells, genes FROM pentimento_sample"
        )
    }
    print(f"live pentimento_sample rows: {len(live)}")

    verdict: Counter[str] = Counter()
    conflicts = []
    for key, (cells, genes) in staged.items():
        cur = live.get(key)
        if cur is None:
            verdict["new"] += 1
            continue
        for col, mine, theirs in (("cells", cells, cur[0]), ("genes", genes, cur[1])):
            if mine is None and theirs is not None:
                verdict[f"{col}:would_null"] += 1
            elif mine is not None and theirs is None:
                verdict[f"{col}:fill"] += 1
            elif mine == theirs:
                verdict[f"{col}:agree"] += 1
            elif mine is not None and theirs is not None:
                verdict[f"{col}:CONFLICT"] += 1
                if len(conflicts) < 10:
                    conflicts.append((key, col, theirs, mine))

    print("\n--- what a transfer would do ---")
    for k in sorted(verdict):
        print(f"  {k:22s} {verdict[k]}")
    if conflicts:
        print("\n--- sample conflicts (pg -> staged) ---")
        for (s, a), col, old, new in conflicts:
            print(f"  {s}/{a} {col}: {old} -> {new}")
    print("\nNOTHING WAS WRITTEN. This script only runs SELECTs.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
